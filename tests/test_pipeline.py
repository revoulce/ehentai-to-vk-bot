import asyncio
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Test credentials and database are isolated from the user's deployment.
os.environ.update(
    {
        "VK_ACCESS_TOKEN": "test",
        "VK_GROUP_ID": "1",
        "TG_BOT_TOKEN": "test",
        "ADMIN_IDS": "[1]",
        "EH_COOKIES": "{}",
        "API_SECRET": "test-secret",
        "DB_URL": "sqlite+aiosqlite:///:memory:",
    }
)

from aiohttp import ClientConnectionError
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from tenacity import wait_none

import api
import services
import workers
from config import Settings, settings
from models import Base, Gallery, PostStatus
from publisher import VkAPIError, VkPublisher
from scheduling import calculate_next_slot
from scraper import EhentaiHarvester
from utils import normalize_gallery_url


class InputTests(unittest.TestCase):
    def test_gallery_url_normalization(self):
        self.assertEqual(
            normalize_gallery_url(" http://e-hentai.org/g/123/ABCdef?p=1#tag "),
            "https://e-hentai.org/g/123/abcdef/",
        )

    def test_only_gallery_hosts_and_paths_are_accepted(self):
        for value in [
            None,
            "https://e-hentai.org.evil/g/1/abc/",
            "file:///g/1/abc/",
            "http://localhost/g/1/abc/",
            "https://e-hentai.org/s/abc/1-1",
            "https://user@e-hentai.org/g/1/abc/",
            "https://e-hentai.org:bad/g/1/abc/",
        ]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_gallery_url(value)

    def test_settings_reject_invalid_interval_and_missing_secret(self):
        values = dict(
            VK_ACCESS_TOKEN="test",
            VK_GROUP_ID=1,
            TG_BOT_TOKEN="test",
            ADMIN_IDS=[1],
            EH_COOKIES={},
        )
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValidationError):
                Settings(_env_file=None, **values)
            with self.assertRaises(ValidationError):
                Settings(
                    _env_file=None,
                    API_SECRET="test",
                    SCHEDULE_INTERVAL_HOURS=0,
                    **values,
                )

    def test_failed_atomic_write_removes_partial_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.jpg"
            with patch.object(Path, "replace", side_effect=OSError("disk failure")):
                with self.assertRaises(OSError):
                    EhentaiHarvester._write_file(path, b"image")
            self.assertEqual(list(Path(directory).iterdir()), [])


class SchedulingTests(unittest.TestCase):
    def test_buffer_crosses_midnight(self):
        now = datetime(2026, 9, 30, 23, 58, tzinfo=timezone.utc)
        self.assertEqual(
            calculate_next_slot(now, None, 1),
            datetime(2026, 10, 1, 1, tzinfo=timezone.utc),
        )

    def test_from_time_does_not_override_a_reserved_slot(self):
        now = datetime(2026, 9, 30, 12, tzinfo=timezone.utc)
        last = now.replace(hour=18)
        self.assertEqual(
            calculate_next_slot(now, last, 2, now), last + timedelta(hours=2)
        )

    def test_unaligned_slot_still_preserves_minimum_spacing(self):
        now = datetime(2026, 9, 30, 12, tzinfo=timezone.utc)
        last = now.replace(hour=13, minute=30, tzinfo=None)
        self.assertEqual(calculate_next_slot(now, last, 2), now.replace(hour=16))

    def test_exact_five_minute_buffer_is_valid(self):
        now = datetime(2026, 9, 30, 12, 55, tzinfo=timezone.utc)
        self.assertEqual(
            calculate_next_slot(now, None, 1), now.replace(hour=13, minute=0)
        )


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        database = Path(self.directory.name) / "test.db"
        self.engine = create_async_engine(f"sqlite+aiosqlite:///{database.as_posix()}")
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.patches = [
            patch.object(services, "AsyncSessionLocal", self.sessions),
            patch.object(workers, "AsyncSessionLocal", self.sessions),
        ]
        for mock in self.patches:
            mock.start()

    async def asyncTearDown(self):
        for mock in reversed(self.patches):
            mock.stop()
        await self.engine.dispose()
        self.directory.cleanup()

    async def add_gallery(self, **overrides):
        values = dict(
            source_url="https://e-hentai.org/g/1/abc/",
            title="Test",
            tags={"character": ["test character"]},
            local_images=["1.jpg", "2.jpg", "3.jpg", "4.jpg", "5.jpg"],
            status=PostStatus.DOWNLOADED,
            include_cosplayer=False,
            scheduled_for=datetime.now(timezone.utc) + timedelta(hours=2),
        )
        values.update(overrides)
        async with self.sessions() as session:
            gallery = Gallery(**values)
            session.add(gallery)
            await session.commit()
            return gallery.id

    async def test_concurrent_duplicate_queue_returns_service_error(self):
        results = await asyncio.gather(
            services.queue_gallery("https://e-hentai.org/g/12/abc/"),
            services.queue_gallery("http://e-hentai.org/g/12/abc?p=2"),
            return_exceptions=True,
        )
        self.assertEqual(results.count("Queued"), 1)
        self.assertEqual(
            sum(isinstance(result, services.ServiceError) for result in results), 1
        )
        async with self.sessions() as session:
            self.assertEqual(
                len((await session.execute(select(Gallery))).scalars().all()), 1
            )

    async def test_failed_gallery_does_not_reserve_time(self):
        last = datetime.now(timezone.utc) + timedelta(hours=3)
        await self.add_gallery(scheduled_for=last)
        await self.add_gallery(
            source_url="https://e-hentai.org/g/2/abc/",
            status=PostStatus.FAILED,
            scheduled_for=last + timedelta(days=30),
        )
        slot = await services.get_next_available_slot(
            from_time=datetime.now(timezone.utc)
        )
        self.assertGreaterEqual(
            slot, last + timedelta(hours=settings.SCHEDULE_INTERVAL_HOURS)
        )
        self.assertLess(
            slot, last + timedelta(hours=settings.SCHEDULE_INTERVAL_HOURS + 1)
        )

    async def test_donut_upload_failure_does_not_repeat_public_post(self):
        gallery_id = await self.add_gallery()
        publisher = MagicMock()
        publisher.upload_photos = AsyncMock(
            side_effect=[["photo1_1"], RuntimeError("Donut failed")]
        )
        publisher.publish = AsyncMock(return_value=42)
        with patch.object(workers, "cleanup_files", new_callable=AsyncMock):
            async with self.sessions() as session:
                gallery = await session.get(Gallery, gallery_id)
                await workers.publish_gallery(gallery, session, publisher)
        async with self.sessions() as session:
            gallery = await session.get(Gallery, gallery_id)
            self.assertEqual(gallery.vk_post_id, 42)
            self.assertEqual(gallery.status, PostStatus.POSTED)
        publisher.publish.assert_awaited_once()

    async def test_public_post_is_saved_before_donut_starts(self):
        gallery_id = await self.add_gallery()
        calls = 0
        checkpoints = []

        async def upload(paths):
            nonlocal calls
            calls += 1
            if calls == 2:
                async with self.sessions() as checkpoint:
                    saved = await checkpoint.get(Gallery, gallery_id)
                    checkpoints.append(saved.vk_post_id)
            return ["photo1_1"]

        publisher = MagicMock(
            upload_photos=AsyncMock(side_effect=upload),
            publish=AsyncMock(return_value=42),
        )
        with patch.object(workers, "cleanup_files", new_callable=AsyncMock):
            async with self.sessions() as session:
                await workers.publish_gallery(
                    await session.get(Gallery, gallery_id), session, publisher
                )
        self.assertEqual(calls, 2)
        self.assertEqual(checkpoints, [42])

    async def test_recovered_public_post_is_not_created_again(self):
        gallery_id = await self.add_gallery(vk_post_id=42, local_images=["1.jpg"])
        publisher = MagicMock(upload_photos=AsyncMock(), publish=AsyncMock())
        with patch.object(workers, "cleanup_files", new_callable=AsyncMock):
            async with self.sessions() as session:
                await workers.publish_gallery(
                    await session.get(Gallery, gallery_id), session, publisher
                )
        publisher.publish.assert_not_awaited()
        publisher.upload_photos.assert_not_awaited()

    async def test_recovery_keeps_donut_images_separate(self):
        gallery_id = await self.add_gallery(vk_post_id=42)
        publisher = MagicMock(
            upload_photos=AsyncMock(return_value=["photo1_1"]),
            publish=AsyncMock(return_value=43),
        )
        with patch.object(workers, "cleanup_files", new_callable=AsyncMock):
            async with self.sessions() as session:
                await workers.publish_gallery(
                    await session.get(Gallery, gallery_id), session, publisher
                )
        publisher.upload_photos.assert_awaited_once_with(["5.jpg"])
        self.assertTrue(publisher.publish.call_args.kwargs["is_donut"])

    async def test_scheduling_failure_marks_gallery_failed_and_cleans_downloads(self):
        gallery_id = await self.add_gallery(status=PostStatus.PENDING)
        data = {"title": "Downloaded", "tags": {}, "local_images": ["download.jpg"]}
        with (
            patch.object(
                services.EhentaiHarvester,
                "parse_gallery",
                new_callable=AsyncMock,
                return_value=data,
            ),
            patch.object(
                services,
                "get_next_available_slot",
                side_effect=RuntimeError("schedule failed"),
            ),
            patch.object(services, "cleanup_files", new_callable=AsyncMock) as cleanup,
        ):
            with self.assertRaisesRegex(RuntimeError, "schedule failed"):
                await services.process_pending_gallery(gallery_id)
        cleanup.assert_awaited_once_with(["download.jpg"])
        async with self.sessions() as session:
            self.assertEqual(
                (await session.get(Gallery, gallery_id)).status, PostStatus.FAILED
            )

    async def test_schedule_is_rechecked_after_upload(self):
        gallery_id = await self.add_gallery(
            scheduled_for=datetime.now(timezone.utc) + timedelta(minutes=1),
            local_images=["1.jpg"],
        )
        publisher = MagicMock(
            upload_photos=AsyncMock(return_value=["photo1_1"]),
            publish=AsyncMock(return_value=42),
        )
        with patch.object(workers, "cleanup_files", new_callable=AsyncMock):
            async with self.sessions() as session:
                await workers.publish_gallery(
                    await session.get(Gallery, gallery_id), session, publisher
                )
        self.assertGreaterEqual(
            publisher.publish.call_args.kwargs["publish_date"],
            int((datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()),
        )


class ApiTests(unittest.IsolatedAsyncioTestCase):
    def request(self, payload):
        return MagicMock(
            json=AsyncMock(return_value=payload),
            method="POST",
            headers={"Authorization": "Bearer test-secret"},
        )

    async def test_invalid_json_shapes_and_boolean_are_rejected(self):
        for payload in [
            [],
            None,
            {},
            {"url": 123},
            {"url": "x", "include_cosplayer": "false"},
        ]:
            with (
                self.subTest(payload=payload),
                patch.object(api, "queue_gallery", new_callable=AsyncMock) as queue,
            ):
                response = await api.api_queue_handler(self.request(payload))
                self.assertEqual(response.status, 400)
                queue.assert_not_awaited()

    async def test_duplicate_is_conflict(self):
        with patch.object(
            api,
            "queue_gallery",
            side_effect=services.ServiceError("Gallery already exists"),
        ):
            response = await api.api_queue_handler(
                self.request({"url": "https://e-hentai.org/g/1/abc/"})
            )
        self.assertEqual(response.status, 409)

    async def test_internal_failure_is_500(self):
        with patch.object(
            api, "queue_gallery", side_effect=RuntimeError("database failure")
        ):
            response = await api.cors_middleware(
                self.request({"url": "x"}), api.api_queue_handler
            )
        self.assertEqual(response.status, 500)
        self.assertNotIn("database failure", response.text)
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], "*")

    async def test_auth_and_preflight(self):
        request = self.request({})
        request.headers = {}
        handler = AsyncMock(return_value=api.web.Response())
        response = await api.security_middleware(request, handler)
        self.assertEqual(response.status, 401)
        handler.assert_not_awaited()
        request.method = "OPTIONS"
        await api.security_middleware(request, handler)
        handler.assert_awaited_once()


class PublisherTests(unittest.IsolatedAsyncioTestCase):
    async def test_scraper_retry_callback_handles_network_failures(self):
        response = MagicMock(text=AsyncMock(return_value="gallery html"))
        session = MagicMock()
        successful_context = MagicMock()
        successful_context.__aenter__.return_value = response
        session.get.side_effect = [ClientConnectionError("offline"), successful_context]
        harvester = EhentaiHarvester()
        fetch = harvester.fetch_text.retry_with(wait=wait_none())
        self.assertEqual(
            await fetch(harvester, session, "https://e-hentai.org/g/1/abc/"),
            "gallery html",
        )

    async def test_missing_post_id_is_failure(self):
        publisher = VkPublisher(MagicMock())
        publisher._request = AsyncMock(return_value={})
        with self.assertRaises(RuntimeError):
            await publisher.publish("test", ["photo1_1"])

    async def test_vk_error_code_is_preserved(self):
        response = MagicMock(
            json=AsyncMock(
                return_value={"error": {"error_code": 100, "error_msg": "invalid date"}}
            )
        )
        session = MagicMock()
        session.post.return_value.__aenter__.return_value = response
        with self.assertRaises(VkAPIError) as caught:
            await VkPublisher(session)._request("wall.post", {})
        self.assertEqual(caught.exception.code, 100)

    async def test_network_retry_is_per_photo(self):
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / "image.png"
            image.write_bytes(b"image")
            response = MagicMock(
                json=AsyncMock(
                    return_value={"photo": "photo", "hash": "hash", "server": 1}
                )
            )
            session = MagicMock()
            session.post.return_value.__aenter__.return_value = response
            publisher = VkPublisher(session)
            publisher._request = AsyncMock(
                side_effect=[
                    ClientConnectionError("offline"),
                    {"upload_url": "https://upload.test"},
                    [{"owner_id": 1, "id": 2}],
                ]
            )
            upload = publisher._upload_photo.retry_with(wait=wait_none())
            self.assertEqual(await upload(publisher, str(image)), "photo1_2")
            self.assertEqual(publisher._request.await_count, 3)


if __name__ == "__main__":
    unittest.main()
