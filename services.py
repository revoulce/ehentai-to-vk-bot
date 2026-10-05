from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus
from scraper import EhentaiHarvester
from scheduling import calculate_next_slot
from storage import cleanup_files
from utils import normalize_gallery_url, process_tags


class ServiceError(Exception):
    pass


def generate_caption(gallery: Gallery, include_cosplayer: bool = False) -> str:
    """Constructs the VK post body from gallery metadata."""
    tags_dict = gallery.tags if isinstance(gallery.tags, dict) else {}
    lines = []

    def add_group(label: str, key: str) -> None:
        if key in tags_dict and tags_dict[key]:
            processed = process_tags(tags_dict[key])
            if processed:
                lines.append(f"{label}: {' '.join(processed)}")

    add_group("Фэндом", "parody")
    add_group("Персонаж", "character")

    if include_cosplayer:
        add_group("Модель", "cosplayer")

    return "\n".join(lines)


async def get_next_available_slot(
    from_time: datetime | None = None, *, exclude_gallery_id: int | None = None
) -> datetime:
    async with AsyncSessionLocal() as session:
        stmt = select(func.max(Gallery.scheduled_for)).where(
            or_(
                Gallery.status.in_([PostStatus.DOWNLOADED, PostStatus.POSTED]),
                Gallery.vk_post_id.is_not(None),
            )
        )
        if exclude_gallery_id is not None:
            stmt = stmt.where(Gallery.id != exclude_gallery_id)
        last_db_time = (await session.execute(stmt)).scalar()
    return calculate_next_slot(
        datetime.now(timezone.utc),
        last_db_time,
        settings.SCHEDULE_INTERVAL_HOURS,
        from_time,
    )


async def queue_gallery(url: str, include_cosplayer: bool = False) -> str:
    """
    Fast-path: inserts a placeholder record into the DB.
    Actual processing happens in the background downloader loop.
    """
    url = normalize_gallery_url(url)
    async with AsyncSessionLocal() as session:
        stmt = select(Gallery).where(Gallery.source_url == url)
        existing = (await session.execute(stmt)).scalar_one_or_none()
        if existing is not None:
            if existing.status != PostStatus.FAILED:
                raise ServiceError(
                    f"Gallery already exists (status: {existing.status.value})"
                )
            # Reuse the row and any saved public post; never reset its VK ID.
            can_resume = existing.vk_post_id is not None or (
                bool(existing.local_images)
                and all(Path(path).is_file() for path in existing.local_images)
            )
            values = {
                "status": PostStatus.DOWNLOADED if can_resume else PostStatus.PENDING,
            }
            if existing.vk_post_id is None:
                values["include_cosplayer"] = include_cosplayer
                values["scheduled_for"] = datetime.now(timezone.utc)
            if not can_resume:
                values["local_images"] = []
            # Only one concurrent request may reactivate a failed gallery.
            result = await session.execute(
                update(Gallery)
                .where(Gallery.id == existing.id, Gallery.status == PostStatus.FAILED)
                .values(**values)
                .execution_options(synchronize_session=False)
            )
            await session.commit()
            if result.rowcount != 1:
                raise ServiceError("Gallery already exists (retry already queued)")
            return "Requeued"

        new_gallery = Gallery(
            source_url=url,
            title="Pending Download...",
            tags={},
            local_images=[],
            status=PostStatus.PENDING,
            include_cosplayer=include_cosplayer,
            scheduled_for=datetime.now(timezone.utc),
        )
        session.add(new_gallery)
        try:
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            if (await session.execute(stmt)).scalar_one_or_none() is not None:
                raise ServiceError("Gallery already exists") from exc
            raise

    return "Queued"


async def process_pending_gallery(gallery_id: int) -> bool:
    """
    Worker task: scrapes metadata and downloads images for a queued gallery.
    Updates status to DOWNLOADED upon success.
    """
    harvester = EhentaiHarvester()

    async with AsyncSessionLocal() as session:
        gallery = await session.get(Gallery, gallery_id)
        if gallery is None or gallery.status != PostStatus.PENDING:
            return False

        downloaded_paths = []
        try:
            data = await harvester.parse_gallery(gallery.source_url)
            if not data:
                gallery.status = PostStatus.FAILED
                await session.commit()
                return False

            downloaded_paths = data["local_images"]
            gallery.title = data["title"]
            gallery.tags = data["tags"]
            gallery.local_images = data["local_images"]

            schedule_time = await get_next_available_slot()
            gallery.scheduled_for = schedule_time
            gallery.status = PostStatus.DOWNLOADED

            await session.commit()
            return True
        except Exception:
            await session.rollback()
            await cleanup_files(downloaded_paths)
            gallery = await session.get(Gallery, gallery_id)
            if gallery is None:
                raise
            gallery.status = PostStatus.FAILED
            await session.commit()
            raise


async def get_queue_status() -> str:
    async with AsyncSessionLocal() as session:
        rows = await session.execute(
            select(Gallery.status, func.count(Gallery.id)).group_by(Gallery.status)
        )
        counts = dict(rows.all())
    return (
        f"Queue Status:\n[PENDING] {counts.get(PostStatus.PENDING, 0)}"
        f"\n[READY TO UPLOAD] {counts.get(PostStatus.DOWNLOADED, 0)}"
        f"\n[FAILED] {counts.get(PostStatus.FAILED, 0)}"
        "\nTo retry a failed gallery, send /add <url> again."
    )
