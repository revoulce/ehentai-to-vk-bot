import asyncio
import sys
import random
from pathlib import Path
from datetime import datetime, timezone
from sqlalchemy import select
from loguru import logger

from config import settings
from models import init_db, AsyncSessionLocal, Gallery, PostStatus
from publisher import VkPublisher
from services import generate_caption, get_next_available_slot, process_pending_gallery
from tg_bot import start_bot

logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")


async def cleanup_files(file_paths: list[str]) -> None:
    """Offloads file deletion to a thread to avoid blocking the loop."""
    for path_str in file_paths:
        try:
            path = Path(path_str)
            if await asyncio.to_thread(path.exists):
                await asyncio.to_thread(path.unlink)
        except Exception as e:
            logger.warning(f"Failed to delete {path_str}: {e}")


async def downloader_loop() -> None:
    """Consumes the PENDING queue."""
    logger.info("Downloader started.")
    while True:
        try:
            async with AsyncSessionLocal() as session:
                # FIFO strategy
                stmt = (
                    select(Gallery.id)
                    .where(Gallery.status == PostStatus.PENDING)
                    .order_by(Gallery.id)
                    .limit(1)
                )
                gallery_id = (await session.execute(stmt)).scalar_one_or_none()

            if gallery_id:
                logger.info(f"Downloading ID: {gallery_id}")
                await process_pending_gallery(gallery_id)
                logger.info(f"Download complete ID: {gallery_id}")
                # Polite delay between scrapes
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(10)

        except Exception as e:
            logger.exception(f"Downloader error: {e}")
            await asyncio.sleep(10)


async def process_upload_queue() -> None:
    """Single iteration of the upload logic."""
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Gallery)
            .where(Gallery.status == PostStatus.DOWNLOADED)
            .order_by(Gallery.scheduled_for)
            .limit(1)
        )
        gallery = (await session.execute(stmt)).scalar_one_or_none()

        if not gallery:
            return

        logger.info(f"Uploading: {gallery.title}")

        # Validate Schedule Time
        now_utc = datetime.now(timezone.utc)
        target_time = gallery.scheduled_for
        if target_time.tzinfo is None:
            target_time = target_time.replace(tzinfo=timezone.utc)

        # If time is past or too close, reschedule
        if (target_time - now_utc).total_seconds() < 300:
            logger.warning(f"Rescheduling {gallery.title}...")
            new_time = await get_next_available_slot(from_time=now_utc)
            gallery.scheduled_for = new_time
            await session.commit()
            target_time = new_time

        publisher = VkPublisher()
        try:
            message = generate_caption(gallery)
            all_images = gallery.local_images
            selected_images = random.sample(all_images, min(len(all_images), 4))

            attachments = await publisher.upload_photos(selected_images)

            unix_time = int(target_time.timestamp())
            post_id = await publisher.publish(
                message, attachments, publish_date=unix_time
            )

            gallery.status = PostStatus.POSTED
            gallery.posted_at = datetime.now(timezone.utc)
            gallery.vk_post_id = post_id

            await session.commit()
            logger.success(f"Scheduled in VK: {gallery.title}")

            await cleanup_files(gallery.local_images)

        except Exception as e:
            logger.error(f"Upload failed for {gallery.id}: {e}")
            # Logic to mark as FAILED or retry could be added here


async def uploader_loop() -> None:
    """Consumes the DOWNLOADED queue."""
    logger.info("Uploader started.")
    while True:
        try:
            await process_upload_queue()
        except Exception as e:
            logger.exception(f"Uploader error: {e}")

        await asyncio.sleep(30)


async def main() -> None:
    await init_db()

    # TaskGroup manages the lifecycle of concurrent tasks
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(start_bot())
            tg.create_task(downloader_loop())
            tg.create_task(uploader_loop())
    except Exception as e:
        logger.critical(f"System crash: {e}")


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown.")
