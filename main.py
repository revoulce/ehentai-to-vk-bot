import asyncio
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import select

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus, init_db
from publisher import VkPublisher
from services import generate_caption, get_next_available_slot
from tg_bot import start_bot

logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")


async def cleanup_files(file_paths: list[str]) -> None:
    """Non-blocking file deletion."""
    for path_str in file_paths:
        try:
            path = Path(path_str)
            if await asyncio.to_thread(path.exists):
                await asyncio.to_thread(path.unlink)
        except Exception as e:
            logger.warning(f"Failed to delete {path_str}: {e}")


async def process_queue_item() -> None:
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

        logger.info(f"Processing: {gallery.title}")

        # --- Time Validation ---
        now_utc = datetime.now(timezone.utc)
        target_time = gallery.scheduled_for
        if target_time.tzinfo is None:
            target_time = target_time.replace(tzinfo=timezone.utc)

        if (target_time - now_utc).total_seconds() < 300:
            logger.warning(f"Time {target_time} is invalid. Rescheduling...")
            new_time = await get_next_available_slot(from_time=now_utc)
            gallery.scheduled_for = new_time
            await session.commit()
            logger.info(f"Rescheduled to: {new_time}")
            target_time = new_time
        # -----------------------

        publisher = VkPublisher()

        try:
            message = generate_caption(gallery)

            # Select random images
            all_images = gallery.local_images
            selected_images = random.sample(all_images, min(len(all_images), 4))

            # Upload
            attachments = await publisher.upload_photos(selected_images)

            # Publish (Schedule)
            unix_time = int(target_time.timestamp())
            post_id = await publisher.publish(
                message, attachments, publish_date=unix_time
            )

            # Update DB
            gallery.status = PostStatus.POSTED
            gallery.posted_at = datetime.now(timezone.utc)
            gallery.vk_post_id = post_id

            await session.commit()
            logger.success(f"Scheduled in VK: {gallery.title} at {target_time}")

            # Cleanup local files
            await cleanup_files(gallery.local_images)

        except Exception as e:
            logger.error(f"Failed to process {gallery.id}: {e}")


async def worker_loop() -> None:
    logger.info("Worker started.")
    while True:
        try:
            await process_queue_item()
        except Exception as e:
            logger.exception(f"Worker error: {e}")

        await asyncio.sleep(30)


async def main() -> None:
    await init_db()

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(start_bot())
            tg.create_task(worker_loop())
    except Exception as e:
        logger.critical(f"System crash: {e}")


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown.")
