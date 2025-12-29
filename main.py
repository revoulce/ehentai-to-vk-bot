# main.py
import asyncio
import sys
from datetime import datetime

from loguru import logger
from sqlalchemy import select

from config import settings
from models import init_db, AsyncSessionLocal, Gallery, PostStatus
from publisher import VkPublisher
from tg_bot import start_bot

# Logging setup
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")


async def process_scheduled_post() -> None:
    """
    Fetches one due post from DB and pushes to VK.
    Atomic operation per gallery.
    """
    async with AsyncSessionLocal() as session:
        stmt = select(Gallery).where(
            Gallery.status == PostStatus.DOWNLOADED,
            Gallery.scheduled_for <= datetime.now()
        ).limit(1)

        gallery = (await session.execute(stmt)).scalar_one_or_none()

        if not gallery:
            return

        logger.info(f"Processing gallery: {gallery.title}")
        publisher = VkPublisher()

        try:
            # VK limits: max 10 attachments, strict text length
            hashtags = [f"#{t.replace(' ', '_')}" for t in gallery.tags if len(t) > 2]
            msg = f"{gallery.title}\n\n{' '.join(hashtags[:10])}\n\nSource: {gallery.source_url}"

            attachments = await publisher.upload_photos(gallery.local_images)
            await publisher.publish(msg, attachments)

            gallery.status = PostStatus.POSTED
            await session.commit()
            logger.success(f"Published: {gallery.title}")

        except Exception as e:
            logger.error(f"Publishing failed for {gallery.id}: {e}")
            gallery.status = PostStatus.FAILED
            await session.commit()


async def scheduler_loop() -> None:
    """Background cron-like task."""
    logger.info("Scheduler started.")
    while True:
        try:
            await process_scheduled_post()
        except Exception as e:
            logger.exception(f"Scheduler critical error: {e}")

        await asyncio.sleep(60 * settings.SCHEDULE_INTERVAL_MINUTES)


async def main() -> None:
    await init_db()

    # TaskGroup for modern structured concurrency (Py3.11+)
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(start_bot())
            tg.create_task(scheduler_loop())
    except Exception as e:
        logger.critical(f"System crash: {e}")


if __name__ == "__main__":
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown.")
