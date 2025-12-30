import asyncio
import sys
from datetime import datetime, timezone

from loguru import logger
from sqlalchemy import select

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus, init_db
from publisher import VkPublisher
from tg_bot import start_bot

logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")


async def process_scheduled_post() -> None:
    async with AsyncSessionLocal() as session:
        stmt = (
            select(Gallery)
            .where(
                Gallery.status == PostStatus.DOWNLOADED,
                Gallery.scheduled_for <= datetime.now(timezone.utc),
            )
            .limit(1)
        )

        gallery = (await session.execute(stmt)).scalar_one_or_none()

        if not gallery:
            return

        logger.info(f"Publishing: {gallery.title}")
        publisher = VkPublisher()

        try:
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
    logger.info("Scheduler started.")
    while True:
        try:
            await process_scheduled_post()
        except Exception as e:
            logger.exception(f"Scheduler error: {e}")

        await asyncio.sleep(60 * settings.SCHEDULE_INTERVAL_MINUTES)


async def main() -> None:
    await init_db()

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(start_bot())
            tg.create_task(scheduler_loop())
    except Exception as e:
        logger.critical(f"System crash: {e}")


if __name__ == "__main__":
    try:
        # Keep strict asyncio mode
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown.")
