import asyncio
import random
from datetime import datetime, timezone

import aiohttp
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import AsyncSessionLocal, Gallery, PostStatus
from publisher import VkPublisher
from scheduling import SCHEDULE_BUFFER, as_utc
from services import generate_caption, get_next_available_slot, process_pending_gallery
from storage import cleanup_files


async def downloader_loop() -> None:
    logger.info("Downloader worker started")
    while True:
        delay = 10
        try:
            async with AsyncSessionLocal() as session:
                gallery_id = (
                    await session.execute(
                        select(Gallery.id)
                        .where(Gallery.status == PostStatus.PENDING)
                        .order_by(Gallery.id)
                        .limit(1)
                    )
                ).scalar_one_or_none()
            if gallery_id is not None:
                if await process_pending_gallery(gallery_id):
                    logger.success("Downloaded gallery {}", gallery_id)
                else:
                    logger.warning("Gallery {} could not be downloaded", gallery_id)
                delay = 5
        except Exception:
            logger.exception("Downloader failed")
        await asyncio.sleep(delay)


async def refresh_schedule(gallery: Gallery, session: AsyncSession) -> datetime:
    target = as_utc(gallery.scheduled_for)
    now = datetime.now(timezone.utc)
    if target - now < SCHEDULE_BUFFER:
        target = await get_next_available_slot(
            from_time=now, exclude_gallery_id=gallery.id
        )
        gallery.scheduled_for = target
        await session.commit()
        logger.info("Rescheduled gallery {} to {}", gallery.id, target)
    return target


async def publish_gallery(
    gallery: Gallery, session: AsyncSession, publisher: VkPublisher
) -> None:
    """Persist the main post before attempting the optional Donut continuation."""
    images = list(gallery.local_images)
    if not images and gallery.vk_post_id is None:
        raise RuntimeError("Gallery has no images to publish")
    if gallery.vk_post_id is None:
        random.shuffle(images)
        # Recovery must keep the same public/Donut split after the main post is saved.
        gallery.local_images = images
        await session.commit()
    public_images, donut_images = images[:4], images[4:13]
    message = generate_caption(gallery, include_cosplayer=gallery.include_cosplayer)

    if gallery.vk_post_id is None:
        attachments = await publisher.upload_photos(public_images)
        if not attachments:
            raise RuntimeError("VK rejected all public images")
        # Uploading can take minutes; check the time immediately before wall.post.
        target = await refresh_schedule(gallery, session)
        gallery.vk_post_id = await publisher.publish(
            message, attachments, publish_date=int(target.timestamp())
        )
        await session.commit()

    # Donut is best effort: its failures must never repeat the main publication.
    if donut_images:
        try:
            attachments = await publisher.upload_photos(donut_images)
            if attachments:
                donut_time = max(
                    as_utc(gallery.scheduled_for),
                    datetime.now(timezone.utc) + SCHEDULE_BUFFER,
                )
                await publisher.publish(
                    f"{message}\n\n⭐ Эксклюзивное продолжение для Донов",
                    attachments,
                    publish_date=int(donut_time.timestamp()) + 60,
                    is_donut=True,
                )
            else:
                logger.warning(
                    "VK rejected all Donut images for gallery {}", gallery.id
                )
        except Exception:
            logger.exception("Donut publication failed for gallery {}", gallery.id)

    gallery.status = PostStatus.POSTED
    gallery.posted_at = datetime.now(timezone.utc)
    await session.commit()
    await cleanup_files(gallery.local_images)
    logger.success("Scheduled gallery {} in VK", gallery.id)


async def uploader_loop() -> None:
    logger.info("Uploader worker started")
    timeout = aiohttp.ClientTimeout(total=120, connect=10, sock_read=30)
    async with aiohttp.ClientSession(timeout=timeout) as http_session:
        publisher = VkPublisher(http_session)
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    gallery = (
                        await session.execute(
                            select(Gallery)
                            .where(Gallery.status == PostStatus.DOWNLOADED)
                            .order_by(Gallery.scheduled_for, Gallery.id)
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    if gallery is not None:
                        await publish_gallery(gallery, session, publisher)
            except Exception:
                logger.exception("Uploader failed")
            await asyncio.sleep(30)
