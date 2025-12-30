import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus
from scraper import EhentaiHarvester
from utils import process_tags


class ServiceError(Exception):
    pass


def generate_caption(gallery: Gallery) -> str:
    tags_dict = gallery.tags
    lines = [gallery.title, ""]

    def add_group(label: str, key: str):
        if key in tags_dict and tags_dict[key]:
            processed = process_tags(tags_dict[key])
            if processed:
                lines.append(f"{label}: {' '.join(processed)}")

    add_group("Фэндом", "parody")
    add_group("Персонаж", "character")
    add_group("Модель", "cosplayer")

    return "\n".join(lines)


async def get_next_available_slot(from_time: datetime | None = None) -> datetime:
    """
    Calculates the next valid round hour slot.
    If from_time is provided, it searches after that time.
    Otherwise, it searches after the last scheduled post in DB.
    """
    now_utc = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        # Find the absolute latest scheduled time in the system
        stmt = select(func.max(Gallery.scheduled_for))
        last_db_time = (await session.execute(stmt)).scalar()

        if last_db_time and last_db_time.tzinfo is None:
            last_db_time = last_db_time.replace(tzinfo=timezone.utc)

    # Determine the baseline to start counting from
    # If we are rescheduling a specific post, we don't care about last_db_time,
    # we just want the next slot relative to NOW or the provided time.
    if from_time:
        base_time = max(from_time, now_utc)
    else:
        # New post logic: append to the end of the queue
        base_time = max(last_db_time, now_utc) if last_db_time else now_utc

    # Round up to next hour
    # Example: 14:05 -> 15:00. 14:59 -> 15:00.
    next_hour = base_time.replace(minute=0, second=0, microsecond=0) + timedelta(
        hours=1
    )

    # Safety: Ensure we are at least 5 minutes in the future for VK API
    if (next_hour - now_utc).total_seconds() < 300:
        next_hour += timedelta(hours=1)

    return next_hour


async def add_gallery_task(url: str) -> str:
    harvester = EhentaiHarvester()

    async with AsyncSessionLocal() as session:
        stmt = select(Gallery).where(Gallery.source_url == url)
        if (await session.execute(stmt)).scalar():
            raise ServiceError("Gallery already exists.")

    data = await harvester.parse_gallery(url)
    if not data:
        raise ServiceError("Parsing failed.")

    schedule_time = await get_next_available_slot()

    async with AsyncSessionLocal() as session:
        new_gallery = Gallery(
            source_url=data["source_url"],
            title=data["title"],
            tags=data["tags"],
            local_images=data["local_images"],
            status=PostStatus.DOWNLOADED,
            scheduled_for=schedule_time,
        )
        session.add(new_gallery)
        await session.commit()

    return f"Added: {data['title']}\nScheduled: {schedule_time.strftime('%Y-%m-%d %H:%M UTC')}"


async def get_queue_status() -> str:
    async with AsyncSessionLocal() as session:
        count = (
            await session.execute(
                select(func.count(Gallery.id)).where(
                    Gallery.status == PostStatus.DOWNLOADED
                )
            )
        ).scalar()
        return f"Pending upload to VK: {count}"
