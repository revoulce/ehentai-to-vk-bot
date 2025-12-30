from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus
from scraper import EhentaiHarvester


class ServiceError(Exception):
    pass


async def add_gallery_task(url: str) -> str:
    """
    Orchestrates scraping and scheduling.
    Atomic DB check-then-insert.
    """
    harvester = EhentaiHarvester()

    # 1. Pre-check
    async with AsyncSessionLocal() as session:
        stmt = select(Gallery).where(Gallery.source_url == url)
        if (await session.execute(stmt)).scalar():
            raise ServiceError("Gallery already exists.")

    # 2. Scrape (Time consuming)
    data = await harvester.parse_gallery(url)
    if not data:
        raise ServiceError("Parsing failed or tags blacklisted.")

    # 3. Schedule & Save
    async with AsyncSessionLocal() as session:
        # Calculate schedule time based on last scheduled post
        last_post_stmt = select(func.max(Gallery.scheduled_for))
        last_time = (await session.execute(last_post_stmt)).scalar()

        now_utc = datetime.now(timezone.utc)

        # Ensure last_time is timezone-aware
        if last_time and last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone.utc)

        if not last_time or last_time < now_utc:
            schedule_time = now_utc + timedelta(minutes=5)
        else:
            schedule_time = last_time + timedelta(
                minutes=settings.SCHEDULE_INTERVAL_MINUTES
            )

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
        count_stmt = select(func.count(Gallery.id)).where(
            Gallery.status == PostStatus.DOWNLOADED
        )
        count = (await session.execute(count_stmt)).scalar() or 0

        next_stmt = (
            select(Gallery)
            .where(Gallery.status == PostStatus.DOWNLOADED)
            .order_by(Gallery.scheduled_for)
            .limit(1)
        )

        next_post = (await session.execute(next_stmt)).scalar_one_or_none()

        lines = [f"Queue size: {count}"]
        if next_post:
            ts = next_post.scheduled_for.strftime("%H:%M UTC")
            lines.append(f"Next: {next_post.title}")
            lines.append(f"Time: {ts}")
        else:
            lines.append("Queue is empty.")

        return "\n".join(lines)
