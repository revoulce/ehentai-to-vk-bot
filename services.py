from datetime import datetime, timedelta

from sqlalchemy import select, func

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus
from scraper import EhentaiHarvester


class ServiceError(Exception):
    pass


async def add_gallery_task(url: str) -> str:
    """
    Orchestrates scraping and scheduling.
    Returns success message with schedule details.
    """
    harvester = EhentaiHarvester()

    async with AsyncSessionLocal() as session:
        stmt = select(Gallery).where(Gallery.source_url == url)
        if (await session.execute(stmt)).scalar():
            raise ServiceError("Gallery already exists in database.")

    data = await harvester.parse_gallery(url)
    if not data:
        raise ServiceError("Parsing failed or tags blacklisted.")

    async with AsyncSessionLocal() as session:
        # Determine next available slot
        last_post_stmt = select(func.max(Gallery.scheduled_for))
        last_time = (await session.execute(last_post_stmt)).scalar()

        now = datetime.now()
        if not last_time or last_time < now:
            schedule_time = now + timedelta(minutes=5)
        else:
            schedule_time = last_time + timedelta(minutes=settings.SCHEDULE_INTERVAL_MINUTES)

        new_gallery = Gallery(
            source_url=data["source_url"],
            title=data["title"],
            tags=data["tags"],
            local_images=data["local_images"],
            status=PostStatus.DOWNLOADED,
            scheduled_for=schedule_time
        )
        session.add(new_gallery)
        await session.commit()

    return f"Added: {data['title']}\nScheduled: {schedule_time.strftime('%Y-%m-%d %H:%M')}"


async def get_queue_status() -> str:
    async with AsyncSessionLocal() as session:
        count_stmt = select(func.count(Gallery.id)).where(Gallery.status == PostStatus.DOWNLOADED)
        count = (await session.execute(count_stmt)).scalar() or 0

        next_stmt = select(Gallery).where(
            Gallery.status == PostStatus.DOWNLOADED
        ).order_by(Gallery.scheduled_for).limit(1)

        next_post = (await session.execute(next_stmt)).scalar_one_or_none()

        lines = [f"Queue size: {count}"]
        if next_post:
            lines.append(f"Next: {next_post.title}")
            lines.append(f"Time: {next_post.scheduled_for.strftime('%H:%M')}")
        else:
            lines.append("Queue is empty.")

        return "\n".join(lines)
