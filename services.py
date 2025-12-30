from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func

from models import AsyncSessionLocal, Gallery, PostStatus
from scraper import EhentaiHarvester
from utils import process_tags


class ServiceError(Exception):
    pass


def generate_caption(gallery: Gallery) -> str:
    """Constructs the VK post body from gallery metadata."""
    tags_dict = gallery.tags
    lines = [gallery.title, ""]

    def add_group(label: str, key: str) -> None:
        if key in tags_dict and tags_dict[key]:
            processed = process_tags(tags_dict[key])
            if processed:
                lines.append(f"{label}: {' '.join(processed)}")

    add_group("Фэндом", "parody")
    add_group("Персонаж", "character")
    add_group("Модель", "cosplayer")

    lines.append("")
    lines.append(f"Source: {gallery.source_url}")
    return "\n".join(lines)


async def get_next_available_slot(from_time: datetime | None = None) -> datetime:
    """
    Determines the next valid hourly slot for scheduling.
    Ensures a minimum 5-minute buffer from the current time.
    """
    now_utc = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        stmt = select(func.max(Gallery.scheduled_for))
        last_db_time = (await session.execute(stmt)).scalar()

        if last_db_time and last_db_time.tzinfo is None:
            last_db_time = last_db_time.replace(tzinfo=timezone.utc)

    # Determine baseline: either the requested time, the last scheduled post, or now
    if from_time:
        base_time = max(from_time, now_utc)
    else:
        base_time = max(last_db_time, now_utc) if last_db_time else now_utc

    # Round up to the next full hour
    next_hour = base_time.replace(minute=0, second=0, microsecond=0) + timedelta(
        hours=1
    )

    # VK API requires publish_date to be strictly in the future
    if (next_hour - now_utc).total_seconds() < 300:
        next_hour += timedelta(hours=1)

    return next_hour


async def queue_gallery(url: str) -> str:
    """
    Fast-path: inserts a placeholder record into the DB.
    Actual processing happens in the background downloader loop.
    """
    async with AsyncSessionLocal() as session:
        stmt = select(Gallery).where(Gallery.source_url == url)
        if (await session.execute(stmt)).scalar():
            raise ServiceError("Gallery already exists")

        new_gallery = Gallery(
            source_url=url,
            title="Pending Download...",
            tags=[],
            local_images=[],
            status=PostStatus.PENDING,
            # Placeholder time, updated after download
            scheduled_for=datetime.now(timezone.utc),
        )
        session.add(new_gallery)
        await session.commit()

    return "Queued"


async def process_pending_gallery(gallery_id: int) -> None:
    """
    Worker task: scrapes metadata and downloads images for a queued gallery.
    Updates status to DOWNLOADED upon success.
    """
    harvester = EhentaiHarvester()

    async with AsyncSessionLocal() as session:
        gallery = await session.get(Gallery, gallery_id)
        if not gallery:
            return

        try:
            data = await harvester.parse_gallery(gallery.source_url)
            if not data:
                gallery.status = PostStatus.FAILED
                await session.commit()
                return

            gallery.title = data["title"]
            gallery.tags = data["tags"]
            gallery.local_images = data["local_images"]

            # Calculate actual schedule slot only after successful download
            schedule_time = await get_next_available_slot()
            gallery.scheduled_for = schedule_time
            gallery.status = PostStatus.DOWNLOADED

            await session.commit()
        except Exception:
            gallery.status = PostStatus.FAILED
            await session.commit()
            raise


async def get_queue_status() -> str:
    async with AsyncSessionLocal() as session:
        pending = (
            await session.execute(
                select(func.count(Gallery.id)).where(
                    Gallery.status == PostStatus.PENDING
                )
            )
        ).scalar()
        downloaded = (
            await session.execute(
                select(func.count(Gallery.id)).where(
                    Gallery.status == PostStatus.DOWNLOADED
                )
            )
        ).scalar()
        return f"Queue Status:\n[PENDING] {pending}\n[READY TO UPLOAD] {downloaded}"
