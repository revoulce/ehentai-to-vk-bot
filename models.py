import enum
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import JSON, DateTime, Integer, String
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import settings


class Base(AsyncAttrs, DeclarativeBase):
    pass


class PostStatus(str, enum.Enum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    POSTED = "posted"
    FAILED = "failed"


class Gallery(Base):
    __tablename__ = "galleries"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_url: Mapped[str] = mapped_column(String, unique=True, index=True)
    title: Mapped[str] = mapped_column(String)
    tags: Mapped[List[str]] = mapped_column(JSON)  # Dict stored as JSON
    local_images: Mapped[List[str]] = mapped_column(JSON, default=list)

    status: Mapped[PostStatus] = mapped_column(
        SQLEnum(PostStatus), default=PostStatus.PENDING
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # Planned time (Target)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # Actual execution time (Result)
    posted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    vk_post_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


engine = create_async_engine(settings.DB_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(
    bind=engine, expire_on_commit=False, autoflush=False
)


async def init_db() -> None:
    async with engine.begin() as conn:
        # Note: Alembic is recommended for migrations in prod,
        # but for this script, create_all handles new tables only.
        # To add columns to existing DB without dropping, you might need to manually alter table or delete db file.
        await conn.run_sync(Base.metadata.create_all)
