import enum
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine
from sqlalchemy.engine import make_url
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
    tags: Mapped[dict[str, list[str]]] = mapped_column(JSON, default=dict)
    local_images: Mapped[list[str]] = mapped_column(JSON, default=list)

    status: Mapped[PostStatus] = mapped_column(
        SQLEnum(PostStatus), default=PostStatus.PENDING
    )

    include_cosplayer: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    scheduled_for: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    posted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    vk_post_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


engine = create_async_engine(settings.DB_URL, echo=False)

AsyncSessionLocal = async_sessionmaker(
    bind=engine, expire_on_commit=False, autoflush=False
)


async def init_db() -> None:
    database_url = make_url(settings.DB_URL)
    if database_url.get_backend_name() == "sqlite" and database_url.database not in (
        None,
        "",
        ":memory:",
    ):
        Path(database_url.database).parent.mkdir(parents=True, exist_ok=True)
    settings.STORAGE_PATH.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
