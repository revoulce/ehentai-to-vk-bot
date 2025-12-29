import enum
from datetime import datetime
from typing import List

from sqlalchemy import String, JSON, DateTime, Enum as SQLEnum
from sqlalchemy.ext.asyncio import AsyncAttrs, create_async_engine, async_sessionmaker
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
    # Storing tags as JSON list for simplicity in SQLite
    tags: Mapped[List[str]] = mapped_column(JSON)
    local_images: Mapped[List[str]] = mapped_column(JSON, default=list)

    status: Mapped[PostStatus] = mapped_column(SQLEnum(PostStatus), default=PostStatus.PENDING)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    scheduled_for: Mapped[datetime] = mapped_column(DateTime, index=True)


engine = create_async_engine(settings.DB_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
