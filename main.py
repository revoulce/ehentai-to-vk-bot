# main.py
import asyncio
import sys
from aiohttp import web
import aiohttp_cors
from loguru import logger

from config import settings
from models import init_db
from tg_bot import start_bot
from services import queue_gallery, ServiceError

# Import existing loops
from services import process_pending_gallery  # Used in downloader
from services import generate_caption, get_next_available_slot  # Used in uploader
from models import AsyncSessionLocal, Gallery, PostStatus
from sqlalchemy import select
from publisher import VkPublisher
from pathlib import Path
import random
from datetime import datetime, timezone

# --- Re-declaring loops for context (assuming they exist in main.py as per previous state) ---
# ... [downloader_loop and uploader_loop implementation from previous step] ...

# --- API Implementation ---


async def api_queue_handler(request: web.Request) -> web.Response:
    auth = request.headers.get("Authorization")
    if auth != f"Bearer {settings.API_SECRET.get_secret_value()}":
        return web.json_response({"error": "Unauthorized"}, status=401)

    try:
        data = await request.json()
        url = data.get("url")
        if not url:
            return web.json_response({"error": "Missing URL"}, status=400)

        result = await queue_gallery(url)
        logger.info(f"API Queued: {url}")
        return web.json_response({"status": "success", "message": result})

    except ServiceError as e:
        return web.json_response({"status": "error", "message": str(e)}, status=409)
    except Exception as e:
        logger.error(f"API Error: {e}")
        return web.json_response(
            {"status": "error", "message": "Internal Error"}, status=500
        )


async def start_api_server() -> None:
    app = web.Application()
    app.router.add_post("/api/queue", api_queue_handler)

    # CORS setup is mandatory for browser extensions interacting with localhost
    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
            )
        },
    )

    for route in list(app.router.routes()):
        cors.add(route)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, settings.API_HOST, settings.API_PORT)

    logger.info(f"API Server running on port {settings.API_PORT}")
    await site.start()

    # Keep task alive
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


# --- Main ---


async def cleanup_files(file_paths: list[str]) -> None:
    for path_str in file_paths:
        try:
            path = Path(path_str)
            if await asyncio.to_thread(path.exists):
                await asyncio.to_thread(path.unlink)
        except Exception as e:
            logger.warning(f"Failed to delete {path_str}: {e}")


async def downloader_loop() -> None:
    logger.info("Downloader started.")
    while True:
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    select(Gallery.id)
                    .where(Gallery.status == PostStatus.PENDING)
                    .order_by(Gallery.id)
                    .limit(1)
                )
                gallery_id = (await session.execute(stmt)).scalar_one_or_none()

            if gallery_id:
                logger.info(f"Downloading ID: {gallery_id}")
                # Import here to avoid circular dependency issues if any
                from services import process_pending_gallery

                await process_pending_gallery(gallery_id)
                logger.info(f"Download complete ID: {gallery_id}")
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            logger.exception(f"Downloader error: {e}")
            await asyncio.sleep(10)


async def uploader_loop() -> None:
    logger.info("Uploader started.")
    while True:
        try:
            async with AsyncSessionLocal() as session:
                stmt = (
                    select(Gallery)
                    .where(Gallery.status == PostStatus.DOWNLOADED)
                    .order_by(Gallery.scheduled_for)
                    .limit(1)
                )
                gallery = (await session.execute(stmt)).scalar_one_or_none()

                if gallery:
                    logger.info(f"Uploading: {gallery.title}")

                    now_utc = datetime.now(timezone.utc)
                    target_time = gallery.scheduled_for
                    if target_time.tzinfo is None:
                        target_time = target_time.replace(tzinfo=timezone.utc)

                    if (target_time - now_utc).total_seconds() < 300:
                        from services import get_next_available_slot

                        new_time = await get_next_available_slot(from_time=now_utc)
                        gallery.scheduled_for = new_time
                        await session.commit()
                        target_time = new_time

                    publisher = VkPublisher()
                    from services import generate_caption

                    message = generate_caption(gallery)
                    all_images = gallery.local_images
                    selected_images = random.sample(all_images, min(len(all_images), 4))

                    attachments = await publisher.upload_photos(selected_images)
                    unix_time = int(target_time.timestamp())
                    post_id = await publisher.publish(
                        message, attachments, publish_date=unix_time
                    )

                    gallery.status = PostStatus.POSTED
                    gallery.posted_at = datetime.now(timezone.utc)
                    gallery.vk_post_id = post_id
                    await session.commit()

                    await cleanup_files(gallery.local_images)

        except Exception as e:
            logger.exception(f"Uploader error: {e}")

        await asyncio.sleep(30)


async def main() -> None:
    await init_db()

    # Configure logging
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(start_bot())
            tg.create_task(downloader_loop())
            tg.create_task(uploader_loop())
            tg.create_task(start_api_server())
    except Exception as e:
        logger.critical(f"System crash: {e}")


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown.")
