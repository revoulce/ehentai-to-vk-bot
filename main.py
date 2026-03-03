# main.py
import asyncio
import sys
import random
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Callable, Awaitable

from aiohttp import web
from sqlalchemy import select
from loguru import logger

from config import settings
from models import init_db, AsyncSessionLocal, Gallery, PostStatus
from publisher import VkPublisher
from tg_bot import start_bot
from services import (
    queue_gallery,
    process_pending_gallery,
    generate_caption,
    get_next_available_slot,
    ServiceError,
)

# --- Logging Configuration ---


# Filter to suppress "BadHttpMessage" (Scanner noise) from aiohttp.server
class AiohttpScannerFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        # Suppress "Pause on PRI" and "BadHttpMessage" errors caused by port scanners
        if "BadHttpMessage" in msg or "Pause on PRI" in msg:
            return False
        return True


# Configure Loguru
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")

# Apply filter to standard python logging used by aiohttp
logging.getLogger("aiohttp.server").addFilter(AiohttpScannerFilter())

# --- Middleware ---


@web.middleware
async def cors_middleware(
    request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]
) -> web.Response:
    if request.method == "OPTIONS":
        response = web.Response()
    else:
        try:
            response = await handler(request)
        except web.HTTPException as ex:
            response = ex
        except Exception as e:
            logger.exception(f"API Handler Exception: {e}")
            response = web.json_response(
                {"status": "error", "message": "Internal Server Error"}, status=500
            )

    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
    return response


@web.middleware
async def security_middleware(
    request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]
) -> web.Response:
    """
    Blocks unauthorized access with a custom message.
    """
    # Allow CORS preflight (OPTIONS) without auth
    if request.method == "OPTIONS":
        return await handler(request)

    auth_header = request.headers.get("Authorization")
    expected_auth = f"Bearer {settings.API_SECRET.get_secret_value()}"

    if auth_header != expected_auth:
        # Log warning with IP
        logger.warning(f"Unauthorized access attempt from {request.remote}")
        # Explicit rejection
        return web.Response(text="Иди нахуй.", status=401)

    return await handler(request)


# --- API Handlers ---


async def api_queue_handler(request: web.Request) -> web.Response:
    # Auth is handled by security_middleware
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
        logger.error(f"API Request Error: {e}")
        return web.json_response(
            {"status": "error", "message": "Bad Request"}, status=400
        )


async def start_api_server() -> None:
    # Middleware order: CORS first (to handle OPTIONS), then Security (to block others)
    app = web.Application(middlewares=[cors_middleware, security_middleware])
    app.router.add_post("/api/queue", api_queue_handler)

    # access_log=None disables the "200 OK" spam in console
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", settings.API_PORT)

    logger.info(f"API Server listening on 0.0.0.0:{settings.API_PORT}")
    await site.start()

    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


# --- Background Workers ---


async def cleanup_files(file_paths: list[str]) -> None:
    for path_str in file_paths:
        try:
            path = Path(path_str)
            if await asyncio.to_thread(path.exists):
                await asyncio.to_thread(path.unlink)
        except Exception as e:
            logger.warning(f"Cleanup failed for {path_str}: {e}")


async def downloader_loop() -> None:
    logger.info("Downloader worker started.")
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
                logger.info(f"Downloading Gallery ID: {gallery_id}")
                await process_pending_gallery(gallery_id)
                logger.success(f"Download complete ID: {gallery_id}")
                await asyncio.sleep(5)
            else:
                await asyncio.sleep(10)
        except Exception as e:
            logger.exception(f"Downloader crash: {e}")
            await asyncio.sleep(10)


async def uploader_loop() -> None:
    logger.info("Uploader worker started.")
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
                    logger.info(f"Processing Upload: {gallery.title}")

                    now_utc = datetime.now(timezone.utc)
                    target_time = gallery.scheduled_for
                    if target_time.tzinfo is None:
                        target_time = target_time.replace(tzinfo=timezone.utc)

                    if (target_time - now_utc).total_seconds() < 300:
                        new_time = await get_next_available_slot(from_time=now_utc)
                        gallery.scheduled_for = new_time
                        await session.commit()
                        target_time = new_time
                        logger.info(f"Rescheduled to {new_time}")

                    publisher = VkPublisher()
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

                    logger.success(f"Scheduled in VK: {gallery.title}")
                    await cleanup_files(gallery.local_images)

        except Exception as e:
            logger.exception(f"Uploader crash: {e}")

        await asyncio.sleep(30)


async def main() -> None:
    await init_db()

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(start_bot())
            tg.create_task(downloader_loop())
            tg.create_task(uploader_loop())
            tg.create_task(start_api_server())
    except Exception as e:
        logger.critical(f"System critical failure: {e}")


if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown.")
