import asyncio
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from aiohttp import web
from loguru import logger
from sqlalchemy import select

from config import settings
from models import AsyncSessionLocal, Gallery, PostStatus, init_db
from publisher import VkPublisher
from services import (
    ServiceError,
    generate_caption,
    get_next_available_slot,
    process_pending_gallery,
    queue_gallery,
)
from tg_bot import start_bot

# --- Logging Setup ---
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("bot.log", rotation="10 MB", level="DEBUG", compression="zip")


# --- CORS Middleware ---
@web.middleware
async def cors_middleware(
    request: web.Request, handler: Callable[[web.Request], Awaitable[web.Response]]
) -> web.Response:
    """
    Manual CORS handling to avoid external dependency issues.
    Handles preflight OPTIONS and adds headers to all responses.
    """
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


# --- API Handlers ---
async def api_queue_handler(request: web.Request) -> web.Response:
    auth_header = request.headers.get("Authorization")
    expected_auth = f"Bearer {settings.API_SECRET.get_secret_value()}"

    if auth_header != expected_auth:
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
        logger.error(f"API Request Error: {e}")
        return web.json_response(
            {"status": "error", "message": "Bad Request"}, status=400
        )


async def start_api_server() -> None:
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_post("/api/queue", api_queue_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    # Force bind to 0.0.0.0 to ensure Docker visibility
    site = web.TCPSite(runner, "0.0.0.0", settings.API_PORT)

    logger.info(f"API Server listening on 0.0.0.0:{settings.API_PORT}")
    await site.start()

    try:
        # Keep the server running indefinitely
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
                # FIFO Queue processing
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
                await asyncio.sleep(5)  # Rate limiting
            else:
                await asyncio.sleep(10)  # Idle wait

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

                    # Time Validation
                    now_utc = datetime.now(timezone.utc)
                    target_time = gallery.scheduled_for
                    if target_time.tzinfo is None:
                        target_time = target_time.replace(tzinfo=timezone.utc)

                    # Ensure future scheduling (VK requirement)
                    if (target_time - now_utc).total_seconds() < 300:
                        new_time = await get_next_available_slot(from_time=now_utc)
                        gallery.scheduled_for = new_time
                        await session.commit()
                        target_time = new_time
                        logger.info(f"Rescheduled to {new_time}")

                    # Execution
                    publisher = VkPublisher()
                    message = generate_caption(gallery)

                    # Select 4 random images
                    all_images = gallery.local_images
                    selected_images = random.sample(all_images, min(len(all_images), 4))

                    attachments = await publisher.upload_photos(selected_images)
                    unix_time = int(target_time.timestamp())

                    post_id = await publisher.publish(
                        message, attachments, publish_date=unix_time
                    )

                    # Finalize
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
