import asyncio
import hmac
from collections.abc import Awaitable, Callable

from aiohttp import web
from loguru import logger
from pydantic import BaseModel, ConfigDict, ValidationError

from config import settings
from services import ServiceError, queue_gallery

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


class QueueRequest(BaseModel):
    model_config = ConfigDict(strict=True)
    url: str
    include_cosplayer: bool = False


def error_response(message: str, status: int) -> web.Response:
    return web.json_response({"status": "error", "message": message}, status=status)


@web.middleware
async def cors_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = error_response(exc.reason, exc.status)
        except Exception:
            logger.exception("API handler failed")
            response = error_response("Internal Server Error", 500)
    response.headers.update(
        {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
        }
    )
    return response


@web.middleware
async def security_middleware(
    request: web.Request, handler: Handler
) -> web.StreamResponse:
    if request.method == "OPTIONS":
        return await handler(request)
    expected = f"Bearer {settings.API_SECRET.get_secret_value()}".encode()
    actual = request.headers.get("Authorization", "").encode()
    if not hmac.compare_digest(actual, expected):
        logger.warning("Unauthorized API access from {}", request.remote)
        return error_response("Unauthorized", 401)
    return await handler(request)


async def api_queue_handler(request: web.Request) -> web.Response:
    try:
        payload = QueueRequest.model_validate(await request.json())
    except (ValueError, ValidationError):
        return error_response("Expected URL and a boolean include_cosplayer", 400)
    try:
        result = await queue_gallery(payload.url, payload.include_cosplayer)
    except ValueError as exc:
        return error_response(str(exc), 400)
    except ServiceError as exc:
        return error_response(str(exc), 409)
    logger.info("API queued: {}", payload.url)
    return web.json_response({"status": "success", "message": result})


def create_app() -> web.Application:
    app = web.Application(middlewares=[cors_middleware, security_middleware])
    app.router.add_post("/api/queue", api_queue_handler)
    return app


async def start_api_server() -> None:
    runner = web.AppRunner(create_app(), access_log=None)
    await runner.setup()
    try:
        site = web.TCPSite(runner, settings.API_HOST, settings.API_PORT)
        await site.start()
        logger.info("API listening on {}:{}", settings.API_HOST, settings.API_PORT)
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
