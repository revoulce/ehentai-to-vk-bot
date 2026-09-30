import asyncio
import logging
import sys

from loguru import logger

from api import start_api_server
from models import engine, init_db
from tg_bot import start_bot
from workers import downloader_loop, uploader_loop


class AiohttpScannerFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "BadHttpMessage" not in message and "Pause on PRI" not in message


def configure_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO", diagnose=False)
    logger.add(
        "bot.log", rotation="10 MB", level="DEBUG", compression="zip", diagnose=False
    )
    logging.getLogger("aiohttp.server").addFilter(AiohttpScannerFilter())


async def main() -> None:
    try:
        await init_db()
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(start_bot())
            tasks.create_task(downloader_loop())
            tasks.create_task(uploader_loop())
            tasks.create_task(start_api_server())
    except Exception:
        logger.exception("System critical failure")
        raise
    finally:
        await engine.dispose()


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown")
