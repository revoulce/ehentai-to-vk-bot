import asyncio
from pathlib import Path

from loguru import logger


async def cleanup_files(file_paths: list[str]) -> None:
    for file_path in file_paths:
        try:
            await asyncio.to_thread(Path(file_path).unlink, missing_ok=True)
        except OSError as exc:
            logger.warning("Cleanup failed for {}: {}", file_path, exc)
