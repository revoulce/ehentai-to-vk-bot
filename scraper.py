import asyncio
import hashlib
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientError, ClientSession, ClientTimeout
from loguru import logger
from parsel import Selector
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings


class EhentaiHarvester:
    def __init__(self) -> None:
        self.headers: Dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://e-hentai.org/",
        }
        # Limit concurrency strictly to avoid IP bans since we are not using proxies
        self.sem = asyncio.Semaphore(1)

    def _get_session_kwargs(self) -> Dict[str, Any]:
        return {
            "headers": self.headers,
            "cookies": settings.EH_COOKIES,
            "timeout": ClientTimeout(total=45, connect=10, sock_read=30),
            "trust_env": True,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, asyncio.TimeoutError)),
        before_sleep=before_sleep_log(logger, "WARNING"),
    )
    async def fetch_text(self, session: ClientSession, url: str) -> str:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def download_image(
        self, session: ClientSession, url: str, dest: Path
    ) -> Path:
        if dest.exists() and dest.stat().st_size > 0:
            return dest

        async with session.get(url) as resp:
            resp.raise_for_status()
            content = await resp.read()

            # CPU-bound file writing offloaded to thread
            await asyncio.to_thread(self._write_file, dest, content)
            return dest

    @staticmethod
    def _write_file(path: Path, content: bytes) -> None:
        with open(path, "wb") as f:
            f.write(content)

    async def parse_gallery(self, url: str) -> Optional[Dict[str, Any]]:
        async with self.sem:
            async with ClientSession(**self._get_session_kwargs()) as session:
                try:
                    logger.info(f"Processing: {url}")
                    html = await self.fetch_text(session, url)
                    sel = Selector(text=html)

                    # 1. Title
                    title = sel.css("h1#gn::text").get() or sel.css("h1#gj::text").get()
                    if not title:
                        logger.error(f"Failed to extract title: {url}")
                        return None

                    # 2. Tags
                    tags = sel.css(
                        "div#taglist tr td:nth-child(2) div a::text"
                    ).getall()
                    if any(t in settings.TAG_BLACKLIST for t in tags):
                        logger.warning(f"Blacklisted tags in {title}")
                        return None

                    # 3. Image Links (Universal Selector)
                    raw_urls = sel.css("div#gdt a[href*='/s/']::attr(href)").getall()

                    # Deduplicate
                    seen: set[str] = set()
                    image_page_urls: List[str] = []
                    for u in raw_urls:
                        if u not in seen:
                            image_page_urls.append(u)
                            seen.add(u)

                    image_page_urls = image_page_urls[:10]

                    if not image_page_urls:
                        logger.error(f"No images found for: {title}")
                        return None

                    # 4. Download
                    image_paths: List[str] = []
                    for idx, page_url in enumerate(image_page_urls):
                        # Increased delay slightly since we are direct connection
                        await asyncio.sleep(random.uniform(1.0, 3.0))

                        try:
                            page_html = await self.fetch_text(session, page_url)
                            page_sel = Selector(text=page_html)
                            img_src = page_sel.css("img#img::attr(src)").get()

                            if img_src:
                                ext = img_src.split(".")[-1].split("?")[0]
                                if len(ext) > 4:
                                    ext = "jpg"

                                fname = (
                                    hashlib.md5(f"{url}_{idx}".encode()).hexdigest()
                                    + f".{ext}"
                                )
                                local_path = settings.STORAGE_PATH / fname

                                await self.download_image(session, img_src, local_path)
                                image_paths.append(str(local_path))
                        except Exception as e:
                            logger.warning(f"Image {idx} failed: {e}")
                            continue

                    if not image_paths:
                        return None

                    return {
                        "title": title,
                        "source_url": url,
                        "tags": tags,
                        "local_images": image_paths,
                    }

                except Exception as e:
                    logger.exception(f"Scraping failed for {url}")
                    return None
