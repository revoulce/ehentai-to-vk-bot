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
        reraise=True,
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

                    # 2. Tags with Namespaces (NEW)
                    # Structure: dict {"parody": ["tag1", "tag2"], "character": [...]}
                    tags_data: Dict[str, List[str]] = {}

                    # Iterate over rows in taglist
                    tag_rows = sel.css("div#taglist table tr")
                    for row in tag_rows:
                        # Namespace is in the first td, e.g., "parody:"
                        ns = row.css("td.tc::text").get()
                        if ns:
                            ns = ns.strip().rstrip(":")
                            # Tags are in the second td
                            t_list = row.css("td:nth-child(2) div a::text").getall()
                            tags_data[ns] = t_list

                    # Check blacklist (flatten tags for check)
                    all_tags = [t for sublist in tags_data.values() for t in sublist]
                    if any(t in settings.TAG_BLACKLIST for t in all_tags):
                        logger.warning(f"Blacklisted tags in {title}")
                        return None

                    # 3. Image Links
                    # Download more images (up to 15) to allow random selection of 4
                    raw_urls = sel.css("div#gdt a[href*='/s/']::attr(href)").getall()

                    seen = set()
                    image_page_urls = []
                    for u in raw_urls:
                        if u not in seen:
                            image_page_urls.append(u)
                            seen.add(u)

                    # Get up to 15 images to have a pool for random selection
                    image_page_urls = image_page_urls[:15]

                    if not image_page_urls:
                        logger.error(f"No images found for: {title}")
                        return None

                    # 4. Download
                    image_paths: List[str] = []
                    for idx, page_url in enumerate(image_page_urls):
                        await asyncio.sleep(random.uniform(1.0, 2.5))
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
                        "tags": tags_data,  # Now storing dict
                        "local_images": image_paths,
                    }

                except Exception as e:
                    logger.exception(f"Scraping failed for {url}")
                    return None
