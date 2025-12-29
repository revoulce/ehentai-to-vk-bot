import asyncio
import hashlib
import random
from pathlib import Path
from typing import Optional, Dict, Any

from aiohttp import ClientTimeout, ClientSession, ClientError
from loguru import logger
from parsel import Selector
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

from config import settings


class EhentaiHarvester:
    def __init__(self) -> None:
        self.headers: Dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://e-hentai.org/",
        }
        self.sem = asyncio.Semaphore(2)

        # Proxy validation
        self.proxy = settings.EH_PROXY
        if self.proxy and "1.2.3.4" in self.proxy:
            self.proxy = None

    def _get_session_kwargs(self) -> Dict[str, Any]:
        return {
            "headers": self.headers,
            "cookies": settings.EH_COOKIES,
            "timeout": ClientTimeout(total=60, connect=15, sock_read=30),
            "trust_env": True
        }

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=20),
        retry=retry_if_exception_type((ClientError, asyncio.TimeoutError, ConnectionResetError)),
        before_sleep=before_sleep_log(logger, "WARNING")
    )
    async def fetch_text(self, session: ClientSession, url: str) -> str:
        async with session.get(url, proxy=self.proxy) as resp:
            resp.raise_for_status()
            return await resp.text()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, asyncio.TimeoutError)),
        reraise=True
    )
    async def download_image(self, session: ClientSession, url: str, dest: Path) -> Path:
        if dest.exists() and dest.stat().st_size > 0:
            return dest

        async with session.get(url, proxy=self.proxy) as resp:
            resp.raise_for_status()
            content = await resp.read()

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._write_file, dest, content)
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

                    # 1. Extract Title
                    title = sel.css("h1#gn::text").get() or sel.css("h1#gj::text").get()
                    if not title:
                        logger.error(f"Failed to extract title from {url}")
                        return None

                    # 2. Extract Tags
                    tags = sel.css("div#taglist tr td:nth-child(2) div a::text").getall()
                    if any(t in settings.TAG_BLACKLIST for t in tags):
                        logger.warning(f"Blacklisted tags in {title}")
                        return None

                    # 3. Extract Image Links (Robust Method)
                    # We look for ANY link inside #gdt that contains '/s/' (which denotes an image page)
                    raw_urls = sel.css("div#gdt a[href*='/s/']::attr(href)").getall()

                    # Deduplicate preserving order
                    seen = set()
                    image_page_urls = []
                    for u in raw_urls:
                        if u not in seen:
                            image_page_urls.append(u)
                            seen.add(u)

                    # Take first 10
                    image_page_urls = image_page_urls[:10]

                    if not image_page_urls:
                        logger.error(f"Found 0 images. HTML dump snippet: {html[:500]}")
                        return None

                    logger.info(f"Found {len(image_page_urls)} images for: {title}")

                    # 4. Download Images
                    image_paths = []
                    for idx, page_url in enumerate(image_page_urls):
                        await asyncio.sleep(random.uniform(0.5, 1.5))

                        try:
                            page_html = await self.fetch_text(session, page_url)
                            page_sel = Selector(text=page_html)
                            img_src = page_sel.css("img#img::attr(src)").get()

                            if img_src:
                                ext = img_src.split('.')[-1].split('?')[0]
                                if len(ext) > 4: ext = "jpg"

                                fname = hashlib.md5(f"{url}_{idx}".encode()).hexdigest() + f".{ext}"
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
                        "local_images": image_paths
                    }

                except Exception as e:
                    logger.exception(f"Scraping failed for {url}")
                    return None
