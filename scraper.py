import asyncio
import random
import re
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urljoin, urlsplit
from uuid import uuid4

from aiohttp import ClientError, ClientSession, ClientTimeout
from loguru import logger
from parsel import Selector
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings
from utils import normalize_gallery_url


class GalleryData(TypedDict):
    title: str
    source_url: str
    tags: dict[str, list[str]]
    local_images: list[str]


class EhentaiHarvester:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Referer": "https://e-hentai.org/",
        }
        self.sem = asyncio.Semaphore(2)

    def _get_session_kwargs(self) -> dict[str, Any]:
        return {
            "headers": self.headers,
            "cookies": settings.EH_COOKIES,
            # Увеличенный таймаут для загрузки до 13 изображений
            "timeout": ClientTimeout(total=120, connect=10, sock_read=30),
            "trust_env": True,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ClientError, asyncio.TimeoutError)),
        before_sleep=lambda state: logger.warning(
            "Retrying gallery request after attempt {}", state.attempt_number
        ),
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
            if not content or not resp.headers.get("Content-Type", "").startswith(
                "image/"
            ):
                raise ValueError("Image server returned empty or non-image content")
            await asyncio.to_thread(self._write_file, dest, content)
            return dest

    @staticmethod
    def _write_file(path: Path, content: bytes) -> None:
        temporary = path.with_suffix(path.suffix + ".part")
        try:
            temporary.write_bytes(content)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    async def parse_gallery(self, url: str) -> GalleryData | None:
        url = normalize_gallery_url(url)
        async with self.sem:
            async with ClientSession(**self._get_session_kwargs()) as session:
                try:
                    logger.info(f"Processing metadata: {url}")

                    html_p0 = await self.fetch_text(session, url)
                    sel_p0 = Selector(text=html_p0)

                    title = (
                        sel_p0.css("h1#gn::text").get()
                        or sel_p0.css("h1#gj::text").get()
                    )
                    if not title:
                        logger.error(f"Failed to extract title: {url}")
                        return None

                    tags_data: dict[str, list[str]] = {}
                    tag_rows = sel_p0.css("div#taglist table tr")
                    for row in tag_rows:
                        ns = row.css("td.tc::text").get()
                        if ns:
                            ns = ns.strip().rstrip(":")
                            t_list = row.css("td:nth-child(2) div a::text").getall()
                            tags_data[ns] = t_list

                    all_tags = [t for sublist in tags_data.values() for t in sublist]
                    blacklist = {
                        tag.strip().casefold() for tag in settings.TAG_BLACKLIST
                    }
                    if any(t.strip().casefold() in blacklist for t in all_tags):
                        logger.warning(f"Blacklisted tags in {title}")
                        return None

                    total_images = 0
                    gpc_text = sel_p0.css("p.gpc::text").get()
                    if gpc_text:
                        match = re.search(r"of ([\d,]+) images", gpc_text)
                        if match:
                            total_images = int(match.group(1).replace(",", ""))

                    page_0_links = list(
                        dict.fromkeys(
                            sel_p0.css("div#gdt a[href*='/s/']::attr(href)").getall()
                        )
                    )
                    page_size = len(page_0_links)

                    if page_size == 0:
                        logger.error("No images found on page 0")
                        return None

                    if total_images == 0:
                        total_images = page_size

                    logger.info(f"Total images: {total_images}, Page size: {page_size}")

                    # 13: 4 на основной паблик + 9 на донат (если в галерее есть столько)
                    target_count = min(total_images, 13)
                    target_indices = set(
                        random.sample(range(total_images), target_count)
                    )

                    pages_to_fetch: dict[int, set[int]] = {}
                    for global_idx in target_indices:
                        page_num = global_idx // page_size
                        local_idx = global_idx % page_size
                        if page_num not in pages_to_fetch:
                            pages_to_fetch[page_num] = set()
                        pages_to_fetch[page_num].add(local_idx)

                    image_page_urls: list[str] = []
                    for page_num, local_indices in pages_to_fetch.items():
                        if page_num == 0:
                            sel = sel_p0
                        else:
                            page_url = f"{url}?p={page_num}"
                            logger.info(f"Fetching page {page_num}: {page_url}")
                            await asyncio.sleep(random.uniform(0.5, 1.0))
                            page_html = await self.fetch_text(session, page_url)
                            sel = Selector(text=page_html)

                        unique_links = list(
                            dict.fromkeys(
                                sel.css("div#gdt a[href*='/s/']::attr(href)").getall()
                            )
                        )

                        for local_idx in local_indices:
                            if local_idx < len(unique_links):
                                image_page_urls.append(
                                    urljoin(url, unique_links[local_idx])
                                )
                            else:
                                logger.error(
                                    f"Index {local_idx} out of bounds for page {page_num}"
                                )

                    if not image_page_urls:
                        logger.error("No image links found after traversing pages.")
                        return None

                    image_paths: list[str] = []
                    for page_url in image_page_urls:
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                        try:
                            page_html = await self.fetch_text(session, page_url)
                            page_sel = Selector(text=page_html)
                            img_src = page_sel.css("img#img::attr(src)").get()

                            if img_src:
                                img_src = urljoin(page_url, img_src)
                                ext = Path(urlsplit(img_src).path).suffix.lower()
                                if ext not in {
                                    ".jpg",
                                    ".jpeg",
                                    ".png",
                                    ".gif",
                                    ".webp",
                                }:
                                    ext = ".jpg"
                                fname = f"{uuid4().hex}{ext}"
                                local_path = settings.STORAGE_PATH / fname

                                await self.download_image(session, img_src, local_path)
                                image_paths.append(str(local_path))
                        except Exception as e:
                            logger.warning(f"Image download failed {page_url}: {e}")
                            continue

                    if not image_paths:
                        return None

                    return {
                        "title": title,
                        "source_url": url,
                        "tags": tags_data,
                        "local_images": image_paths,
                    }

                except Exception:
                    logger.exception(f"Scraping failed for {url}")
                    return None
