import asyncio
import hashlib
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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
        self.sem = asyncio.Semaphore(2)

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
                    logger.info(f"Processing metadata: {url}")

                    # 1. Fetch Page 0 (Base URL)
                    html_p0 = await self.fetch_text(session, url)
                    sel_p0 = Selector(text=html_p0)

                    # --- Metadata Extraction ---
                    title = (
                        sel_p0.css("h1#gn::text").get()
                        or sel_p0.css("h1#gj::text").get()
                    )
                    if not title:
                        logger.error(f"Failed to extract title: {url}")
                        return None

                    # Extract tags with namespaces
                    tags_data: Dict[str, List[str]] = {}
                    tag_rows = sel_p0.css("div#taglist table tr")
                    for row in tag_rows:
                        ns = row.css("td.tc::text").get()
                        if ns:
                            ns = ns.strip().rstrip(":")
                            t_list = row.css("td:nth-child(2) div a::text").getall()
                            tags_data[ns] = t_list

                    # Blacklist check
                    all_tags = [t for sublist in tags_data.values() for t in sublist]
                    if any(t in settings.TAG_BLACKLIST for t in all_tags):
                        logger.warning(f"Blacklisted tags in {title}")
                        return None

                    # --- Global Random Selection Logic ---

                    # Determine Total Images count from "Showing 1 - 20 of X images"
                    total_images = 0
                    gpc_text = sel_p0.css("p.gpc::text").get()
                    if gpc_text:
                        match = re.search(r"of ([\d,]+) images", gpc_text)
                        if match:
                            total_images = int(match.group(1).replace(",", ""))

                    if total_images == 0:
                        # Fallback: Count images on current page
                        total_images = len(sel_p0.css("div#gdt a").getall())
                        logger.warning(
                            f"Could not parse total images count. Fallback to {total_images}"
                        )

                    logger.info(f"Total images in gallery: {total_images}")

                    # Pick 4 unique random indices across the entire gallery
                    target_count = min(total_images, 4)
                    target_indices = set(
                        random.sample(range(total_images), target_count)
                    )

                    # Map global indices to specific pages (Standard EH page size is 20)
                    PAGE_SIZE = 20
                    pages_to_fetch: Dict[int, Set[int]] = {}

                    for global_idx in target_indices:
                        page_num = global_idx // PAGE_SIZE
                        local_idx = global_idx % PAGE_SIZE
                        if page_num not in pages_to_fetch:
                            pages_to_fetch[page_num] = set()
                        pages_to_fetch[page_num].add(local_idx)

                    # Fetch required pages and extract specific image links
                    image_page_urls: List[str] = []

                    for page_num, local_indices in pages_to_fetch.items():
                        # Optimization: Reuse Page 0 HTML if needed
                        if page_num == 0:
                            sel = sel_p0
                        else:
                            # E-Hentai pagination: /?p=1 is page 2
                            page_url = f"{url}?p={page_num}"
                            logger.info(f"Fetching page {page_num}: {page_url}")
                            await asyncio.sleep(random.uniform(0.5, 1.0))
                            page_html = await self.fetch_text(session, page_url)
                            sel = Selector(text=page_html)

                        # Extract all image links on this page
                        # Universal selector for both Extended and Thumbnail views
                        raw_links = sel.css(
                            "div#gdt a[href*='/s/']::attr(href)"
                        ).getall()

                        # Deduplicate preserving order
                        unique_links = []
                        seen = set()
                        for link in raw_links:
                            if link not in seen:
                                unique_links.append(link)
                                seen.add(link)

                        # Select specific images based on local index
                        for local_idx in local_indices:
                            if local_idx < len(unique_links):
                                image_page_urls.append(unique_links[local_idx])
                            else:
                                logger.error(
                                    f"Index {local_idx} out of bounds for page {page_num}"
                                )

                    if not image_page_urls:
                        logger.error("No image links found after traversing pages.")
                        return None

                    # Download the selected images
                    image_paths: List[str] = []
                    for idx, page_url in enumerate(image_page_urls):
                        await asyncio.sleep(random.uniform(1.0, 2.0))
                        try:
                            page_html = await self.fetch_text(session, page_url)
                            page_sel = Selector(text=page_html)
                            img_src = page_sel.css("img#img::attr(src)").get()

                            if img_src:
                                ext = img_src.split(".")[-1].split("?")[0]
                                if len(ext) > 4:
                                    ext = "jpg"

                                # Hash filename to avoid collisions and ensure uniqueness
                                fname = (
                                    hashlib.md5(
                                        f"{url}_{idx}_{random.randint(0, 1000)}".encode()
                                    ).hexdigest()
                                    + f".{ext}"
                                )
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

                except Exception as e:
                    logger.exception(f"Scraping failed for {url}")
                    return None
