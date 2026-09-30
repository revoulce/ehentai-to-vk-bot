import asyncio
import mimetypes
from pathlib import Path
from typing import Any

import aiohttp
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from config import settings


class VkAPIError(RuntimeError):
    def __init__(self, code: int | None, message: str) -> None:
        self.code = code
        super().__init__(f"VK Error {code}: {message}")


class VkPublisher:
    """Reuse one connection pool for API calls and photo uploads."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.base_url = "https://api.vk.com/method/"
        self.session_params = {
            "access_token": settings.VK_ACCESS_TOKEN.get_secret_value(),
            "v": settings.VK_API_VERSION,
        }
        self.group_id = settings.VK_GROUP_ID

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        async with self.session.post(
            f"{self.base_url}{method}", data={**self.session_params, **params}
        ) as response:
            response.raise_for_status()
            try:
                data = await response.json()
            except ValueError as exc:
                raise RuntimeError("VK returned invalid JSON") from exc
        if "error" in data:
            error = data["error"]
            raise VkAPIError(
                error.get("error_code"), error.get("error_msg", "Unknown error")
            )
        if "response" not in data:
            raise RuntimeError("VK response is missing its payload")
        return data["response"]

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        reraise=True,
    )
    async def _upload_photo(self, file_path: str) -> str:
        server = await self._request(
            "photos.getWallUploadServer", {"group_id": self.group_id}
        )
        path = Path(file_path)
        with path.open("rb") as image:
            data = aiohttp.FormData()
            data.add_field(
                "photo",
                image,
                filename=path.name,
                content_type=mimetypes.guess_type(path.name)[0]
                or "application/octet-stream",
            )
            async with self.session.post(server["upload_url"], data=data) as response:
                response.raise_for_status()
                uploaded = await response.json()
        if not uploaded or uploaded.get("photo") in (None, "", "[]"):
            raise RuntimeError("VK rejected the uploaded photo")
        saved = await self._request(
            "photos.saveWallPhoto",
            {
                "group_id": self.group_id,
                "photo": uploaded["photo"],
                "server": uploaded["server"],
                "hash": uploaded["hash"],
            },
        )
        if not saved:
            raise RuntimeError("VK did not save the uploaded photo")
        photo = saved[0]
        return f"photo{photo['owner_id']}_{photo['id']}"

    async def upload_photos(self, file_paths: list[str]) -> list[str]:
        attachments = []
        for file_path in file_paths:
            try:
                attachments.append(await self._upload_photo(file_path))
            except Exception as exc:
                logger.error("Photo upload failed for {}: {}", file_path, exc)
        return attachments

    async def publish(
        self,
        message: str,
        attachments: list[str],
        publish_date: int | None = None,
        is_donut: bool = False,
    ) -> int:
        params = {
            "owner_id": -self.group_id,
            "from_group": 1,
            "message": message,
            "attachments": ",".join(attachments),
            "primary_attachments_mode": "grid",
        }
        if publish_date is not None:
            params["publish_date"] = publish_date
        if is_donut:
            params["donut_paid_duration"] = -1
        response = await self._request("wall.post", params)
        post_id = response.get("post_id")
        if not isinstance(post_id, int) or post_id <= 0:
            raise RuntimeError("VK did not return a valid post ID")
        logger.info("VK scheduled post {} (Donut: {})", post_id, is_donut)
        return post_id
