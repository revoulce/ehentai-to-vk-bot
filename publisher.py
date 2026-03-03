from typing import Optional

import aiohttp
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed

from config import settings


class VkPublisher:
    def __init__(self) -> None:
        self.base_url = "https://api.vk.com/method/"
        self.session_params = {
            "access_token": settings.VK_ACCESS_TOKEN.get_secret_value(),
            "v": settings.VK_API_VERSION,
        }
        self.group_id = int(settings.VK_GROUP_ID)

    async def _request(self, method: str, params: dict) -> dict:
        final_params = {**self.session_params, **params}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}{method}", data=final_params
            ) as resp:
                try:
                    data = await resp.json()
                except Exception:
                    text = await resp.text()
                    raise Exception(f"VK invalid JSON: {text}")

                if "error" in data:
                    err = data["error"]
                    logger.error(
                        f"VK API Error {err.get('error_code')}: {err.get('error_msg')}"
                    )
                    raise Exception(f"VK Error: {err.get('error_msg')}")

                return data.get("response", {})

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def upload_photos(self, file_paths: list[str]) -> list[str]:
        if not file_paths:
            return []

        attachments = []

        # Загрузка батчами до 5 фото за раз (лимит VK API)
        for i in range(0, len(file_paths), 5):
            chunk = file_paths[i : i + 5]

            server_data = await self._request(
                "photos.getWallUploadServer", {"group_id": self.group_id}
            )
            upload_url = server_data["upload_url"]

            data = aiohttp.FormData()
            opened_files = []
            try:
                for j, path in enumerate(chunk, start=1):
                    f = open(path, "rb")
                    opened_files.append(f)
                    data.add_field(f"photo{j}", f, filename=f"img{j}.jpg")

                async with aiohttp.ClientSession() as session:
                    async with session.post(upload_url, data=data) as upload_resp:
                        upload_result = await upload_resp.json()
            finally:
                for f in opened_files:
                    f.close()

            if (
                not upload_result
                or not upload_result.get("photo")
                or upload_result.get("photo") == "[]"
            ):
                continue

            save_params = {
                "group_id": self.group_id,
                "photo": upload_result["photo"],
                "server": upload_result["server"],
                "hash": upload_result["hash"],
            }

            saved_photos = await self._request("photos.saveWallPhoto", save_params)
            attachments.extend(
                [f"photo{p['owner_id']}_{p['id']}" for p in saved_photos]
            )

        return attachments

    async def publish(
        self,
        message: str,
        attachments: list[str],
        publish_date: Optional[int] = None,
        is_donut: bool = False,
    ) -> int:
        params = {
            "owner_id": -self.group_id,
            "from_group": 1,
            "message": message,
            "attachments": ",".join(attachments),
            "primary_attachments_mode": "grid",
        }

        if publish_date:
            params["publish_date"] = publish_date

        if is_donut:
            params["donut_paid_duration"] = -1

        response = await self._request("wall.post", params)
        post_id = response.get("post_id")
        logger.info(
            f"Published (Scheduled: {bool(publish_date)}, Donut: {is_donut}). Post ID: {post_id}"
        )
        return post_id
