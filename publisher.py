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

        # 1. Get Upload Server (Requires Group ID even with User Token for group wall)
        server_data = await self._request(
            "photos.getWallUploadServer", {"group_id": self.group_id}
        )
        upload_url = server_data["upload_url"]

        async with aiohttp.ClientSession() as session:
            for path in file_paths:
                try:
                    with open(path, "rb") as f:
                        data = aiohttp.FormData()
                        data.add_field("photo", f, filename="img.jpg")

                        async with session.post(upload_url, data=data) as upload_resp:
                            upload_result = await upload_resp.json()

                    if (
                        not upload_result.get("photo")
                        or upload_result.get("photo") == "[]"
                    ):
                        logger.warning(f"Upload failed: {path}")
                        continue

                    # 2. Save Photo (Requires Group ID)
                    save_params = {
                        "group_id": self.group_id,
                        "photo": upload_result["photo"],
                        "server": upload_result["server"],
                        "hash": upload_result["hash"],
                    }

                    saved_photos = await self._request(
                        "photos.saveWallPhoto", save_params
                    )

                    if saved_photos:
                        p = saved_photos[0]
                        attachments.append(f"photo{p['owner_id']}_{p['id']}")

                except Exception as e:
                    logger.error(f"Image processing error {path}: {e}")
                    continue

        return attachments

    async def publish(self, message: str, attachments: list[str]) -> int:
        params = {
            "owner_id": -self.group_id,  # Negative for Group
            "from_group": 1,  # Post as Group
            "message": message,
            "attachments": ",".join(attachments),
        }

        response = await self._request("wall.post", params)
        post_id = response.get("post_id")
        logger.info(f"Published post ID: {post_id}")
        return post_id
