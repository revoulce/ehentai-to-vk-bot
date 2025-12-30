from pathlib import Path
from typing import Dict, List, Set

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # VK
    VK_ACCESS_TOKEN: SecretStr
    VK_GROUP_ID: int
    VK_API_VERSION: str = "5.199"

    # Telegram
    TG_BOT_TOKEN: SecretStr
    ADMIN_IDS: List[int]

    # E-Hentai
    EH_COOKIES: Dict[str, str]

    # API (New)
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    API_SECRET: SecretStr = SecretStr("change_me_in_env")

    # System
    DB_URL: str = "sqlite+aiosqlite:///./data/bot.db"
    STORAGE_PATH: Path = Path("./downloads")
    SCHEDULE_INTERVAL_MINUTES: int = 60

    TAG_BLACKLIST: Set[str] = {
        "guro",
        "scat",
        "furry",
        "lolicon",
        "shotacon",
        "bestiality",
    }

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=True
    )


settings = Settings()
settings.STORAGE_PATH.mkdir(parents=True, exist_ok=True)
