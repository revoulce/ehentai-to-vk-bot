from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # VK Configuration
    VK_ACCESS_TOKEN: SecretStr
    VK_GROUP_ID: int
    VK_API_VERSION: str = "5.199"

    # Telegram Configuration
    TG_BOT_TOKEN: SecretStr
    ADMIN_IDS: list[int]

    # E-Hentai Configuration
    EH_COOKIES: dict[str, str]
    EH_PROXY: str | None = None

    # System Configuration
    DB_URL: str = "sqlite+aiosqlite:///./data/bot.db"
    STORAGE_PATH: Path = Path("./downloads")
    SCHEDULE_INTERVAL_MINUTES: int = 60

    # Filtering
    TAG_BLACKLIST: set[str] = {"guro", "scat", "furry", "lolicon", "shotacon"}

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True
    )


settings = Settings()
settings.STORAGE_PATH.mkdir(parents=True, exist_ok=True)
