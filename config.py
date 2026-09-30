from pathlib import Path
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    VK_ACCESS_TOKEN: SecretStr
    VK_GROUP_ID: int = Field(gt=0)
    VK_API_VERSION: str = "5.199"

    TG_BOT_TOKEN: SecretStr
    ADMIN_IDS: list[int]

    EH_COOKIES: dict[str, str]

    API_HOST: str = "0.0.0.0"
    API_PORT: int = Field(default=45000, ge=1, le=65535)
    API_SECRET: SecretStr = Field(min_length=1)

    DB_URL: str = "sqlite+aiosqlite:///./data/bot.db"
    STORAGE_PATH: Path = Path("./downloads")
    SCHEDULE_INTERVAL_HOURS: int = Field(default=1, ge=1)

    TAG_BLACKLIST: set[str] = {
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
