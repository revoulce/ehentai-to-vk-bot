# E-Hentai to VK Content Pipeline

Asynchronous service for scraping gallery metadata/images from E-Hentai and scheduling posts to a VKontakte community wall.

## Architecture

- **Core:** Python 3.14 (AsyncIO)
- **Network:** Aiohttp + Tenacity (Retries/Backoff)
- **Parsing:** Parsel (XPath/CSS Selectors)
- **Database:** SQLite + SQLAlchemy 2.0 (Async)
- **Interface:** Aiogram 3.x (Telegram Bot)
- **Deployment:** Docker Compose

## Features

1.  **Scraping:** Extracts title, tags (namespaces: parody, character, cosplayer), and downloads images.
2.  **Processing:**
    -   Random selection of 4 images per gallery for the grid layout.
    -   Tag cleaning (strips special chars, handles composite tags).
    -   Deduplication based on source URL.
3.  **Scheduling:**
    -   Calculates the next available round hour slot (e.g., 14:00, 15:00).
    -   Uses VK Native Scheduling (`publish_date`).
    -   Auto-rescheduling if the target slot passes during processing.
4.  **Lifecycle:** Auto-deletes local files after successful API handoff.

## Prerequisites

- **Docker Engine** & **Docker Compose**.
- **VK Account** with Admin rights to the target group.
- **E-Hentai Cookies** (Required for access to specific galleries/original images).

## Configuration

Create a `.env` file in the project root:

```ini
# --- VK Configuration ---
# CRITICAL: Use a USER Token (Kate Mobile/VK Admin app), NOT a Community Token.
# Community tokens cannot post to the wall with 'from_group=1' and attachments.
# Scope required: wall, photos, groups, offline.
VK_ACCESS_TOKEN=vk1.a.YOUR_USER_TOKEN_HERE

# Target Community ID (Positive integer)
VK_GROUP_ID=123456789

# --- Telegram Configuration ---
TG_BOT_TOKEN=123456:ABC-DEF...
# List of Admin IDs allowed to control the bot
ADMIN_IDS=[12345678, 87654321]

# --- E-Hentai Configuration ---
# JSON format required. Copy from browser DevTools -> Application -> Cookies.
EH_COOKIES='{"ipb_member_id": "12345", "ipb_pass_hash": "abcdef..."}'

# --- System ---
# Minimum interval between posts (minutes)
SCHEDULE_INTERVAL_MINUTES=60

# Galleries containing these tags will be rejected immediately
TAG_BLACKLIST='["guro", "scat", "furry", "bestiality"]'
```

## Deployment

Build and run the container:

```Bash
docker compose up -d --build
```

Check logs:

```Bash
docker compose logs -f
```

## Usage

Interact via the Telegram Bot (only responds to ADMIN_IDS).

- `/add <url>`
<br> Queues a gallery. The bot will:
    - Check for duplicates.
    - Scrape metadata and download images.
    - Calculate the next schedule slot.
    - Upload to VK and schedule the post.
    - Delete local files.
- `/status`
<br> Shows the count of galleries pending upload to VK.

## Troubleshooting

- **VK API Error 100 (invalid publish_date):** The worker loop automatically handles this by rescheduling the post to the next available future slot.
- **VK API Error 27 (Group auth failed):** You are using a Community Token. Switch to a User Token in `.env`.
- **Database Schema Changes:** If you modify `models.py`, delete `data/bot.db` before restarting, as no migration system is included in this MVP.