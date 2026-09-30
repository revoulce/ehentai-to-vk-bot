# E-Hentai to VK Content Pipeline

Asynchronous service that downloads gallery images and schedules posts to a VK community. Galleries can be queued from Telegram or the companion browser extension.

## Architecture

- Python 3.12+ (Docker uses Python 3.13), aiohttp, SQLAlchemy/SQLite, aiogram.
- `main.py`: logging, startup and shutdown of the four concurrent tasks.
- `api.py`: authenticated HTTP API and request validation.
- `services.py`: queue operations, captions and gallery processing.
- `scheduling.py`: UTC schedule calculation with a five-minute buffer.
- `workers.py`: downloader and VK uploader loops.
- `scraper.py`: metadata parsing and image downloads.
- `publisher.py`: VK API calls with a shared HTTP session and per-photo network retries.
- `storage.py`: local file cleanup.

One downloader and one uploader run per process. Run a single bot instance against a database; the queue does not implement distributed worker claims.

## Configuration

Copy `.env.example` to `.env` and replace the sample values:

```ini
VK_ACCESS_TOKEN=your-vk-user-token
VK_GROUP_ID=123456789
TG_BOT_TOKEN=123456:your-telegram-token
ADMIN_IDS=[12345678]
EH_COOKIES={"ipb_member_id":"12345","ipb_pass_hash":"your-cookie"}
API_SECRET=your-long-random-secret
API_HOST=0.0.0.0
API_PORT=45000
SCHEDULE_INTERVAL_HOURS=1
```

Use a VK user token with permission to manage the target community and upload photos. `VK_GROUP_ID` must be positive, `API_SECRET` is required, and `SCHEDULE_INTERVAL_HOURS` must be at least 1.

The default blacklist is `guro`, `scat`, `furry`, `lolicon`, `shotacon`, `bestiality`. Override it with a JSON array in `TAG_BLACKLIST` if needed.

The SQLite database defaults to `./data/bot.db` and downloads to `./downloads`; the bot creates these directories at startup. Override `DB_URL` and `STORAGE_PATH` for other paths.

## Deployment

```sh
docker compose up -d --build
docker compose logs -f
```

Compose exposes port **45000** and explicitly sets the container API host/port. Set the extension API endpoint to `http://localhost:45000/api/queue` for a local deployment, or to your bot's reachable endpoint. Enter the same `API_SECRET` in the extension settings. Existing saved extension settings are preserved; update an old endpoint manually.

For local development:

```sh
python -m venv .venv
# Windows: .venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
python -m pip install -r requirements.txt
python main.py
```

## Usage

Telegram commands are restricted to `ADMIN_IDS`:

- `/add <url> [url2] ...`: enqueue galleries.
- `/status`: show pending, ready and failed counts.

`POST /api/queue` accepts JSON and requires `Authorization: Bearer <API_SECRET>`:

```json
{"url":"https://e-hentai.org/g/123/abcdef/","include_cosplayer":false}
```

Only E-Hentai and ExHentai gallery URLs are accepted. URLs are normalized to HTTPS without query parameters or fragments before insertion. Concurrent duplicate submissions return HTTP 409. Malformed input returns 400, invalid credentials 401, and internal errors 500.

The worker selects up to 13 images: up to 4 for the public post and up to 9 for the Donut continuation. It schedules posts at whole UTC hours, with the configured minimum spacing, and checks the five-minute buffer again after uploading photos. The public post ID and image order are saved before the optional Donut operation. Donut failures are logged and do not retry the public post. Local images are removed once processing is complete.

## Verification

```sh
python -m unittest discover -s tests -v
python -m pip install black
python -m black --check *.py tests
```

Tests use temporary SQLite databases and mocked HTTP/VK responses; they never publish posts. Browser extension checks are documented in its own README.

## Operational limits

- The VK request and SQLite commit cannot be atomic. A process crash or lost VK response between creating a post and saving its ID can still cause a duplicate on retry. Donut delivery is best effort.
- Records queued before URL normalization retain their existing source URLs. A legacy URL with query parameters may not match a newly normalized submission.
- Database schema changes require migrations; startup only creates missing tables. This refactor does not change the table schema and does not require deleting the existing database.
