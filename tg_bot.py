import html
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message
from aiogram.filters import Command, CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import settings
from services import queue_gallery, get_queue_status, ServiceError

router = Router()
router.message.filter(F.from_user.id.in_(settings.ADMIN_IDS))


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "<b>E-Hentai Manager</b>\n\n"
        "/add &lt;url&gt; [url2] ... - Queue galleries\n"
        "/status - Check queue"
    )


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    raw_status = await get_queue_status()
    await message.answer(html.escape(raw_status))


@router.message(Command("add"))
async def cmd_add(message: Message) -> None:
    if not message.text:
        return

    entities = message.text.split()
    urls = [w for w in entities if ("e-hentai.org/g/" in w or "exhentai.org/g/" in w)]

    if not urls:
        await message.answer("Usage: /add &lt;url&gt;")
        return

    await message.answer(f"Queuing {len(urls)} galleries...")

    results = []
    for url in urls:
        # Extract ID/Token for brevity in logs
        short_name = url.split("/")[-3] if len(url.split("/")) > 3 else "Gallery"
        try:
            res = await queue_gallery(url)
            results.append(f"[OK] {short_name}: {res}")
        except ServiceError as e:
            results.append(f"[WARN] {short_name}: {str(e)}")
        except Exception:
            results.append(f"[ERR] {short_name}: System Error")

    # Split long messages if necessary
    response_text = "\n".join(results)
    if len(response_text) > 4000:
        response_text = response_text[:4000] + "..."

    await message.answer(response_text)


async def start_bot() -> None:
    bot = Bot(
        token=settings.TG_BOT_TOKEN.get_secret_value(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)
