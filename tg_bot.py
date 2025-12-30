import html

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from config import settings
from services import ServiceError, add_gallery_task, get_queue_status

router = Router()
router.message.filter(F.from_user.id.in_(settings.ADMIN_IDS))


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "<b>E-Hentai Manager</b>\n\n"
        "/add &lt;url&gt; - Queue gallery\n"
        "/status - Check queue"
    )


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    raw_status = await get_queue_status()
    await message.answer(html.escape(raw_status))


@router.message(Command("add"))
async def cmd_add(message: Message) -> None:
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Usage: /add &lt;url&gt;")
        return

    url = args[1]
    if "e-hentai.org/g/" not in url and "exhentai.org/g/" not in url:
        await message.answer("Invalid URL format.")
        return

    status_msg = await message.answer("Processing...")

    try:
        result = await add_gallery_task(url)
        await status_msg.edit_text(html.escape(result))
    except ServiceError as e:
        await status_msg.edit_text(f"Error: {html.escape(str(e))}")
    except Exception as e:
        await status_msg.edit_text("Critical system error.")
        raise e


async def start_bot() -> None:
    bot = Bot(
        token=settings.TG_BOT_TOKEN.get_secret_value(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)
