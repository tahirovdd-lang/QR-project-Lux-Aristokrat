import os
import json
import time
import asyncio
import logging
from typing import List, Set

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiogram import Bot, Dispatcher, F
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    WebAppInfo,
    MenuButtonWebApp,
    ReplyKeyboardMarkup,
    KeyboardButton,
)

# ==========================================
# LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)

print("=== LUX ARISTOKRAT MULTILANG ADMIN VERSION FIXED ===")

# ==========================================
# ENV
# ==========================================

BOT_TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("API_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_API_TOKEN")
    or ""
).strip()

WEBAPP_URL = os.getenv("WEBAPP_URL", "").strip()
if not WEBAPP_URL:
    WEBAPP_URL = "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/?v=1"

ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

logging.info("WEBAPP_URL (effective) = %s", WEBAPP_URL)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN / API_TOKEN / TELEGRAM_BOT_TOKEN is not set")


def parse_admin_ids() -> List[int]:
    result: Set[int] = set()

    try:
        admin_id = int(ADMIN_ID_RAW)
        if admin_id > 0:
            result.add(admin_id)
    except Exception:
        pass

    if ADMIN_IDS_RAW:
        for part in ADMIN_IDS_RAW.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                value = int(part)
                if value > 0:
                    result.add(value)
            except Exception:
                continue

    return sorted(result)


ADMIN_IDS = parse_admin_ids()

logging.info("ADMIN_ID from env: %s", ADMIN_ID_RAW)
logging.info("ADMIN_IDS from env: %s", ADMIN_IDS if ADMIN_IDS else "EMPTY")

# ==========================================
# NETWORK SESSION
# ==========================================

def build_bot_session() -> AiohttpSession:
    timeout = ClientTimeout(
        total=75,
        connect=20,
        sock_connect=20,
        sock_read=60,
    )

    connector = TCPConnector(
        ssl=False,
        limit=100,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    client_session = ClientSession(
        timeout=timeout,
        connector=connector,
        trust_env=True,
    )

    return AiohttpSession(session=client_session)


session = build_bot_session()
bot = Bot(
    token=BOT_TOKEN,
    session=session,
    parse_mode=ParseMode.HTML,
)
dp = Dispatcher()

# ==========================================
# ANTIDUPLICATE SCAN PROTECT
# ==========================================

RECENT_SCANS = {}
SCAN_TTL_SECONDS = 8


def cleanup_recent_scans():
    now = time.time()
    expired_keys = [k for k, ts in RECENT_SCANS.items() if now - ts > SCAN_TTL_SECONDS]
    for k in expired_keys:
        RECENT_SCANS.pop(k, None)


def is_duplicate_scan(user_id: int, code: str) -> bool:
    cleanup_recent_scans()

    key = f"{user_id}:{code}"
    now = time.time()

    if key in RECENT_SCANS:
        return True

    RECENT_SCANS[key] = now
    return False

# ==========================================
# I18N
# ==========================================

TEXTS = {
    "ru": {
        "welcome": (
            "Добро пожаловать в <b>Lux Aristokrat</b>\n\n"
            "Откройте mini app для сканирования QR / Data Matrix."
        ),
        "open_scanner": "Открыть сканер",
        "menu_button": "Сканер QR",
        "empty_code": "Пустой код.",
        "bad_data": "Ошибка чтения данных.",
        "scan_received": "QR принят:\n<code>{code}</code>",
        "scan_duplicate": "Этот код уже был только что обработан.",
        "processing_error": "Ошибка обработки QR-кода.",
        "unknown_action": "Неизвестное действие.",
        "admin_only": "Команда только для администратора.",
        "debug_url": "Текущий WEBAPP_URL:\n{url}",
        "help": (
            "Доступные команды:\n"
            "/start — старт\n"
            "/help — помощь\n"
            "/debug_url — показать WebApp URL (admin)\n"
            "/id — показать Telegram ID"
        ),
        "your_id": "Ваш Telegram ID: <code>{user_id}</code>\nAdmin access: <b>{admin}</b>",
    },
    "uz": {
        "welcome": (
            "<b>Lux Aristokrat</b> ga xush kelibsiz\n\n"
            "QR / Data Matrix skanerlash uchun mini app ni oching."
        ),
        "open_scanner": "Skanerni ochish",
        "menu_button": "QR skaner",
        "empty_code": "Bo‘sh kod.",
        "bad_data": "Ma’lumotni o‘qishda xatolik.",
        "scan_received": "QR qabul qilindi:\n<code>{code}</code>",
        "scan_duplicate": "Bu kod hozirgina qayta ishlangan.",
        "processing_error": "QR-kodni qayta ishlashda xatolik.",
        "unknown_action": "Noma’lum harakat.",
        "admin_only": "Buyruq faqat administrator uchun.",
        "debug_url": "Joriy WEBAPP_URL:\n{url}",
        "help": (
            "Mavjud buyruqlar:\n"
            "/start — start\n"
            "/help — yordam\n"
            "/debug_url — WebApp URL ni ko‘rsatish (admin)\n"
            "/id — Telegram ID ni ko‘rsatish"
        ),
        "your_id": "Sizning Telegram ID: <code>{user_id}</code>\nAdmin access: <b>{admin}</b>",
    },
    "en": {
      "welcome": (
            "Welcome to <b>Lux Aristokrat</b>\n\n"
            "Open the mini app to scan QR / Data Matrix."
        ),
        "open_scanner": "Open scanner",
        "menu_button": "QR Scanner",
        "empty_code": "Empty code.",
        "bad_data": "Failed to read data.",
        "scan_received": "QR received:\n<code>{code}</code>",
        "scan_duplicate": "This code has just been processed already.",
        "processing_error": "QR code processing error.",
        "unknown_action": "Unknown action.",
        "admin_only": "Admin only command.",
        "debug_url": "Current WEBAPP_URL:\n{url}",
        "help": (
            "Available commands:\n"
            "/start — start\n"
            "/help — help\n"
            "/debug_url — show WebApp URL (admin)\n"
            "/id — show Telegram ID"
        ),
        "your_id": "Your Telegram ID: <code>{user_id}</code>\nAdmin access: <b>{admin}</b>",
    },
    "tj": {
        "welcome": (
            "Хуш омадед ба <b>Lux Aristokrat</b>\n\n"
            "Барои скан кардани QR / Data Matrix mini app-ро кушоед."
        ),
        "open_scanner": "Кушодани сканер",
        "menu_button": "Сканери QR",
        "empty_code": "Код холӣ аст.",
        "bad_data": "Хатогӣ ҳангоми хондани маълумот.",
        "scan_received": "QR қабул шуд:\n<code>{code}</code>",
        "scan_duplicate": "Ин код ҳозир аллакай коркард шудааст.",
        "processing_error": "Хатогӣ ҳангоми коркарди QR-код.",
        "unknown_action": "Амали номаълум.",
        "admin_only": "Фармон танҳо барои админ.",
        "debug_url": "WEBAPP_URL ҷорӣ:\n{url}",
        "help": (
            "Фармонҳои дастрас:\n"
            "/start — оғоз\n"
            "/help — ёрӣ\n"
            "/debug_url — нишон додани WebApp URL (admin)\n"
            "/id — нишон додани Telegram ID"
        ),
        "your_id": "Telegram ID шумо: <code>{user_id}</code>\nAdmin access: <b>{admin}</b>",
    },
}


def normalize_lang(lang_code: str) -> str:
    lang_code = (lang_code or "").strip().lower()
    if lang_code.startswith("ru"):
        return "ru"
    if lang_code.startswith("uz"):
        return "uz"
    if lang_code.startswith("en"):
        return "en"
    if lang_code.startswith("tg") or lang_code.startswith("tj"):
        return "tj"
    return "ru"


def get_user_lang(message: Message) -> str:
    try:
        code = message.from_user.language_code if message.from_user else "ru"
        return normalize_lang(code or "ru")
    except Exception:
        return "ru"


def t(lang: str, key: str, **kwargs) -> str:
    pack = TEXTS.get(lang) or TEXTS["ru"]
    text = pack.get(key) or TEXTS["ru"].get(key) or key
    if kwargs:
        return text.format(**kwargs)
    return text

# ==========================================
# HELPERS
# ==========================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def build_main_keyboard(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(
                    text=t(lang, "open_scanner"),
                    web_app=WebAppInfo(url=WEBAPP_URL),
                )
            ]
        ],
        resize_keyboard=True
    )

# ==========================================
# YOUR QR LOGIC PLACE
# ==========================================

async def process_scanned_qr_code(message: Message, code: str, language: str) -> None:
    """
    СЮДА ВСТАВЬ СВОЮ ТЕКУЩУЮ ЛОГИКУ ОБРАБОТКИ QR,
    ЕСЛИ ОНА У ТЕБЯ УЖЕ ЕСТЬ.

    Сейчас стоит безопасная заглушка, которая ничего не ломает.
    """
    await message.answer(
        t(language, "scan_received", code=code)
    )

# ==========================================
# COMMANDS
# ==========================================

@dp.message(CommandStart())
async def start_handler(message: Message):
    lang = get_user_lang(message)
    await message.answer(
        t(lang, "welcome"),
        reply_markup=build_main_keyboard(lang),
    )


@dp.message(Command("help"))
async def help_handler(message: Message):
    lang = get_user_lang(message)
    await message.answer(t(lang, "help"))


@dp.message(Command("id"))
async def id_handler(message: Message):
    lang = get_user_lang(message)
    user_id = message.from_user.id if message.from_user else 0
    await message.answer(
        t(
            lang,
            "your_id",
            user_id=user_id,
            admin="YES" if is_admin(user_id) else "NO",
        )
    )


@dp.message(Command("debug_url"))
async def debug_url_handler(message: Message):
    lang = get_user_lang(message)
    user_id = message.from_user.id if message.from_user else 0

    if not is_admin(user_id):
        await message.answer(t(lang, "admin_only"))
        return

    await message.answer(t(lang, "debug_url", url=WEBAPP_URL))

# ==========================================
# WEB APP DATA
# ==========================================

@dp.message(F.web_app_data)
async def handle_web_app_data(message: Message):
    raw_data = message.web_app_data.data if message.web_app_data else ""
    logging.info("WEB_APP_DATA RAW: %s", raw_data)

    try:
        data = json.loads(raw_data)
    except Exception as e:
        logging.exception("WEB_APP_DATA JSON parse error: %s", e)
        lang = get_user_lang(message)
        await message.answer(t(lang, "bad_data"))
        return

    action = str(data.get("action", "")).strip()
    code = str(data.get("code", "")).strip()
    language = normalize_lang(str(data.get("language", "ru")).strip().lower() or "ru")

    if action != "scan_qr":
        logging.info("Ignored web app action: %s", action)
        lang = language or get_user_lang(message)
        await message.answer(t(lang, "unknown_action"))
        return

    if not code:
        await message.answer(t(language, "empty_code"))
        return

    user_id = message.from_user.id if message.from_user else 0

    if is_duplicate_scan(user_id, code):
        logging.info(
            "Duplicate scan ignored: user_id=%s code=%s",
            user_id,
            code
        )
        return

    logging.info(
        "Accepted scan: user_id=%s code=%s language=%s",
        user_id,
        code,
        language
    )

    try:
        await process_scanned_qr_code(
            message=message,
            code=code,
            language=language,
        )
    except Exception as e:
        logging.exception("QR processing error: %s", e)
        await message.answer(t(language, "processing_error"))

# ==========================================
# TELEGRAM MENU BUTTON
# ==========================================

async def set_menu_button():
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text=TEXTS["ru"]["menu_button"],
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        )
        logging.info("Menu button WebApp set successfully")
    except Exception as e:
        logging.exception("Failed to set menu button: %s", e)

# ==========================================
# STARTUP / SHUTDOWN
# ==========================================

async def on_startup():
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        logging.info("Webhook deleted successfully")
    except Exception as e:
        logging.exception("Failed to delete webhook: %s", e)

    await set_menu_button()
    logging.info("Bot started successfully")


async def on_shutdown():
    try:
        await bot.session.close()
    except Exception:
        pass
    logging.info("Bot stopped")

# ==========================================
# MAIN
# ==========================================

async def main():
    await on_startup()

    try:
        await dp.start_polling(
            bot,
            polling_timeout=30,
            allowed_updates=dp.resolve_used_update_types(),
            handle_signals=True,
            close_bot_session=False,
        )
    finally:
        await on_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Application interrupted")
