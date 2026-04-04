import asyncio
import logging
import os
import re
import sqlite3
import secrets
from datetime import datetime
from urllib.parse import quote

from dotenv import load_dotenv
import qrcode

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)

# =========================
# ЗАГРУЗКА НАСТРОЕК
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN не найден в .env")

BOT_USERNAME = os.getenv("BOT_USERNAME", "lux_aristokrat_bot").replace("@", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "lux_aristokrat.db")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# =========================
# БАЗА ДАННЫХ SQLITE
# =========================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            phone TEXT,
            points INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS qr_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            points INTEGER NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_by INTEGER,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            qr_code_id INTEGER NOT NULL,
            scanned_at TEXT NOT NULL,
            UNIQUE(user_id, qr_code_id)
        )
    """)

    conn.commit()
    conn.close()


# =========================
# УТИЛИТЫ
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def esc(s: str) -> str:
    return (s or "").replace("<", "&lt;").replace(">", "&gt;")


def tg_name(user: types.User) -> str:
    if user.username:
        return f"@{user.username}"
    return user.full_name or str(user.id)


def normalize_code(raw: str) -> str:
    raw = (raw or "").strip().upper()
    raw = re.sub(r"[^A-Z0-9_-]+", "", raw)
    return raw


def build_qr_deeplink(code: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={quote('qr_' + code)}"


def ensure_user(message: types.Message):
    user = message.from_user
    if not user:
        return

    conn = get_conn()
    cur = conn.cursor()

    full_name = user.full_name or ""
    username = user.username or ""
    created = now_str()

    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
    row = cur.fetchone()

    if row:
        cur.execute("""
            UPDATE users
            SET username = ?, full_name = ?, updated_at = ?
            WHERE user_id = ?
        """, (username, full_name, created, user.id))
    else:
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, phone, points, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, ?)
        """, (user.id, username, full_name, "", created, created))

    conn.commit()
    conn.close()


def get_user_points(user_id: int) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT points FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return int(row["points"]) if row else 0


def add_points_to_user(user_id: int, points: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET points = points + ?, updated_at = ?
        WHERE user_id = ?
    """, (points, now_str(), user_id))
    conn.commit()
    conn.close()


def get_qr_by_code(code: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM qr_codes
        WHERE code = ?
    """, (code,))
    row = cur.fetchone()
    conn.close()
    return row


def has_user_scanned(user_id: int, qr_code_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id FROM scans
        WHERE user_id = ? AND qr_code_id = ?
    """, (user_id, qr_code_id))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_scan(user_id: int, qr_code_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scans (user_id, qr_code_id, scanned_at)
        VALUES (?, ?, ?)
    """, (user_id, qr_code_id, now_str()))
    conn.commit()
    conn.close()


def create_qr_code(title: str, points: int, created_by: int, custom_code: str | None = None) -> str:
    code = normalize_code(custom_code) if custom_code else f"LUX{secrets.token_hex(4).upper()}"
    if not code:
        raise ValueError("Некорректный код")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO qr_codes (code, title, points, is_active, created_by, created_at)
        VALUES (?, ?, ?, 1, ?, ?)
    """, (code, title.strip(), int(points), created_by, now_str()))
    conn.commit()
    conn.close()
    return code


def list_qr_codes(limit: int = 30):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM qr_codes
        ORDER BY id DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_user_info_by_id(user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM users
        WHERE user_id = ?
    """, (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def save_qr_png(code: str) -> str:
    link = build_qr_deeplink(code)
    img = qrcode.make(link)
    os.makedirs("generated_qr", exist_ok=True)
    path = os.path.join("generated_qr", f"{code}.png")
    img.save(path)
    return path


# =========================
# СОСТОЯНИЕ ДЛЯ АДМИНА
# =========================
admin_states: dict[int, dict] = {}
# Пример:
# admin_states[user_id] = {"mode": "add_qr_wait_data"}
# admin_states[user_id] = {"mode": "manual_add_points_wait_data"}


# =========================
# КЛАВИАТУРЫ
# =========================
def main_kb(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="💎 Мои баллы")],
        [KeyboardButton(text="ℹ️ Как получить баллы")],
    ]

    if is_admin(user_id):
        rows.extend([
            [KeyboardButton(text="➕ Добавить QR"), KeyboardButton(text="📋 Список QR")],
            [KeyboardButton(text="🎁 Начислить баллы"), KeyboardButton(text="👤 Найти пользователя")],
        ])

    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True
    )


def admin_qr_actions_kb(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Ссылка QR", url=build_qr_deeplink(code))]
    ])


# =========================
# ТЕКСТЫ
# =========================
def welcome_text(user: types.User) -> str:
    return (
        f"✨ <b>Lux Aristokrat</b>\n\n"
        f"Здравствуйте, {esc(user.full_name)}!\n"
        f"Это бонусный бот.\n\n"
        f"Что можно делать:\n"
        f"• сканировать QR-коды\n"
        f"• получать бонусные баллы\n"
        f"• смотреть свой баланс\n\n"
        f"Нажмите <b>«💎 Мои баллы»</b>, чтобы увидеть баланс."
    )


def help_points_text() -> str:
    return (
        "ℹ️ <b>Как получить баллы</b>\n\n"
        "1. Отсканируйте QR-код Lux Aristokrat\n"
        "2. Откроется бот\n"
        "3. Баллы начислятся автоматически\n\n"
        "Один и тот же QR-код одному пользователю начисляется только один раз."
    )


# =========================
# ОБРАБОТКА /start
# =========================
@dp.message(CommandStart())
async def start_handler(message: types.Message, command: CommandStart):
    ensure_user(message)

    deep_link_arg = command.args
    if deep_link_arg and deep_link_arg.startswith("qr_"):
        code = normalize_code(deep_link_arg[3:])
        await process_qr_scan(message, code)
        return

    await message.answer(
        welcome_text(message.from_user),
        reply_markup=main_kb(message.from_user.id)
    )


# =========================
# QR-СКАН / НАЧИСЛЕНИЕ
# =========================
async def process_qr_scan(message: types.Message, code: str):
    ensure_user(message)
    user = message.from_user
    qr_row = get_qr_by_code(code)

    if not qr_row:
        await message.answer(
            "❌ Такой QR-код не найден.\n"
            "Проверьте код или обратитесь к администратору.",
            reply_markup=main_kb(user.id)
        )
        return

    if int(qr_row["is_active"]) != 1:
        await message.answer(
            "⚠️ Этот QR-код отключён.",
            reply_markup=main_kb(user.id)
        )
        return

    if has_user_scanned(user.id, qr_row["id"]):
        points = get_user_points(user.id)
        await message.answer(
            f"⚠️ Вы уже сканировали этот QR-код.\n\n"
            f"<b>{esc(qr_row['title'])}</b>\n"
            f"Ваш баланс: <b>{points}</b> баллов.",
            reply_markup=main_kb(user.id)
        )
        return

    mark_scan(user.id, qr_row["id"])
    add_points_to_user(user.id, int(qr_row["points"]))
    new_balance = get_user_points(user.id)

    await message.answer(
        f"✅ <b>Баллы начислены!</b>\n\n"
        f"QR: <b>{esc(qr_row['title'])}</b>\n"
        f"Начислено: <b>+{int(qr_row['points'])}</b> баллов\n"
        f"Ваш баланс: <b>{new_balance}</b> баллов",
        reply_markup=main_kb(user.id)
    )

    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"📥 <b>Новый скан QR</b>\n\n"
                f"Код: <code>{esc(qr_row['code'])}</code>\n"
                f"Название: <b>{esc(qr_row['title'])}</b>\n"
                f"Баллы: <b>{int(qr_row['points'])}</b>\n\n"
                f"Пользователь: {esc(tg_name(user))}\n"
                f"ID: <code>{user.id}</code>"
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление админу")


# =========================
# КОМАНДЫ ПОЛЬЗОВАТЕЛЯ
# =========================
@dp.message(Command("points"))
async def points_cmd(message: types.Message):
    ensure_user(message)
    points = get_user_points(message.from_user.id)
    await message.answer(
        f"💎 Ваш баланс: <b>{points}</b> баллов",
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text == "💎 Мои баллы")
async def points_btn(message: types.Message):
    await points_cmd(message)


@dp.message(F.text == "ℹ️ Как получить баллы")
async def how_points_btn(message: types.Message):
    await message.answer(
        help_points_text(),
        reply_markup=main_kb(message.from_user.id)
    )


# =========================
# АДМИН: ДОБАВИТЬ QR
# Формат:
# Название|Баллы
# или
# CODE|Название|Баллы
# =========================
@dp.message(Command("add_qr"))
async def add_qr_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    admin_states[message.from_user.id] = {"mode": "add_qr_wait_data"}
    await message.answer(
        "➕ <b>Добавление нового QR</b>\n\n"
        "Отправьте данные в одном из форматов:\n\n"
        "<code>Покупка в бутике|10</code>\n"
        "или\n"
        "<code>LUXVIP001|Покупка в бутике|10</code>\n\n"
        "Где:\n"
        "• CODE — необязательный код\n"
        "• Название — описание QR\n"
        "• Баллы — сколько начислять"
    )


@dp.message(F.text == "➕ Добавить QR")
async def add_qr_btn(message: types.Message):
    await add_qr_cmd(message)


# =========================
# АДМИН: СПИСОК QR
# =========================
@dp.message(Command("list_qr"))
async def list_qr_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    rows = list_qr_codes(30)
    if not rows:
        await message.answer("Список QR пуст.")
        return

    parts = ["📋 <b>Последние QR-коды</b>\n"]
    for row in rows:
        status = "✅" if int(row["is_active"]) == 1 else "⛔️"
        parts.append(
            f"{status} <b>{esc(row['title'])}</b>\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Баллы: <b>{int(row['points'])}</b>\n"
        )

    await message.answer("\n".join(parts))


@dp.message(F.text == "📋 Список QR")
async def list_qr_btn(message: types.Message):
    await list_qr_cmd(message)


# =========================
# АДМИН: НАЙТИ ПОЛЬЗОВАТЕЛЯ
# =========================
@dp.message(Command("find_user"))
async def find_user_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    admin_states[message.from_user.id] = {"mode": "find_user_wait_id"}
    await message.answer(
        "👤 Отправьте <b>ID пользователя</b>, которого нужно найти."
    )


@dp.message(F.text == "👤 Найти пользователя")
async def find_user_btn(message: types.Message):
    await find_user_cmd(message)


# =========================
# АДМИН: НАЧИСЛИТЬ БАЛЛЫ ВРУЧНУЮ
# Формат:
# user_id|баллы
# =========================
@dp.message(Command("manual_points"))
async def manual_points_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    admin_states[message.from_user.id] = {"mode": "manual_add_points_wait_data"}
    await message.answer(
        "🎁 Отправьте данные в формате:\n"
        "<code>123456789|50</code>"
    )


@dp.message(F.text == "🎁 Начислить баллы")
async def manual_points_btn(message: types.Message):
    await manual_points_cmd(message)


# =========================
# ОБРАБОТКА СОСТОЯНИЙ АДМИНА
# =========================
@dp.message(F.text)
async def admin_state_router(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    state = admin_states.get(message.from_user.id)
    if not state:
        return

    mode = state.get("mode")
    text = (message.text or "").strip()

    # --- Добавление QR
    if mode == "add_qr_wait_data":
        parts = [x.strip() for x in text.split("|")]

        try:
            if len(parts) == 2:
                title, points_str = parts
                code = create_qr_code(
                    title=title,
                    points=int(points_str),
                    created_by=message.from_user.id
                )
            elif len(parts) == 3:
                custom_code, title, points_str = parts
                code = create_qr_code(
                    title=title,
                    points=int(points_str),
                    created_by=message.from_user.id,
                    custom_code=custom_code
                )
            else:
                await message.answer(
                    "❌ Неверный формат.\n"
                    "Используйте:\n"
                    "<code>Название|Баллы</code>\n"
                    "или\n"
                    "<code>CODE|Название|Баллы</code>"
                )
                return

            png_path = save_qr_png(code)
            qr_row = get_qr_by_code(code)

            admin_states.pop(message.from_user.id, None)

            caption = (
                f"✅ <b>QR-код создан</b>\n\n"
                f"Название: <b>{esc(qr_row['title'])}</b>\n"
                f"Код: <code>{esc(qr_row['code'])}</code>\n"
                f"Баллы: <b>{int(qr_row['points'])}</b>\n"
                f"Ссылка:\n<code>{esc(build_qr_deeplink(code))}</code>"
            )

            await message.answer_photo(
                photo=FSInputFile(png_path),
                caption=caption,
                reply_markup=admin_qr_actions_kb(code)
            )
            return

        except sqlite3.IntegrityError:
            await message.answer("❌ Такой код уже существует. Укажите другой CODE.")
            return
        except ValueError:
            await message.answer("❌ Баллы должны быть числом.")
            return
        except Exception as e:
            logger.exception("Ошибка создания QR")
            await message.answer(f"❌ Ошибка: <code>{esc(str(e))}</code>")
            return

    # --- Поиск пользователя
    if mode == "find_user_wait_id":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return

        user_row = get_user_info_by_id(int(text))
        admin_states.pop(message.from_user.id, None)

        if not user_row:
            await message.answer("Пользователь не найден.")
            return

        await message.answer(
            f"👤 <b>Пользователь</b>\n\n"
            f"ID: <code>{user_row['user_id']}</code>\n"
            f"Username: @{esc(user_row['username']) if user_row['username'] else '—'}\n"
            f"Имя: <b>{esc(user_row['full_name']) or '—'}</b>\n"
            f"Телефон: <b>{esc(user_row['phone']) or '—'}</b>\n"
            f"Баллы: <b>{int(user_row['points'])}</b>\n"
            f"Создан: <code>{esc(user_row['created_at'])}</code>"
        )
        return

    # --- Ручное начисление
    if mode == "manual_add_points_wait_data":
        parts = [x.strip() for x in text.split("|")]
        if len(parts) != 2:
            await message.answer("❌ Формат: <code>user_id|баллы</code>")
            return

        user_id_str, points_str = parts
        if not user_id_str.isdigit():
            await message.answer("❌ user_id должен быть числом.")
            return

        try:
            user_id = int(user_id_str)
            points = int(points_str)
        except ValueError:
            await message.answer("❌ Баллы должны быть числом.")
            return

        user_row = get_user_info_by_id(user_id)
        if not user_row:
            await message.answer("❌ Пользователь не найден.")
            return

        add_points_to_user(user_id, points)
        new_balance = get_user_points(user_id)
        admin_states.pop(message.from_user.id, None)

        await message.answer(
            f"✅ Начислено <b>{points}</b> баллов пользователю <code>{user_id}</code>.\n"
            f"Новый баланс: <b>{new_balance}</b>"
        )

        try:
            await bot.send_message(
                user_id,
                f"🎁 Вам начислено <b>{points}</b> баллов.\n"
                f"Новый баланс: <b>{new_balance}</b>"
            )
        except Exception:
            logger.exception("Не удалось уведомить пользователя")
        return


# =========================
# КОМАНДА ОТМЕНЫ
# =========================
@dp.message(Command("cancel"))
async def cancel_cmd(message: types.Message):
    admin_states.pop(message.from_user.id, None)
    await message.answer("Отменено.", reply_markup=main_kb(message.from_user.id))


# =========================
# /make_qr CODE
# =========================
@dp.message(Command("make_qr"))
async def make_qr_cmd(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/make_qr CODE</code>")
        return

    code = normalize_code(parts[1])
    qr_row = get_qr_by_code(code)
    if not qr_row:
        await message.answer("❌ Такой QR-код не найден.")
        return

    png_path = save_qr_png(code)
    await message.answer_photo(
        photo=FSInputFile(png_path),
        caption=(
            f"🧾 <b>QR для Lux Aristokrat</b>\n\n"
            f"Название: <b>{esc(qr_row['title'])}</b>\n"
            f"Код: <code>{esc(qr_row['code'])}</code>\n"
            f"Баллы: <b>{int(qr_row['points'])}</b>\n"
            f"Ссылка:\n<code>{esc(build_qr_deeplink(code))}</code>"
        ),
        reply_markup=admin_qr_actions_kb(code)
    )


# =========================
# ПРОЧИЕ СООБЩЕНИЯ
# =========================
@dp.message()
async def fallback_handler(message: types.Message):
    ensure_user(message)
    await message.answer(
        "Выберите действие через кнопки ниже.",
        reply_markup=main_kb(message.from_user.id)
    )


# =========================
# ЗАПУСК
# =========================
async def main():
    init_db()
    logger.info("Bot is starting...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
