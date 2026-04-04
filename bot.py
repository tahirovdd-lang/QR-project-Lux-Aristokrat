import asyncio
import csv
import logging
import os
import re
import secrets
import sqlite3
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
# CONFIG
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
# HELPERS
# =========================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


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


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_dirs():
    os.makedirs("generated_qr", exist_ok=True)
    os.makedirs("exports", exist_ok=True)


# =========================
# DB INIT
# =========================
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
# USER LEVELS
# =========================
def get_user_level(points: int) -> str:
    if points >= 1000:
        return "VIP"
    if points >= 300:
        return "Gold"
    return "Silver"


# =========================
# USER METHODS
# =========================
def ensure_user_in_db(user: types.User):
    conn = get_conn()
    cur = conn.cursor()
    current = now_str()

    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
    row = cur.fetchone()

    username = user.username or ""
    full_name = user.full_name or ""

    if row:
        cur.execute("""
            UPDATE users
            SET username = ?, full_name = ?, updated_at = ?
            WHERE user_id = ?
        """, (username, full_name, current, user.id))
    else:
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, phone, points, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user.id, username, full_name, "", 0, current, current))

    conn.commit()
    conn.close()


def get_user_by_id(user_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_user_points(user_id: int) -> int:
    row = get_user_by_id(user_id)
    return int(row["points"]) if row else 0


def change_user_points(user_id: int, delta: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET points = CASE
            WHEN points + ? < 0 THEN 0
            ELSE points + ?
        END,
        updated_at = ?
        WHERE user_id = ?
    """, (delta, delta, now_str(), user_id))
    conn.commit()
    conn.close()


def get_user_scan_history(user_id: int, limit: int = 20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.scanned_at, q.title, q.code, q.points
        FROM scans s
        JOIN qr_codes q ON q.id = s.qr_code_id
        WHERE s.user_id = ?
        ORDER BY s.id DESC
        LIMIT ?
    """, (user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_top_users(limit: int = 20):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM users
        ORDER BY points DESC, updated_at DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


# =========================
# QR METHODS
# =========================
def get_qr_by_code(code: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM qr_codes WHERE code = ?", (code,))
    row = cur.fetchone()
    conn.close()
    return row


def get_qr_by_id(qr_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM qr_codes WHERE id = ?", (qr_id,))
    row = cur.fetchone()
    conn.close()
    return row


def list_qr_codes(limit: int = 50):
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


def create_qr_code(title: str, points: int, created_by: int, custom_code: str | None = None) -> str:
    code = normalize_code(custom_code) if custom_code else f"LUX{secrets.token_hex(4).upper()}"
    if not code:
        raise ValueError("Некорректный код")
    if int(points) <= 0:
        raise ValueError("Баллы должны быть больше 0")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO qr_codes (code, title, points, is_active, created_by, created_at)
        VALUES (?, ?, ?, 1, ?, ?)
    """, (code, title.strip(), int(points), created_by, now_str()))
    conn.commit()
    conn.close()
    return code


def set_qr_active(qr_id: int, active: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE qr_codes
        SET is_active = ?
        WHERE id = ?
    """, (1 if active else 0, qr_id))
    conn.commit()
    conn.close()


def delete_qr_by_id(qr_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM scans WHERE qr_code_id = ?", (qr_id,))
    cur.execute("DELETE FROM qr_codes WHERE id = ?", (qr_id,))

    conn.commit()
    conn.close()


def save_qr_png(code: str) -> str:
    ensure_dirs()
    deep_link = build_qr_deeplink(code)
    img = qrcode.make(deep_link)
    path = os.path.join("generated_qr", f"{code}.png")
    img.save(path)
    return path


# =========================
# SCAN METHODS
# =========================
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


def register_scan(user_id: int, qr_code_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scans (user_id, qr_code_id, scanned_at)
        VALUES (?, ?, ?)
    """, (user_id, qr_code_id, now_str()))
    conn.commit()
    conn.close()


def get_total_scans_count() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM scans")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def get_total_users_count() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM users")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def get_total_qr_count() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM qr_codes")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


# =========================
# EXPORTS
# =========================
def export_users_csv() -> str:
    ensure_dirs()
    path = os.path.join("exports", f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, username, full_name, phone, points, created_at, updated_at
        FROM users
        ORDER BY points DESC, updated_at DESC
    """)
    rows = cur.fetchall()
    conn.close()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["user_id", "username", "full_name", "phone", "points", "level", "created_at", "updated_at"])
        for row in rows:
            points = int(row["points"])
            writer.writerow([
                row["user_id"],
                row["username"],
                row["full_name"],
                row["phone"],
                points,
                get_user_level(points),
                row["created_at"],
                row["updated_at"],
            ])
    return path


def export_scans_csv() -> str:
    ensure_dirs()
    path = os.path.join("exports", f"scans_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT s.id, s.user_id, u.username, u.full_name, q.code, q.title, q.points, s.scanned_at
        FROM scans s
        LEFT JOIN users u ON u.user_id = s.user_id
        LEFT JOIN qr_codes q ON q.id = s.qr_code_id
        ORDER BY s.id DESC
    """)
    rows = cur.fetchall()
    conn.close()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["scan_id", "user_id", "username", "full_name", "qr_code", "qr_title", "points", "scanned_at"])
        for row in rows:
            writer.writerow([
                row["id"],
                row["user_id"],
                row["username"],
                row["full_name"],
                row["code"],
                row["title"],
                row["points"],
                row["scanned_at"],
            ])
    return path


# =========================
# STATES
# =========================
admin_states: dict[int, dict] = {}


# =========================
# KEYBOARDS
# =========================
def main_kb(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="💎 Мои баллы"), KeyboardButton(text="📜 История")],
        [KeyboardButton(text="🏆 Мой уровень"), KeyboardButton(text="ℹ️ Как получить баллы")],
    ]

    if is_admin(user_id):
        rows.extend([
            [KeyboardButton(text="➕ Добавить QR"), KeyboardButton(text="📋 Список QR")],
            [KeyboardButton(text="⛔️ Отключить QR"), KeyboardButton(text="✅ Включить QR")],
            [KeyboardButton(text="🗑 Удалить QR"), KeyboardButton(text="🧾 Сделать QR PNG")],
            [KeyboardButton(text="🎁 Начислить баллы"), KeyboardButton(text="➖ Списать баллы")],
            [KeyboardButton(text="👤 Найти пользователя"), KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="📤 Экспорт users"), KeyboardButton(text="📤 Экспорт scans")],
            [KeyboardButton(text="🥇 Топ пользователей")],
        ])

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def qr_link_kb(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Открыть ссылку QR", url=build_qr_deeplink(code))]
    ])


# =========================
# TEXTS
# =========================
def welcome_text(user: types.User) -> str:
    return (
        f"✨ <b>Lux Aristokrat</b>\n\n"
        f"Здравствуйте, {esc(user.full_name)}!\n"
        f"Добро пожаловать в бонусную систему.\n\n"
        f"Здесь вы можете:\n"
        f"• сканировать QR-коды\n"
        f"• получать баллы\n"
        f"• смотреть историю начислений\n"
        f"• отслеживать свой уровень\n\n"
        f"Нажмите <b>«💎 Мои баллы»</b>."
    )


def how_points_text() -> str:
    return (
        "ℹ️ <b>Как получить баллы</b>\n\n"
        "1. Отсканируйте QR-код Lux Aristokrat\n"
        "2. Бот откроется автоматически\n"
        "3. Баллы начислятся на ваш аккаунт\n\n"
        "Один и тот же QR-код одному пользователю засчитывается только один раз."
    )


# =========================
# COMMANDS START
# =========================
@dp.message(CommandStart())
async def start_handler(message: types.Message, command: CommandStart):
    ensure_user_in_db(message.from_user)

    deep_arg = command.args
    if deep_arg and deep_arg.startswith("qr_"):
        code = normalize_code(deep_arg[3:])
        await process_qr_scan(message, code)
        return

    await message.answer(
        welcome_text(message.from_user),
        reply_markup=main_kb(message.from_user.id)
    )


# =========================
# QR PROCESS
# =========================
async def process_qr_scan(message: types.Message, code: str):
    ensure_user_in_db(message.from_user)

    qr_row = get_qr_by_code(code)
    if not qr_row:
        await message.answer(
            "❌ Такой QR-код не найден.",
            reply_markup=main_kb(message.from_user.id)
        )
        return

    if int(qr_row["is_active"]) != 1:
        await message.answer(
            f"⚠️ QR-код <b>{esc(qr_row['title'])}</b> сейчас отключён.",
            reply_markup=main_kb(message.from_user.id)
        )
        return

    if has_user_scanned(message.from_user.id, qr_row["id"]):
        points = get_user_points(message.from_user.id)
        level = get_user_level(points)
        await message.answer(
            f"⚠️ Вы уже сканировали этот QR-код.\n\n"
            f"Название: <b>{esc(qr_row['title'])}</b>\n"
            f"Ваш баланс: <b>{points}</b> баллов\n"
            f"Ваш уровень: <b>{level}</b>",
            reply_markup=main_kb(message.from_user.id)
        )
        return

    register_scan(message.from_user.id, qr_row["id"])
    change_user_points(message.from_user.id, int(qr_row["points"]))

    new_points = get_user_points(message.from_user.id)
    level = get_user_level(new_points)

    await message.answer(
        f"✅ <b>Баллы начислены!</b>\n\n"
        f"QR: <b>{esc(qr_row['title'])}</b>\n"
        f"Начислено: <b>+{int(qr_row['points'])}</b> баллов\n"
        f"Баланс: <b>{new_points}</b> баллов\n"
        f"Уровень: <b>{level}</b>",
        reply_markup=main_kb(message.from_user.id)
    )

    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"📥 <b>Новый скан QR</b>\n\n"
                f"ID QR: <code>{qr_row['id']}</code>\n"
                f"Код: <code>{esc(qr_row['code'])}</code>\n"
                f"Название: <b>{esc(qr_row['title'])}</b>\n"
                f"Баллы: <b>{int(qr_row['points'])}</b>\n\n"
                f"Пользователь: {esc(tg_name(message.from_user))}\n"
                f"User ID: <code>{message.from_user.id}</code>"
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление админу")


# =========================
# USER BUTTONS
# =========================
@dp.message(F.text == "💎 Мои баллы")
async def my_points_btn(message: types.Message):
    ensure_user_in_db(message.from_user)
    points = get_user_points(message.from_user.id)
    level = get_user_level(points)

    await message.answer(
        f"💎 Ваш баланс: <b>{points}</b> баллов\n"
        f"🏆 Ваш уровень: <b>{level}</b>",
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text == "🏆 Мой уровень")
async def my_level_btn(message: types.Message):
    ensure_user_in_db(message.from_user)
    points = get_user_points(message.from_user.id)
    level = get_user_level(points)

    text = (
        f"🏆 Ваш уровень: <b>{level}</b>\n"
        f"Баллы: <b>{points}</b>\n\n"
        f"Уровни:\n"
        f"• Silver: 0–299\n"
        f"• Gold: 300–999\n"
        f"• VIP: 1000+"
    )
    await message.answer(text, reply_markup=main_kb(message.from_user.id))


@dp.message(F.text == "📜 История")
async def history_btn(message: types.Message):
    ensure_user_in_db(message.from_user)
    rows = get_user_scan_history(message.from_user.id, 20)

    if not rows:
        await message.answer(
            "📜 История пока пустая.",
            reply_markup=main_kb(message.from_user.id)
        )
        return

    parts = ["📜 <b>Ваша история сканов</b>\n"]
    for row in rows:
        parts.append(
            f"• <b>{esc(row['title'])}</b>\n"
            f"  Код: <code>{esc(row['code'])}</code>\n"
            f"  Баллы: +{int(row['points'])}\n"
            f"  Дата: {esc(row['scanned_at'])}\n"
        )

    await message.answer("\n".join(parts), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text == "ℹ️ Как получить баллы")
async def how_points_btn(message: types.Message):
    await message.answer(how_points_text(), reply_markup=main_kb(message.from_user.id))


# =========================
# ADMIN ENTRY COMMANDS
# =========================
@dp.message(F.text == "➕ Добавить QR")
@dp.message(Command("add_qr"))
async def add_qr_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    admin_states[message.from_user.id] = {"mode": "add_qr"}
    await message.answer(
        "➕ <b>Добавление QR</b>\n\n"
        "Отправьте:\n"
        "<code>Название|Баллы</code>\n"
        "или\n"
        "<code>CODE|Название|Баллы</code>"
    )


@dp.message(F.text == "📋 Список QR")
@dp.message(Command("list_qr"))
async def list_qr_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    rows = list_qr_codes(50)
    if not rows:
        await message.answer("Список QR пуст.")
        return

    parts = ["📋 <b>Список QR-кодов</b>\n"]
    for row in rows:
        status = "✅" if int(row["is_active"]) == 1 else "⛔️"
        parts.append(
            f"{status} ID: <code>{row['id']}</code>\n"
            f"Название: <b>{esc(row['title'])}</b>\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Баллы: <b>{int(row['points'])}</b>\n"
        )

    await message.answer("\n".join(parts))


@dp.message(F.text == "⛔️ Отключить QR")
async def disable_qr_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "disable_qr"}
    await message.answer("Отправьте <b>ID QR</b>, который нужно отключить.")


@dp.message(F.text == "✅ Включить QR")
async def enable_qr_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "enable_qr"}
    await message.answer("Отправьте <b>ID QR</b>, который нужно включить.")


@dp.message(F.text == "🗑 Удалить QR")
async def delete_qr_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "delete_qr"}
    await message.answer("Отправьте <b>ID QR</b>, который нужно удалить.")


@dp.message(F.text == "🧾 Сделать QR PNG")
@dp.message(Command("make_qr"))
async def make_qr_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2 and parts[1].isdigit():
        qr_id = int(parts[1])
        row = get_qr_by_id(qr_id)
        if not row:
            await message.answer("❌ QR с таким ID не найден.")
            return

        path = save_qr_png(row["code"])
        await message.answer_photo(
            photo=FSInputFile(path),
            caption=(
                f"🧾 <b>QR PNG</b>\n\n"
                f"ID: <code>{row['id']}</code>\n"
                f"Название: <b>{esc(row['title'])}</b>\n"
                f"Код: <code>{esc(row['code'])}</code>\n"
                f"Баллы: <b>{int(row['points'])}</b>\n"
                f"Ссылка:\n<code>{esc(build_qr_deeplink(row['code']))}</code>"
            ),
            reply_markup=qr_link_kb(row["code"])
        )
        return

    admin_states[message.from_user.id] = {"mode": "make_qr_png"}
    await message.answer("Отправьте <b>ID QR</b>, для которого сделать PNG.")


@dp.message(F.text == "🎁 Начислить баллы")
async def add_points_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "add_points"}
    await message.answer("Отправьте:\n<code>user_id|баллы</code>")


@dp.message(F.text == "➖ Списать баллы")
async def minus_points_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "minus_points"}
    await message.answer("Отправьте:\n<code>user_id|баллы</code>")


@dp.message(F.text == "👤 Найти пользователя")
async def find_user_enter(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "find_user"}
    await message.answer("Отправьте <b>ID пользователя</b>.")


@dp.message(F.text == "📊 Статистика")
async def stats_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    users_count = get_total_users_count()
    qr_count = get_total_qr_count()
    scans_count = get_total_scans_count()

    top_users = get_top_users(5)
    lines = [
        "📊 <b>Статистика</b>\n",
        f"Пользователей: <b>{users_count}</b>",
        f"QR-кодов: <b>{qr_count}</b>",
        f"Сканов: <b>{scans_count}</b>",
        "",
        "<b>Топ 5 пользователей:</b>"
    ]

    if top_users:
        for i, row in enumerate(top_users, start=1):
            name = row["full_name"] or row["username"] or row["user_id"]
            points = int(row["points"])
            level = get_user_level(points)
            lines.append(f"{i}. {esc(str(name))} — <b>{points}</b> ({level})")
    else:
        lines.append("Пока нет данных.")

    await message.answer("\n".join(lines))


@dp.message(F.text == "📤 Экспорт users")
async def export_users_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    path = export_users_csv()
    await message.answer_document(
        document=FSInputFile(path),
        caption="📤 Экспорт пользователей готов."
    )


@dp.message(F.text == "📤 Экспорт scans")
async def export_scans_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    path = export_scans_csv()
    await message.answer_document(
        document=FSInputFile(path),
        caption="📤 Экспорт сканов готов."
    )


@dp.message(F.text == "🥇 Топ пользователей")
async def top_users_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    rows = get_top_users(20)
    if not rows:
        await message.answer("Пользователей пока нет.")
        return

    lines = ["🥇 <b>Топ пользователей</b>\n"]
    for i, row in enumerate(rows, start=1):
        name = row["full_name"] or row["username"] or row["user_id"]
        points = int(row["points"])
        level = get_user_level(points)
        lines.append(
            f"{i}. {esc(str(name))}\n"
            f"   ID: <code>{row['user_id']}</code>\n"
            f"   Баллы: <b>{points}</b>\n"
            f"   Уровень: <b>{level}</b>\n"
        )

    await message.answer("\n".join(lines))


@dp.message(Command("cancel"))
async def cancel_handler(message: types.Message):
    admin_states.pop(message.from_user.id, None)
    await message.answer("Отменено.", reply_markup=main_kb(message.from_user.id))


# =========================
# ADMIN STATE ROUTER
# =========================
@dp.message(F.text)
async def admin_state_router(message: types.Message):
    ensure_user_in_db(message.from_user)

    if not is_admin(message.from_user.id):
        return

    state = admin_states.get(message.from_user.id)
    if not state:
        return

    mode = state.get("mode")
    text = (message.text or "").strip()

    # -------- ADD QR
    if mode == "add_qr":
        parts = [p.strip() for p in text.split("|")]
        try:
            if len(parts) == 2:
                title, points_str = parts
                code = create_qr_code(title=title, points=int(points_str), created_by=message.from_user.id)
            elif len(parts) == 3:
                custom_code, title, points_str = parts
                code = create_qr_code(title=title, points=int(points_str), created_by=message.from_user.id, custom_code=custom_code)
            else:
                await message.answer("❌ Формат неверный.")
                return

            row = get_qr_by_code(code)
            path = save_qr_png(code)
            admin_states.pop(message.from_user.id, None)

            await message.answer_photo(
                photo=FSInputFile(path),
                caption=(
                    f"✅ <b>QR создан</b>\n\n"
                    f"ID: <code>{row['id']}</code>\n"
                    f"Название: <b>{esc(row['title'])}</b>\n"
                    f"Код: <code>{esc(row['code'])}</code>\n"
                    f"Баллы: <b>{int(row['points'])}</b>\n"
                    f"Ссылка:\n<code>{esc(build_qr_deeplink(code))}</code>"
                ),
                reply_markup=qr_link_kb(code)
            )
            return
        except sqlite3.IntegrityError:
            await message.answer("❌ Такой код уже существует.")
            return
        except ValueError as e:
            await message.answer(f"❌ Ошибка: {esc(str(e))}")
            return
        except Exception as e:
            logger.exception("Ошибка add_qr")
            await message.answer(f"❌ Ошибка: <code>{esc(str(e))}</code>")
            return

    # -------- DISABLE QR
    if mode == "disable_qr":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        qr_id = int(text)
        row = get_qr_by_id(qr_id)
        if not row:
            await message.answer("❌ QR не найден.")
            return
        set_qr_active(qr_id, False)
        admin_states.pop(message.from_user.id, None)
        await message.answer(f"⛔️ QR <b>{esc(row['title'])}</b> отключён.")
        return

    # -------- ENABLE QR
    if mode == "enable_qr":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        qr_id = int(text)
        row = get_qr_by_id(qr_id)
        if not row:
            await message.answer("❌ QR не найден.")
            return
        set_qr_active(qr_id, True)
        admin_states.pop(message.from_user.id, None)
        await message.answer(f"✅ QR <b>{esc(row['title'])}</b> включён.")
        return

    # -------- DELETE QR
    if mode == "delete_qr":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        qr_id = int(text)
        row = get_qr_by_id(qr_id)
        if not row:
            await message.answer("❌ QR не найден.")
            return
        delete_qr_by_id(qr_id)
        admin_states.pop(message.from_user.id, None)
        await message.answer(f"🗑 QR <b>{esc(row['title'])}</b> удалён.")
        return

    # -------- MAKE PNG
    if mode == "make_qr_png":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        qr_id = int(text)
        row = get_qr_by_id(qr_id)
        if not row:
            await message.answer("❌ QR не найден.")
            return

        path = save_qr_png(row["code"])
        admin_states.pop(message.from_user.id, None)
        await message.answer_photo(
            photo=FSInputFile(path),
            caption=(
                f"🧾 <b>QR PNG</b>\n\n"
                f"ID: <code>{row['id']}</code>\n"
                f"Название: <b>{esc(row['title'])}</b>\n"
                f"Код: <code>{esc(row['code'])}</code>\n"
                f"Баллы: <b>{int(row['points'])}</b>\n"
                f"Ссылка:\n<code>{esc(build_qr_deeplink(row['code']))}</code>"
            ),
            reply_markup=qr_link_kb(row["code"])
        )
        return

    # -------- ADD POINTS
    if mode == "add_points":
        parts = [p.strip() for p in text.split("|")]
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
            if points <= 0:
                raise ValueError("Баллы должны быть больше 0")
        except ValueError:
            await message.answer("❌ Баллы должны быть положительным числом.")
            return

        row = get_user_by_id(user_id)
        if not row:
            await message.answer("❌ Пользователь не найден.")
            return

        change_user_points(user_id, points)
        new_balance = get_user_points(user_id)
        level = get_user_level(new_balance)
        admin_states.pop(message.from_user.id, None)

        await message.answer(
            f"✅ Пользователю <code>{user_id}</code> начислено <b>+{points}</b> баллов.\n"
            f"Новый баланс: <b>{new_balance}</b>\n"
            f"Уровень: <b>{level}</b>"
        )

        try:
            await bot.send_message(
                user_id,
                f"🎁 Вам начислено <b>+{points}</b> баллов.\n"
                f"Баланс: <b>{new_balance}</b>\n"
                f"Уровень: <b>{level}</b>"
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление пользователю")
        return

    # -------- MINUS POINTS
    if mode == "minus_points":
        parts = [p.strip() for p in text.split("|")]
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
            if points <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❌ Баллы должны быть положительным числом.")
            return

        row = get_user_by_id(user_id)
        if not row:
            await message.answer("❌ Пользователь не найден.")
            return

        change_user_points(user_id, -points)
        new_balance = get_user_points(user_id)
        level = get_user_level(new_balance)
        admin_states.pop(message.from_user.id, None)

        await message.answer(
            f"➖ У пользователя <code>{user_id}</code> списано <b>{points}</b> баллов.\n"
            f"Новый баланс: <b>{new_balance}</b>\n"
            f"Уровень: <b>{level}</b>"
        )

        try:
            await bot.send_message(
                user_id,
                f"➖ У вас списано <b>{points}</b> баллов.\n"
                f"Баланс: <b>{new_balance}</b>\n"
                f"Уровень: <b>{level}</b>"
            )
        except Exception:
            logger.exception("Не удалось отправить уведомление пользователю")
        return

    # -------- FIND USER
    if mode == "find_user":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return

        user_id = int(text)
        row = get_user_by_id(user_id)
        if not row:
            await message.answer("❌ Пользователь не найден.")
            return

        points = int(row["points"])
        level = get_user_level(points)
        history = get_user_scan_history(user_id, 5)
        admin_states.pop(message.from_user.id, None)

        lines = [
            "👤 <b>Пользователь</b>\n",
            f"ID: <code>{row['user_id']}</code>",
            f"Username: @{esc(row['username']) if row['username'] else '—'}",
            f"Имя: <b>{esc(row['full_name']) or '—'}</b>",
            f"Телефон: <b>{esc(row['phone']) or '—'}</b>",
            f"Баллы: <b>{points}</b>",
            f"Уровень: <b>{level}</b>",
            f"Создан: <code>{esc(row['created_at'])}</code>",
            "",
            "<b>Последние сканы:</b>"
        ]

        if history:
            for h in history:
                lines.append(
                    f"• {esc(h['title'])} | +{int(h['points'])} | {esc(h['scanned_at'])}"
                )
        else:
            lines.append("История пуста.")

        await message.answer("\n".join(lines))
        return


# =========================
# FALLBACK
# =========================
@dp.message()
async def fallback_handler(message: types.Message):
    ensure_user_in_db(message.from_user)
    await message.answer(
        "Выберите действие через кнопки меню.",
        reply_markup=main_kb(message.from_user.id)
    )


# =========================
# MAIN
# =========================
async def main():
    ensure_dirs()
    init_db()
    logger.info("Lux Aristokrat bot started")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
