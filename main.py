print("=== LUX ARISTOKRAT FINAL LOGIC VERSION ===")

import asyncio
import csv
import logging
import os
import re
import secrets
import sqlite3
import subprocess
import sys
from datetime import datetime
from urllib.parse import quote

logging.basicConfig(level=logging.INFO)

try:
    from aiogram import Bot, Dispatcher, F
    from aiogram.client.default import DefaultBotProperties
    from aiogram.filters import CommandStart, Command
    from aiogram.types import (
        Message,
        ReplyKeyboardMarkup,
        KeyboardButton,
        InlineKeyboardMarkup,
        InlineKeyboardButton,
        FSInputFile,
    )
except ModuleNotFoundError:
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--no-cache-dir", "aiogram==3.22.0"
    ])
    from aiogram import Bot, Dispatcher, F
    from aiogram.client.default import DefaultBotProperties
    from aiogram.filters import CommandStart, Command
    from aiogram.types import (
        Message,
        ReplyKeyboardMarkup,
        KeyboardButton,
        InlineKeyboardMarkup,
        InlineKeyboardButton,
        FSInputFile,
    )

try:
    import qrcode
except ModuleNotFoundError:
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--no-cache-dir", "qrcode==8.2", "pillow==11.3.0"
    ])
    import qrcode


BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found")

BOT_USERNAME = os.getenv("BOT_USERNAME", "QR_Lux_Aristokrat_bot").replace("@", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DB_PATH = os.getenv("DB_PATH", "lux_aristokrat.db")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

os.makedirs(DATA_DIR, exist_ok=True)
QR_DIR = os.path.join(DATA_DIR, "generated_qr")
EXPORT_DIR = os.path.join(DATA_DIR, "exports")
os.makedirs(QR_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

DB_FILE = os.path.join(DATA_DIR, DB_PATH) if not os.path.isabs(DB_PATH) else DB_PATH

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

admin_states: dict[int, dict] = {}
user_states: dict[int, dict] = {}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def esc(value) -> str:
    if value is None:
        return ""
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def tg_label(user) -> str:
    if user.username:
        return f"@{user.username}"
    return user.full_name or str(user.id)


def normalize_code(raw: str) -> str:
    raw = (raw or "").strip().upper()
    raw = re.sub(r"[^A-Z0-9_-]+", "", raw)
    return raw


def normalize_bulk_code(raw: str) -> str:
    raw = (raw or "").strip()

    replacements = {
        ">": "_",
        "<": "_",
        "!": "_",
        "&": "_",
        "'": "_",
        '"': "_",
        ".": "_",
        ",": "_",
        " ": "_",
        "/": "_",
        "\\": "_",
        ":": "_",
        ";": "_",
        "?": "_",
        "=": "_",
        "+": "_",
        "#": "_",
        "%": "_",
        "@": "_",
        "*": "_",
        "(": "_",
        ")": "_",
        "[": "_",
        "]": "_",
        "{": "_",
        "}": "_",
    }

    for old, new in replacements.items():
        raw = raw.replace(old, new)

    raw = re.sub(r"_+", "_", raw)
    raw = raw.strip("_")
    return normalize_code(raw)


def build_qr_link(code: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={quote('qr_' + code)}"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
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


def get_level(points: int) -> str:
    if points >= 1000:
        return "VIP"
    if points >= 300:
        return "Gold"
    return "Silver"


def ensure_user_in_db(user):
    conn = get_conn()
    cur = conn.cursor()
    ts = now_str()

    cur.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
    row = cur.fetchone()

    username = user.username or ""
    full_name = user.full_name or ""

    if row:
        cur.execute("""
            UPDATE users
            SET username = ?, full_name = ?, updated_at = ?
            WHERE user_id = ?
        """, (username, full_name, ts, user.id))
    else:
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, phone, points, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, ?, ?)
        """, (user.id, username, full_name, "", ts, ts))

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


def get_user_history(user_id: int, limit: int = 20):
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


def create_qr(title: str, points: int, created_by: int, custom_code: str | None = None) -> str:
    code = normalize_code(custom_code) if custom_code else f"LUX{secrets.token_hex(4).upper()}"
    if not code:
        raise ValueError("Некорректный код")
    if points <= 0:
        raise ValueError("Баллы должны быть больше 0")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO qr_codes (code, title, points, is_active, created_by, created_at)
        VALUES (?, ?, ?, 1, ?, ?)
    """, (code, title.strip(), points, created_by, now_str()))
    conn.commit()
    conn.close()
    return code


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


def list_qr(limit: int = 50):
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


def set_qr_active(qr_id: int, active: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE qr_codes SET is_active = ? WHERE id = ?", (1 if active else 0, qr_id))
    conn.commit()
    conn.close()


def delete_qr(qr_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM scans WHERE qr_code_id = ?", (qr_id,))
    cur.execute("DELETE FROM qr_codes WHERE id = ?", (qr_id,))
    conn.commit()
    conn.close()


def has_scan(user_id: int, qr_code_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id
        FROM scans
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


def save_qr_png(code: str) -> str:
    img = qrcode.make(build_qr_link(code))
    path = os.path.join(QR_DIR, f"{code}.png")
    img.save(path)
    return path


def get_total_users() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM users")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def get_total_qr() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM qr_codes")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def get_total_scans() -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM scans")
    row = cur.fetchone()
    conn.close()
    return int(row["c"])


def get_top_users(limit: int = 10):
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


def export_users_csv() -> str:
    path = os.path.join(EXPORT_DIR, f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
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
                get_level(points),
                row["created_at"],
                row["updated_at"],
            ])
    return path


def export_scans_csv() -> str:
    path = os.path.join(EXPORT_DIR, f"scans_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
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


def main_kb(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📷 Сканировать QR"), KeyboardButton(text="💎 Мои баллы")],
        [KeyboardButton(text="📜 История"), KeyboardButton(text="🏆 Мой уровень")],
        [KeyboardButton(text="ℹ️ Как получить баллы")],
    ]

    if is_admin(user_id):
        rows.extend([
            [KeyboardButton(text="➕ Добавить QR"), KeyboardButton(text="📥 Bulk QR")],
            [KeyboardButton(text="📋 Список QR"), KeyboardButton(text="🧾 Сделать QR PNG")],
            [KeyboardButton(text="⛔️ Отключить QR"), KeyboardButton(text="✅ Включить QR")],
            [KeyboardButton(text="🗑 Удалить QR"), KeyboardButton(text="👤 Найти пользователя")],
            [KeyboardButton(text="🎁 Начислить баллы"), KeyboardButton(text="➖ Списать баллы")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🥇 Топ пользователей")],
            [KeyboardButton(text="📤 Экспорт users"), KeyboardButton(text="📤 Экспорт scans")],
        ])

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def qr_link_kb(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Открыть QR ссылку", url=build_qr_link(code))]
        ]
    )


def welcome_text(user) -> str:
    return (
        f"✨ <b>Lux Aristokrat</b>\n\n"
        f"Здравствуйте, {esc(user.full_name)}!\n"
        f"Добро пожаловать в бонусную систему.\n\n"
        f"Здесь вы можете:\n"
        f"• сканировать QR-коды\n"
        f"• получать бонусные баллы\n"
        f"• смотреть историю начислений\n"
        f"• отслеживать свой уровень\n\n"
        f"Нажмите <b>«📷 Сканировать QR»</b> или <b>«💎 Мои баллы»</b>."
    )


@dp.message(CommandStart())
async def start_handler(message: Message, command: CommandStart):
    ensure_user_in_db(message.from_user)

    deep_arg = command.args
    if deep_arg and deep_arg.startswith("qr_"):
        code = normalize_code(deep_arg[3:])
        await process_qr_scan(message, code)
        return

    await message.answer(welcome_text(message.from_user), reply_markup=main_kb(message.from_user.id))


async def process_qr_scan(message: Message, code: str):
    ensure_user_in_db(message.from_user)

    qr_row = get_qr_by_code(code)
    if not qr_row:
        await message.answer("❌ Такой QR-код не найден.", reply_markup=main_kb(message.from_user.id))
        return

    if int(qr_row["is_active"]) != 1:
        await message.answer(
            f"⚠️ QR-код <b>{esc(qr_row['title'])}</b> отключён.",
            reply_markup=main_kb(message.from_user.id)
        )
        return

    if has_scan(message.from_user.id, qr_row["id"]):
        points = get_user_points(message.from_user.id)
        await message.answer(
            f"⚠️ Вы уже сканировали этот QR-код.\n\n"
            f"QR: <b>{esc(qr_row['title'])}</b>\n"
            f"Баланс: <b>{points}</b>\n"
            f"Уровень: <b>{get_level(points)}</b>",
            reply_markup=main_kb(message.from_user.id)
        )
        return

    register_scan(message.from_user.id, qr_row["id"])
    change_user_points(message.from_user.id, int(qr_row["points"]))

    new_points = get_user_points(message.from_user.id)
    level = get_level(new_points)

    await message.answer(
        f"✅ <b>Баллы начислены!</b>\n\n"
        f"QR: <b>{esc(qr_row['title'])}</b>\n"
        f"Начислено: <b>+{int(qr_row['points'])}</b>\n"
        f"Баланс: <b>{new_points}</b>\n"
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
                f"Пользователь: {esc(tg_label(message.from_user))}\n"
                f"User ID: <code>{message.from_user.id}</code>"
            )
        except Exception as e:
            logging.warning("Admin notify error: %s", e)


@dp.message(F.text == "📷 Сканировать QR")
async def scan_qr_enter(message: Message):
    user_states[message.from_user.id] = {"mode": "scan_qr"}
    await message.answer(
        "📷 <b>Сканирование QR</b>\n\n"
        "Отправьте код QR вручную одним сообщением.\n\n"
        "Например:\n"
        "<code>046201464492952IPVCNRAE0VAGJP</code>",
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text == "💎 Мои баллы")
async def my_points_handler(message: Message):
    ensure_user_in_db(message.from_user)
    points = get_user_points(message.from_user.id)
    await message.answer(
        f"💎 Ваш баланс: <b>{points}</b>\n🏆 Ваш уровень: <b>{get_level(points)}</b>",
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text == "🏆 Мой уровень")
async def my_level_handler(message: Message):
    ensure_user_in_db(message.from_user)
    points = get_user_points(message.from_user.id)
    await message.answer(
        f"🏆 Ваш уровень: <b>{get_level(points)}</b>\n"
        f"Баллы: <b>{points}</b>\n\n"
        f"Уровни:\n"
        f"• Silver: 0–299\n"
        f"• Gold: 300–999\n"
        f"• VIP: 1000+",
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text == "📜 История")
async def history_handler(message: Message):
    ensure_user_in_db(message.from_user)
    rows = get_user_history(message.from_user.id, 20)

    if not rows:
        await message.answer("📜 История пока пустая.", reply_markup=main_kb(message.from_user.id))
        return

    lines = ["📜 <b>Ваша история</b>\n"]
    for row in rows:
        lines.append(
            f"• <b>{esc(row['title'])}</b>\n"
            f"  Код: <code>{esc(row['code'])}</code>\n"
            f"  Баллы: +{int(row['points'])}\n"
            f"  Дата: {esc(row['scanned_at'])}\n"
        )

    await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text == "ℹ️ Как получить баллы")
async def how_to_get_handler(message: Message):
    await message.answer(
        "ℹ️ <b>Как получить баллы</b>\n\n"
        "1. Отсканируйте QR-код Lux Aristokrat\n"
        "2. Откроется бот или нажмите кнопку «📷 Сканировать QR»\n"
        "3. Баллы начислятся автоматически\n\n"
        "Один и тот же QR-код одному пользователю засчитывается только один раз.",
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text == "➕ Добавить QR")
@dp.message(Command("add_qr"))
async def add_qr_enter(message: Message):
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


@dp.message(F.text == "📥 Bulk QR")
@dp.message(Command("bulk_qr"))
async def bulk_qr_enter(message: Message):
    if not is_admin(message.from_user.id):
        return

    admin_states[message.from_user.id] = {"mode": "bulk_qr"}
    await message.answer(
        "📥 <b>Массовое добавление QR</b>\n\n"
        "Отправьте много строк одним сообщением.\n\n"
        "Форматы строк:\n"
        "<code>CODE</code>\n"
        "<code>CODE|Баллы</code>\n"
        "<code>CODE|Название|Баллы</code>\n\n"
        "Если указать только CODE — будет название <b>QR CODE</b> и 10 баллов.\n"
        "Если указать CODE|Баллы — название будет <b>QR CODE</b>."
    )


@dp.message(F.text == "📋 Список QR")
@dp.message(Command("list_qr"))
async def list_qr_handler(message: Message):
    if not is_admin(message.from_user.id):
        return

    rows = list_qr(50)
    if not rows:
        await message.answer("Список QR пуст.")
        return

    lines = ["📋 <b>Список QR</b>\n"]
    for row in rows:
        status = "✅" if int(row["is_active"]) == 1 else "⛔️"
        lines.append(
            f"{status} ID: <code>{row['id']}</code>\n"
            f"Название: <b>{esc(row['title'])}</b>\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Баллы: <b>{int(row['points'])}</b>\n"
        )

    await message.answer("\n".join(lines))


@dp.message(F.text == "⛔️ Отключить QR")
async def disable_qr_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "disable_qr"}
    await message.answer("Отправьте <b>ID QR</b>, который нужно отключить.")


@dp.message(F.text == "✅ Включить QR")
async def enable_qr_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "enable_qr"}
    await message.answer("Отправьте <b>ID QR</b>, который нужно включить.")


@dp.message(F.text == "🗑 Удалить QR")
async def delete_qr_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "delete_qr"}
    await message.answer("Отправьте <b>ID QR</b>, который нужно удалить.")


@dp.message(F.text == "🧾 Сделать QR PNG")
async def png_qr_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "make_png"}
    await message.answer("Отправьте <b>ID QR</b>, для которого нужно сделать PNG.")


@dp.message(F.text == "🎁 Начислить баллы")
async def add_points_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "add_points"}
    await message.answer("Отправьте:\n<code>user_id|баллы</code>")


@dp.message(F.text == "➖ Списать баллы")
async def remove_points_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "remove_points"}
    await message.answer("Отправьте:\n<code>user_id|баллы</code>")


@dp.message(F.text == "👤 Найти пользователя")
async def find_user_enter(message: Message):
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "find_user"}
    await message.answer("Отправьте <b>ID пользователя</b>.")


@dp.message(F.text == "📊 Статистика")
async def stats_handler(message: Message):
    if not is_admin(message.from_user.id):
        return

    top = get_top_users(5)
    lines = [
        "📊 <b>Статистика</b>\n",
        f"Пользователей: <b>{get_total_users()}</b>",
        f"QR-кодов: <b>{get_total_qr()}</b>",
        f"Сканов: <b>{get_total_scans()}</b>",
        "",
        "<b>Топ 5 пользователей:</b>"
    ]

    if top:
        for i, row in enumerate(top, start=1):
            name = row["full_name"] or row["username"] or row["user_id"]
            lines.append(f"{i}. {esc(name)} — <b>{int(row['points'])}</b> ({get_level(int(row['points']))})")
    else:
        lines.append("Пока нет данных.")

    await message.answer("\n".join(lines))


@dp.message(F.text == "📤 Экспорт users")
async def export_users_handler(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = export_users_csv()
    await message.answer_document(FSInputFile(path), caption="📤 Экспорт users готов.")


@dp.message(F.text == "📤 Экспорт scans")
async def export_scans_handler(message: Message):
    if not is_admin(message.from_user.id):
        return
    path = export_scans_csv()
    await message.answer_document(FSInputFile(path), caption="📤 Экспорт scans готов.")


@dp.message(F.text == "🥇 Топ пользователей")
async def top_users_handler(message: Message):
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
        lines.append(
            f"{i}. {esc(name)}\n"
            f"   ID: <code>{row['user_id']}</code>\n"
            f"   Баллы: <b>{points}</b>\n"
            f"   Уровень: <b>{get_level(points)}</b>\n"
        )

    await message.answer("\n".join(lines))


@dp.message(Command("cancel"))
async def cancel_handler(message: Message):
    admin_states.pop(message.from_user.id, None)
    user_states.pop(message.from_user.id, None)
    await message.answer("Отменено.", reply_markup=main_kb(message.from_user.id))


@dp.message(F.text)
async def text_router(message: Message):
    ensure_user_in_db(message.from_user)

    user_state = user_states.get(message.from_user.id)
    if user_state and user_state.get("mode") == "scan_qr":
        code = normalize_bulk_code(message.text or "")
        user_states.pop(message.from_user.id, None)

        if not code:
            await message.answer("❌ Пустой или некорректный QR-код.", reply_markup=main_kb(message.from_user.id))
            return

        await process_qr_scan(message, code)
        return

    if not is_admin(message.from_user.id):
        await message.answer("Выберите действие через меню.", reply_markup=main_kb(message.from_user.id))
        return

    state = admin_states.get(message.from_user.id)
    if not state:
        await message.answer("Выберите действие через меню.", reply_markup=main_kb(message.from_user.id))
        return

    mode = state.get("mode")
    text = (message.text or "").strip()

    if mode == "bulk_qr":
        raw_lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not raw_lines:
            await message.answer("❌ Пустой список.")
            return

        created = []
        skipped = []

        for idx, line in enumerate(raw_lines, start=1):
            try:
                parts = [p.strip() for p in line.split("|")]

                if len(parts) == 1:
                    raw_code = parts[0]
                    code = normalize_bulk_code(raw_code)
                    title = f"QR {code}"
                    points = 10
                elif len(parts) == 2:
                    raw_code, points_str = parts
                    code = normalize_bulk_code(raw_code)
                    title = f"QR {code}"
                    points = int(points_str)
                elif len(parts) == 3:
                    raw_code, title, points_str = parts
                    code = normalize_bulk_code(raw_code)
                    points = int(points_str)
                else:
                    skipped.append(f"{idx}. Неверный формат: {esc(line)}")
                    continue

                if not code:
                    skipped.append(f"{idx}. Пустой код после очистки: {esc(line)}")
                    continue

                if points <= 0:
                    skipped.append(f"{idx}. Баллы должны быть > 0: {esc(line)}")
                    continue

                create_qr(title=title, points=points, created_by=message.from_user.id, custom_code=code)
                created.append((code, title, points))

            except sqlite3.IntegrityError:
                skipped.append(f"{idx}. Уже существует: {esc(line)}")
            except ValueError:
                skipped.append(f"{idx}. Ошибка в баллах: {esc(line)}")
            except Exception as e:
                skipped.append(f"{idx}. Ошибка: {esc(str(e))}")

        admin_states.pop(message.from_user.id, None)

        lines = [
            "📥 <b>Bulk QR завершён</b>",
            "",
            f"✅ Создано: <b>{len(created)}</b>",
            f"⚠️ Пропущено: <b>{len(skipped)}</b>",
        ]

        if created:
            lines.append("")
            lines.append("<b>Созданные QR:</b>")
            for i, (code, title, points) in enumerate(created[:20], start=1):
                lines.append(f"{i}. <code>{esc(code)}</code> — {esc(title)} — <b>{points}</b>")
            if len(created) > 20:
                lines.append(f"... и ещё {len(created) - 20}")

        if skipped:
            lines.append("")
            lines.append("<b>Пропущенные строки:</b>")
            for item in skipped[:20]:
                lines.append(item)
            if len(skipped) > 20:
                lines.append(f"... и ещё {len(skipped) - 20}")

        await message.answer("\n".join(lines))
        return

    if mode == "add_qr":
        parts = [p.strip() for p in text.split("|")]
        try:
            if len(parts) == 2:
                title, points_str = parts
                code = create_qr(title=title, points=int(points_str), created_by=message.from_user.id)
            elif len(parts) == 3:
                custom_code, title, points_str = parts
                code = create_qr(title=title, points=int(points_str), created_by=message.from_user.id, custom_code=custom_code)
            else:
                await message.answer("❌ Неверный формат.")
                return

            row = get_qr_by_code(code)
            png = save_qr_png(code)
            admin_states.pop(message.from_user.id, None)

            await message.answer_photo(
                photo=FSInputFile(png),
                caption=(
                    f"✅ <b>QR создан</b>\n\n"
                    f"ID: <code>{row['id']}</code>\n"
                    f"Название: <b>{esc(row['title'])}</b>\n"
                    f"Код: <code>{esc(row['code'])}</code>\n"
                    f"Баллы: <b>{int(row['points'])}</b>\n"
                    f"Ссылка:\n<code>{esc(build_qr_link(code))}</code>"
                ),
                reply_markup=qr_link_kb(code)
            )
            return
        except sqlite3.IntegrityError:
            await message.answer("❌ Такой код уже существует.")
            return
        except ValueError:
            await message.answer("❌ Баллы должны быть числом больше 0.")
            return

    if mode == "disable_qr":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer("❌ QR не найден.")
            return
        set_qr_active(int(text), False)
        admin_states.pop(message.from_user.id, None)
        await message.answer(f"⛔️ QR <b>{esc(row['title'])}</b> отключён.")
        return

    if mode == "enable_qr":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer("❌ QR не найден.")
            return
        set_qr_active(int(text), True)
        admin_states.pop(message.from_user.id, None)
        await message.answer(f"✅ QR <b>{esc(row['title'])}</b> включён.")
        return

    if mode == "delete_qr":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer("❌ QR не найден.")
            return
        delete_qr(int(text))
        admin_states.pop(message.from_user.id, None)
        await message.answer(f"🗑 QR <b>{esc(row['title'])}</b> удалён.")
        return

    if mode == "make_png":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer("❌ QR не найден.")
            return
        png = save_qr_png(row["code"])
        admin_states.pop(message.from_user.id, None)
        await message.answer_photo(
            photo=FSInputFile(png),
            caption=(
                f"🧾 <b>QR PNG</b>\n\n"
                f"ID: <code>{row['id']}</code>\n"
                f"Название: <b>{esc(row['title'])}</b>\n"
                f"Код: <code>{esc(row['code'])}</code>\n"
                f"Баллы: <b>{int(row['points'])}</b>\n"
                f"Ссылка:\n<code>{esc(build_qr_link(row['code']))}</code>"
            ),
            reply_markup=qr_link_kb(row["code"])
        )
        return

    if mode == "add_points":
        parts = [p.strip() for p in text.split("|")]
        if len(parts) != 2 or not parts[0].isdigit():
            await message.answer("❌ Формат: <code>user_id|баллы</code>")
            return
        try:
            user_id = int(parts[0])
            points = int(parts[1])
            if points <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❌ Баллы должны быть числом больше 0.")
            return

        user_row = get_user_by_id(user_id)
        if not user_row:
            await message.answer("❌ Пользователь не найден.")
            return

        change_user_points(user_id, points)
        new_points = get_user_points(user_id)
        admin_states.pop(message.from_user.id, None)

        await message.answer(
            f"✅ Начислено <b>+{points}</b> пользователю <code>{user_id}</code>\n"
            f"Баланс: <b>{new_points}</b>\n"
            f"Уровень: <b>{get_level(new_points)}</b>"
        )
        try:
            await bot.send_message(
                user_id,
                f"🎁 Вам начислено <b>+{points}</b> баллов.\n"
                f"Баланс: <b>{new_points}</b>\n"
                f"Уровень: <b>{get_level(new_points)}</b>"
            )
        except Exception:
            pass
        return

    if mode == "remove_points":
        parts = [p.strip() for p in text.split("|")]
        if len(parts) != 2 or not parts[0].isdigit():
            await message.answer("❌ Формат: <code>user_id|баллы</code>")
            return
        try:
            user_id = int(parts[0])
            points = int(parts[1])
            if points <= 0:
                raise ValueError
        except ValueError:
            await message.answer("❌ Баллы должны быть числом больше 0.")
            return

        user_row = get_user_by_id(user_id)
        if not user_row:
            await message.answer("❌ Пользователь не найден.")
            return

        change_user_points(user_id, -points)
        new_points = get_user_points(user_id)
        admin_states.pop(message.from_user.id, None)

        await message.answer(
            f"➖ Списано <b>{points}</b> у пользователя <code>{user_id}</code>\n"
            f"Баланс: <b>{new_points}</b>\n"
            f"Уровень: <b>{get_level(new_points)}</b>"
        )
        try:
            await bot.send_message(
                user_id,
                f"➖ У вас списано <b>{points}</b> баллов.\n"
                f"Баланс: <b>{new_points}</b>\n"
                f"Уровень: <b>{get_level(new_points)}</b>"
            )
        except Exception:
            pass
        return

    if mode == "find_user":
        if not text.isdigit():
            await message.answer("❌ ID должен быть числом.")
            return

        user_row = get_user_by_id(int(text))
        if not user_row:
            await message.answer("❌ Пользователь не найден.")
            return

        history = get_user_history(int(text), 5)
        points = int(user_row["points"])
        lines = [
            "👤 <b>Пользователь</b>\n",
            f"ID: <code>{user_row['user_id']}</code>",
            f"Username: @{esc(user_row['username']) if user_row['username'] else '—'}",
            f"Имя: <b>{esc(user_row['full_name']) or '—'}</b>",
            f"Телефон: <b>{esc(user_row['phone']) or '—'}</b>",
            f"Баллы: <b>{points}</b>",
            f"Уровень: <b>{get_level(points)}</b>",
            "",
            "<b>Последние сканы:</b>"
        ]

        if history:
            for h in history:
                lines.append(f"• {esc(h['title'])} | +{int(h['points'])} | {esc(h['scanned_at'])}")
        else:
            lines.append("История пуста.")

        admin_states.pop(message.from_user.id, None)
        await message.answer("\n".join(lines))
        return

    await message.answer("Выберите действие через меню.", reply_markup=main_kb(message.from_user.id))


async def main():
    init_db()
    logging.info("Bot started successfully")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
