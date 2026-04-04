# === LUX FINAL STABLE FULL FIX ===

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, MenuButtonWebApp

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://your-site.com/?v=2")
ADMIN_IDS = {6013591658}  # ← ВСТАВЬ СВОЙ ID

DB_FILE = "lux.db"

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


# =========================
# 🔐 ADMIN CHECK
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# =========================
# 💾 DB
# =========================
def get_conn():
    conn = sqlite3.connect(DB_FILE, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=60000;")

    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        points INTEGER DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS qr_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        points INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scans (
        user_id INTEGER,
        code TEXT,
        UNIQUE(user_id, code)
    )
    """)

    conn.commit()
    conn.close()


# =========================
# 📊 LOGIC
# =========================
def add_points(user_id, code):
    conn = get_conn()
    cur = conn.cursor()

    qr = cur.execute("SELECT * FROM qr_codes WHERE code=?", (code,)).fetchone()
    if not qr:
        return "❌ Код не найден"

    exists = cur.execute(
        "SELECT 1 FROM scans WHERE user_id=? AND code=?",
        (user_id, code)
    ).fetchone()

    if exists:
        return "⚠️ Уже использован"

    cur.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    cur.execute("INSERT INTO scans VALUES (?,?)", (user_id, code))

    cur.execute(
        "UPDATE users SET points = points + ? WHERE user_id=?",
        (qr["points"], user_id)
    )

    conn.commit()

    points = cur.execute(
        "SELECT points FROM users WHERE user_id=?",
        (user_id,)
    ).fetchone()["points"]

    conn.close()

    return f"✅ +{qr['points']} баллов\nБаланс: {points}"


# =========================
# 🍔 MENU
# =========================
def menu(user_id):
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📷 Сканировать QR", web_app=WebAppInfo(url=WEBAPP_URL))],
            [KeyboardButton(text="💎 Баллы")]
        ],
        resize_keyboard=True
    )


# =========================
# 🚀 START
# =========================
@dp.message(CommandStart())
async def start(message: Message):
    await bot.set_chat_menu_button(
        chat_id=message.chat.id,
        menu_button=MenuButtonWebApp(
            text="📷 Сканер",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )
    )

    await message.answer("Добро пожаловать", reply_markup=menu(message.from_user.id))


# =========================
# 📷 QR FROM WEBAPP
# =========================
@dp.message(F.web_app_data)
async def webapp(message: Message):
    data = json.loads(message.web_app_data.data)

    if data["action"] == "scan_qr":
        result = add_points(message.from_user.id, data["code"])
        await message.answer(result)


# =========================
# 💎 BALANCE
# =========================
@dp.message(F.text == "💎 Баллы")
async def points(message: Message):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (message.from_user.id,))
    row = cur.execute("SELECT points FROM users WHERE user_id=?", (message.from_user.id,)).fetchone()

    conn.close()

    await message.answer(f"💎 Баллы: {row['points']}")


# =========================
# 🔥 RUN
# =========================
async def main():
    init_db()

    await bot.set_chat_menu_button(
        menu_button=MenuButtonWebApp(
            text="📷 Сканер",
            web_app=WebAppInfo(url=WEBAPP_URL)
        )
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
