import os
import re
import time
import html
import sqlite3
import logging
from contextlib import closing
from typing import Optional, Tuple, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    WebAppInfo,
    MenuButtonDefault,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("lux_aristokrat")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("API_TOKEN") or "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN / API_TOKEN не найден")

WEBAPP_URL = os.getenv("WEBAPP_URL", "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/?v=1").strip()
DB_PATH = os.getenv("DB_PATH", "/app/data/lux_aristokrat.db").strip()
QR_CODES_DIR = os.getenv("QR_CODES_DIR", "/app/qr_codes").strip()

DEFAULT_ADMIN_IDS = {6013591658, 6292063248}
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()

def parse_admins():
    admins = set(DEFAULT_ADMIN_IDS)
    for v in [ADMIN_ID_RAW]:
        try:
            if v and int(v) > 0:
                admins.add(int(v))
        except Exception:
            pass
    for p in ADMIN_IDS_RAW.split(","):
        try:
            if p.strip() and int(p.strip()) > 0:
                admins.add(int(p.strip()))
        except Exception:
            pass
    return admins

ADMIN_IDS = parse_admins()
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
pending: Dict[int, str] = {}

def esc(v): return html.escape(str(v or ""))
def is_admin(uid): return int(uid) in ADMIN_IDS

def ensure_dirs():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(QR_CODES_DIR, exist_ok=True)

def db():
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(db()) as conn:
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
            points INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS qr_codes(
            code TEXT PRIMARY KEY, points INTEGER NOT NULL DEFAULT 0, used INTEGER NOT NULL DEFAULT 0,
            used_by INTEGER, used_by_username TEXT, used_by_full_name TEXT, used_at INTEGER,
            created_by INTEGER, created_at INTEGER NOT NULL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS scans(
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, points INTEGER, user_id INTEGER,
            username TEXT, full_name TEXT, scanned_at INTEGER)""")
        c.execute("""CREATE TABLE IF NOT EXISTS catalog(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price_points INTEGER, active INTEGER DEFAULT 1, created_at INTEGER)""")
        c.execute("SELECT COUNT(*) c FROM catalog")
        if int(c.fetchone()["c"]) == 0:
            now = int(time.time())
            for name, price in [("Скидка 10 000 сум",50),("Скидка 25 000 сум",120),("Подарок от Lux Aristokrat",250),("VIP скидка",500)]:
                c.execute("INSERT INTO catalog(name,price_points,active,created_at) VALUES(?,?,1,?)",(name,price,now))
        conn.commit()

def username(m):
    return (m.from_user.username or "").strip().lstrip("@").lower() if m.from_user else ""

def fullname(m):
    if not m.from_user: return ""
    return " ".join([x for x in [(m.from_user.first_name or "").strip(), (m.from_user.last_name or "").strip()] if x])

def save_user(m):
    if not m.from_user: return
    uid = int(m.from_user.id); now = int(time.time())
    with closing(db()) as conn:
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE user_id=?", (uid,))
        if c.fetchone():
            c.execute("UPDATE users SET username=?, full_name=?, updated_at=? WHERE user_id=?", (username(m), fullname(m), now, uid))
        else:
            c.execute("INSERT INTO users(user_id,username,full_name,points,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                      (uid, username(m), fullname(m), 0, now, now))
        conn.commit()

def points(uid):
    with closing(db()) as conn:
        c = conn.cursor(); c.execute("SELECT points FROM users WHERE user_id=?", (uid,))
        r = c.fetchone(); return int(r["points"]) if r else 0

def status(p):
    p=int(p or 0)
    if p >= 500: return "Gold 🟡"
    if p >= 150: return "Silver ⚪"
    return "Bronze 🟤"

def main_keyboard(admin=False):
    rows = [
        [KeyboardButton(text="📷 Сканер QR", web_app=WebAppInfo(url=WEBAPP_URL))],
        [KeyboardButton(text="💰 Мои баллы"), KeyboardButton(text="🎁 Каталог")],
        [KeyboardButton(text="🏆 Мой статус")]
    ]
    if admin:
        rows.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить QR вручную"), KeyboardButton(text="📥 Импорт QR из папки")],
        [KeyboardButton(text="📦 Остаток QR"), KeyboardButton(text="🔎 Проверить QR")],
        [KeyboardButton(text="👤 Проверить клиента"), KeyboardButton(text="🧾 Кто сканировал QR")],
        [KeyboardButton(text="📋 Показать QR"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="◀️ Главное меню")]
    ], resize_keyboard=True)

def parse_code_points(text):
    m = re.match(r"^(.+?)\s+(\d+)\s*(?:балл|балла|баллов|points|point)?\s*$", (text or "").strip(), re.I|re.S)
    if not m: return None
    code = m.group(1).replace("\n","").replace("\r","").strip()
    pts = int(m.group(2))
    return (code, pts) if code and pts > 0 else None

def add_qr(code, pts, by=None):
    with closing(db()) as conn:
        c=conn.cursor()
        c.execute("SELECT code FROM qr_codes WHERE code=?", (code,))
        if c.fetchone(): return False
        c.execute("INSERT INTO qr_codes(code,points,used,created_by,created_at) VALUES(?,?,?,?,?)",
                  (code, pts, 0, by, int(time.time())))
        conn.commit()
        return True

def import_folder():
    os.makedirs(QR_CODES_DIR, exist_ok=True)
    added=exists=skipped=files=0
    for fn in sorted(os.listdir(QR_CODES_DIR)):
        if not fn.lower().endswith(".txt"): continue
        files += 1
        path=os.path.join(QR_CODES_DIR,fn)
        try:
            raw=open(path,"r",encoding="utf-8").read()
        except UnicodeDecodeError:
            raw=open(path,"r",encoding="cp1251",errors="ignore").read()
        for line in raw.splitlines():
            line=line.strip()
            if not line or line.startswith("#"): continue
            p=parse_code_points(line)
            if not p:
                skipped += 1; continue
            if add_qr(p[0],p[1],None): added += 1
            else: exists += 1
    return added, exists, skipped, files

def qr_stats():
    with closing(db()) as conn:
        c=conn.cursor()
        c.execute("SELECT COUNT(*) c FROM qr_codes"); total=int(c.fetchone()["c"])
        c.execute("SELECT COUNT(*) c FROM qr_codes WHERE used=0"); unused=int(c.fetchone()["c"])
        c.execute("SELECT COUNT(*) c FROM qr_codes WHERE used=1"); used=int(c.fetchone()["c"])
        return total, unused, used

@dp.message(CommandStart())
async def start(m: Message):
    save_user(m); uid=int(m.from_user.id) if m.from_user else 0
    await m.answer("Добро пожаловать в <b>Lux Aristokrat</b>!", reply_markup=main_keyboard(is_admin(uid)))

@dp.message(Command("remove_menu"))
async def remove_menu(m: Message):
    uid=int(m.from_user.id) if m.from_user else 0
    if not is_admin(uid):
        await m.answer("❌ Только для администратора."); return
    await bot.set_chat_menu_button(menu_button=MenuButtonDefault())
    await m.answer("✅ Синяя кнопка отключена. Закройте Telegram и откройте чат заново.")

@dp.message(F.text=="⚙️ Админ панель")
async def admin(m):
    uid=int(m.from_user.id) if m.from_user else 0
    if not is_admin(uid): await m.answer("❌ Только для администратора."); return
    await m.answer("⚙️ Админ панель", reply_markup=admin_keyboard())

@dp.message(F.text=="➕ Добавить QR вручную")
async def add_wait(m):
    uid=int(m.from_user.id) if m.from_user else 0
    if not is_admin(uid): await m.answer("❌ Только для администратора."); return
    pending[uid]="add_qr"
    await m.answer("Отправьте так:\n<code>086817670173442n)+&gt;An 10</code>")

@dp.message(F.text=="📥 Импорт QR из папки")
async def imp(m):
    uid=int(m.from_user.id) if m.from_user else 0
    if not is_admin(uid): await m.answer("❌ Только для администратора."); return
    a,e,s,f=import_folder()
    await m.answer(f"📥 Импорт завершён\nФайлов: <b>{f}</b>\nДобавлено: <b>{a}</b>\nУже были: <b>{e}</b>\nПропущено: <b>{s}</b>")

@dp.message(F.text=="📦 Остаток QR")
async def left(m):
    uid=int(m.from_user.id) if m.from_user else 0
    if not is_admin(uid): await m.answer("❌ Только для администратора."); return
    t,u,used=qr_stats()
    await m.answer(f"Всего: <b>{t}</b>\nОстаток: <b>{u}</b>\nИспользовано: <b>{used}</b>")

@dp.message(F.text=="💰 Мои баллы")
async def mybal(m):
    save_user(m); p=points(int(m.from_user.id))
    await m.answer(f"💰 Баллы: <b>{p}</b>\nСтатус: <b>{status(p)}</b>")

@dp.message(F.text=="🏆 Мой статус")
async def myst(m):
    save_user(m); p=points(int(m.from_user.id))
    await m.answer(f"🏆 Статус: <b>{status(p)}</b>\nБаллы: <b>{p}</b>")

@dp.message(F.text=="◀️ Главное меню")
async def home(m):
    uid=int(m.from_user.id) if m.from_user else 0
    pending.pop(uid,None)
    await m.answer("Главное меню", reply_markup=main_keyboard(is_admin(uid)))

@dp.message(F.text)
async def text(m):
    save_user(m); uid=int(m.from_user.id) if m.from_user else 0
    action=pending.get(uid)
    if action=="add_qr":
        p=parse_code_points(m.text)
        if not p:
            await m.answer("❌ Формат неверный. Отправьте так:\n<code>КОД 10</code>")
            return
        pending.pop(uid,None)
        ok=add_qr(p[0],p[1],uid)
        await m.answer(("✅ QR добавлен" if ok else "⚠️ QR уже есть") + f"\n<code>{esc(p[0])}</code>\nБаллы: <b>{p[1]}</b>", reply_markup=admin_keyboard())
        return
    await m.answer("Выберите действие кнопками ниже.", reply_markup=main_keyboard(is_admin(uid)))

async def on_startup():
    init_db()
    await bot.set_chat_menu_button(menu_button=MenuButtonDefault())
    log.info("Blue Telegram menu button disabled")

async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
