import os
import re
import json
import time
import html
import asyncio
import logging
import sqlite3
from contextlib import closing
from typing import Optional, Dict, Any, Tuple, List, Set

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    WebAppInfo,
    MenuButtonWebApp,
    MenuButtonDefault,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("API_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("BOT_API_TOKEN")
    or ""
).strip()

WEBAPP_URL = (os.getenv("WEBAPP_URL", "").strip() or "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/?v=1")
DB_PATH = (os.getenv("DB_PATH", "").strip() or "/app/data/lux_aristokrat.db")
QR_CODES_DIR = (os.getenv("QR_CODES_DIR", "").strip() or "/app/qr_codes")

DEFAULT_ADMIN_IDS = {6013591658, 6292063248}
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "0").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN / API_TOKEN не найден. Добавь переменную окружения BOT_TOKEN.")


def parse_admin_ids() -> List[int]:
    ids: Set[int] = set(DEFAULT_ADMIN_IDS)
    for value in [ADMIN_ID_RAW] + [p.strip() for p in ADMIN_IDS_RAW.split(",") if p.strip()]:
        try:
            number = int(value)
            if number > 0:
                ids.add(number)
        except Exception:
            pass
    return sorted(ids)


ADMIN_IDS = parse_admin_ids()

logger.info("WEBAPP_URL = %s", WEBAPP_URL)
logger.info("DB_PATH = %s", DB_PATH)
logger.info("QR_CODES_DIR = %s", QR_CODES_DIR)
logger.info("ADMIN_IDS effective = %s", ADMIN_IDS)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

ADMIN_STATES: Dict[int, Dict[str, Any]] = {}
RECENT_SCANS: Dict[str, float] = {}
SCAN_TTL_SECONDS = 6

BTN_SCAN = "📷 Сканировать QR"
BTN_MY_BALANCE = "💰 Мои баллы"
BTN_MY_STATUS = "🏅 Мой статус"
BTN_HELP = "ℹ️ Помощь"
BTN_CATALOG = "🛍 Каталог за баллы"
BTN_BUY_POINTS = "🛒 Купить за баллы"
BTN_ADMIN = "⚙️ Админ панель"
BTN_ADMIN_ADD = "➕ Добавить QR вручную"
BTN_ADMIN_IMPORT = "📥 Импорт QR из папки"
BTN_ADMIN_REMAIN = "📦 Остаток QR"
BTN_ADMIN_FIND_QR = "🔎 Проверить QR"
BTN_ADMIN_CLIENT = "👤 Проверить клиента"
BTN_ADMIN_SCANNER = "🧾 Кто сканировал QR"
BTN_ADMIN_STATS = "📊 Статистика"
BTN_ADMIN_UNUSED_LIST = "📋 Показать QR"
BTN_ADMIN_BACK = "⬅️ Главное меню"
BTN_CANCEL = "❌ Отмена"


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def is_admin(user_id: int) -> bool:
    return int(user_id) in ADMIN_IDS


def normalize_username(username: Optional[str]) -> str:
    return (username or "").strip().lstrip("@").lower()


def normalize_code(code: str) -> str:
    return (code or "").strip()


def get_status(balance: int) -> str:
    balance = int(balance or 0)
    if balance >= 100:
        return "Gold 🥇"
    if balance >= 50:
        return "Silver 🥈"
    return "Bronze 🥉"


def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_SCAN, web_app=WebAppInfo(url=WEBAPP_URL))],
        [KeyboardButton(text=BTN_MY_BALANCE), KeyboardButton(text=BTN_MY_STATUS)],
        [KeyboardButton(text=BTN_CATALOG), KeyboardButton(text=BTN_BUY_POINTS)],
        [KeyboardButton(text=BTN_HELP)],
    ]
    if is_admin(user_id):
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, one_time_keyboard=False, input_field_placeholder="Выберите кнопку меню")


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_ADMIN_ADD), KeyboardButton(text=BTN_ADMIN_IMPORT)],
            [KeyboardButton(text=BTN_ADMIN_REMAIN), KeyboardButton(text=BTN_ADMIN_FIND_QR)],
            [KeyboardButton(text=BTN_ADMIN_CLIENT), KeyboardButton(text=BTN_ADMIN_SCANNER)],
            [KeyboardButton(text=BTN_ADMIN_STATS), KeyboardButton(text=BTN_ADMIN_UNUSED_LIST)],
            [KeyboardButton(text=BTN_ADMIN_BACK)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Админ панель",
    )


def cancel_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True, one_time_keyboard=False)


def get_db_connection() -> sqlite3.Connection:
    folder = os.path.dirname(DB_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db() -> None:
    with closing(get_db_connection()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                language TEXT DEFAULT 'ru',
                balance INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS qr_codes (
                code TEXT PRIMARY KEY,
                points INTEGER NOT NULL DEFAULT 1,
                is_used INTEGER NOT NULL DEFAULT 0,
                used_by INTEGER,
                used_by_username TEXT,
                used_by_full_name TEXT,
                used_at INTEGER,
                created_by INTEGER,
                created_at INTEGER NOT NULL,
                source TEXT DEFAULT 'manual'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                points INTEGER NOT NULL DEFAULT 0,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                scanned_at INTEGER NOT NULL,
                result TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bonus_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                reason TEXT,
                admin_id INTEGER,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS catalog_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                points_price INTEGER NOT NULL,
                stock INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                product_id INTEGER NOT NULL,
                product_title TEXT NOT NULL,
                points_spent INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'new'
            )
            """
        )
        seed_catalog_products(conn)
        conn.commit()


def ensure_user(message: Message, language: str = "ru") -> None:
    user = message.from_user
    if not user:
        return
    full_name = " ".join([p for p in [(user.first_name or "").strip(), (user.last_name or "").strip()] if p]).strip()
    username = normalize_username(user.username)
    now = int(time.time())
    with closing(get_db_connection()) as conn:
        row = conn.execute("SELECT user_id FROM users WHERE user_id=?", (int(user.id),)).fetchone()
        if row:
            conn.execute(
                "UPDATE users SET username=?, full_name=?, language=?, updated_at=? WHERE user_id=?",
                (username, full_name, language, now, int(user.id)),
            )
        else:
            conn.execute(
                "INSERT INTO users(user_id, username, full_name, language, balance, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
                (int(user.id), username, full_name, language, 0, now, now),
            )
        conn.commit()



DEFAULT_PRODUCTS = [
    ("Уголь для кальяна", 50, 50),
    ("Мундштук персональный", 30, 100),
    ("Скидка 10% на покупку", 100, 30),
    ("Serbetli 50gr", 250, 20),
    ("Blackburn 100gr", 1180, 10),
    ("Must Have 100gr", 1350, 10),
]


def seed_catalog_products(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) c FROM catalog_products").fetchone()
    if row and int(row["c"] or 0) > 0:
        return
    now = int(time.time())
    for title, price, stock in DEFAULT_PRODUCTS:
        conn.execute(
            "INSERT INTO catalog_products(title, points_price, stock, is_active, created_at) VALUES(?,?,?,?,?)",
            (title, int(price), int(stock), 1, now),
        )


def get_catalog_products(active_only: bool = True) -> List[sqlite3.Row]:
    with closing(get_db_connection()) as conn:
        if active_only:
            return conn.execute("SELECT * FROM catalog_products WHERE is_active=1 ORDER BY id ASC").fetchall()
        return conn.execute("SELECT * FROM catalog_products ORDER BY id ASC").fetchall()


def catalog_text() -> str:
    products = get_catalog_products(True)
    if not products:
        return "🛍 Каталог пока пуст. Админ может добавить товары в таблицу catalog_products."
    lines = ["🛍 <b>Каталог товаров за баллы</b>", "", "Чтобы купить, отправьте команду: <code>/buy ID</code>", ""]
    for p in products:
        stock = int(p["stock"] or 0)
        stock_text = "есть" if stock > 0 else "нет в наличии"
        lines.append(f"<b>{p['id']}.</b> {esc(p['title'])} — <b>{p['points_price']}</b> баллов · {stock_text}")
    return "\n".join(lines)


def buy_product(user_id: int, product_id: int) -> Tuple[bool, str]:
    now = int(time.time())
    with closing(get_db_connection()) as conn:
        user = conn.execute("SELECT * FROM users WHERE user_id=?", (int(user_id),)).fetchone()
        if not user:
            return False, "❌ Клиент не найден. Сначала нажмите /start."
        product = conn.execute("SELECT * FROM catalog_products WHERE id=? AND is_active=1", (int(product_id),)).fetchone()
        if not product:
            return False, "❌ Товар не найден в каталоге."
        if int(product["stock"] or 0) <= 0:
            return False, "❌ Товар закончился."
        price = int(product["points_price"] or 0)
        balance = int(user["balance"] or 0)
        if balance < price:
            return False, f"❌ Недостаточно баллов. Нужно <b>{price}</b>, у вас <b>{balance}</b>."
        new_balance = balance - price
        conn.execute("UPDATE users SET balance=?, updated_at=? WHERE user_id=?", (new_balance, now, int(user_id)))
        conn.execute("UPDATE catalog_products SET stock=stock-1 WHERE id=?", (int(product_id),))
        conn.execute(
            "INSERT INTO redemptions(user_id, username, full_name, product_id, product_title, points_spent, created_at, status) VALUES(?,?,?,?,?,?,?,'new')",
            (int(user_id), user["username"], user["full_name"], int(product_id), product["title"], price, now),
        )
        conn.execute(
            "INSERT INTO bonus_transactions(user_id, amount, reason, admin_id, created_at) VALUES(?,?,?,?,?)",
            (int(user_id), -price, f"Покупка за баллы: {product['title']}", None, now),
        )
        conn.commit()
    return True, (
        "✅ Покупка оформлена!\n\n"
        f"Товар: <b>{esc(product['title'])}</b>\n"
        f"Списано: <b>{price}</b> баллов\n"
        f"Остаток баллов: <b>{new_balance}</b>\n\n"
        "Покажите это сообщение администратору для получения товара."
    )


def unused_qr_list(limit: int = 30) -> str:
    with closing(get_db_connection()) as conn:
        rows = conn.execute("SELECT code, points FROM qr_codes WHERE is_used=0 ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
        total = conn.execute("SELECT COUNT(*) c FROM qr_codes WHERE is_used=0").fetchone()["c"]
    if not rows:
        return "📋 Неотсканированных QR-кодов нет или QR-коды ещё не импортированы. Нажмите <b>📥 Импорт QR из папки</b>."
    lines = [f"📋 <b>Неотсканированные QR-коды</b>\nПоказано: <b>{len(rows)}</b> из <b>{total}</b>\n"]
    for r in rows:
        lines.append(f"<code>{esc(r['code'])}</code> — <b>{r['points']}</b> баллов")
    return "\n".join(lines)


def get_user_row(user_id: int):
    with closing(get_db_connection()) as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (int(user_id),)).fetchone()


def parse_qr_line(line: str) -> Optional[Tuple[str, int]]:
    line = line.strip().replace("\ufeff", "")
    if not line or line.startswith("#"):
        return None
    m = re.match(r"^(.*?)\s+(\d+)\s*(?:балл|балла|баллов|points|point)?\s*$", line, flags=re.I)
    if not m:
        return None
    code = normalize_code(m.group(1))
    points = int(m.group(2))
    if not code or points <= 0:
        return None
    return code, points


def import_qr_from_text(text: str, source: str, created_by: Optional[int] = None) -> Dict[str, int]:
    now = int(time.time())
    added = updated = skipped = total = 0
    with closing(get_db_connection()) as conn:
        for raw_line in text.splitlines():
            parsed = parse_qr_line(raw_line)
            if not parsed:
                continue
            total += 1
            code, points = parsed
            row = conn.execute("SELECT code, points, is_used FROM qr_codes WHERE code=?", (code,)).fetchone()
            if row:
                if int(row["is_used"] or 0) == 1:
                    skipped += 1
                    continue
                conn.execute(
                    "UPDATE qr_codes SET points=?, source=? WHERE code=?",
                    (points, source, code),
                )
                updated += 1
            else:
                conn.execute(
                    "INSERT INTO qr_codes(code, points, is_used, created_by, created_at, source) VALUES(?,?,?,?,?,?)",
                    (code, points, 0, created_by, now, source),
                )
                added += 1
        conn.commit()
    return {"total": total, "added": added, "updated": updated, "skipped": skipped}


def sync_qr_folder(created_by: Optional[int] = None) -> Dict[str, int]:
    os.makedirs(QR_CODES_DIR, exist_ok=True)
    result = {"files": 0, "total": 0, "added": 0, "updated": 0, "skipped": 0}
    for name in sorted(os.listdir(QR_CODES_DIR)):
        if not name.lower().endswith(".txt"):
            continue
        path = os.path.join(QR_CODES_DIR, name)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
        except UnicodeDecodeError:
            with open(path, "r", encoding="cp1251", errors="replace") as f:
                text = f.read()
        one = import_qr_from_text(text, source=f"file:{name}", created_by=created_by)
        result["files"] += 1
        for key in ["total", "added", "updated", "skipped"]:
            result[key] += one[key]
    logger.info("QR sync result: %s", result)
    return result


def add_qr_manual(code: str, points: int, admin_id: int) -> str:
    code = normalize_code(code)
    now = int(time.time())
    with closing(get_db_connection()) as conn:
        row = conn.execute("SELECT * FROM qr_codes WHERE code=?", (code,)).fetchone()
        if row:
            if int(row["is_used"] or 0) == 1:
                return "used"
            conn.execute("UPDATE qr_codes SET points=?, created_by=?, source='manual' WHERE code=?", (points, admin_id, code))
            conn.commit()
            return "updated"
        conn.execute(
            "INSERT INTO qr_codes(code, points, is_used, created_by, created_at, source) VALUES(?,?,?,?,?,'manual')",
            (code, int(points), 0, admin_id, now),
        )
        conn.commit()
        return "added"


def get_qr_stats() -> Dict[str, int]:
    with closing(get_db_connection()) as conn:
        total = conn.execute("SELECT COUNT(*) c FROM qr_codes").fetchone()["c"]
        unused = conn.execute("SELECT COUNT(*) c FROM qr_codes WHERE is_used=0").fetchone()["c"]
        used = conn.execute("SELECT COUNT(*) c FROM qr_codes WHERE is_used=1").fetchone()["c"]
        users = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        scans = conn.execute("SELECT COUNT(*) c FROM scans WHERE result='success'").fetchone()["c"]
        points = conn.execute("SELECT COALESCE(SUM(balance),0) s FROM users").fetchone()["s"]
    return {"total": total, "unused": unused, "used": used, "users": users, "scans": scans, "points": points}


def find_user_by_query(query: str):
    q = (query or "").strip()
    with closing(get_db_connection()) as conn:
        if q.lstrip("-").isdigit():
            return conn.execute("SELECT * FROM users WHERE user_id=?", (int(q),)).fetchone()
        username = normalize_username(q)
        return conn.execute("SELECT * FROM users WHERE lower(username)=?", (username,)).fetchone()


def find_qr(code: str):
    with closing(get_db_connection()) as conn:
        return conn.execute("SELECT * FROM qr_codes WHERE code=?", (normalize_code(code),)).fetchone()


def recent_scans_for_code(code: str) -> List[sqlite3.Row]:
    with closing(get_db_connection()) as conn:
        return conn.execute(
            "SELECT * FROM scans WHERE code=? ORDER BY scanned_at DESC LIMIT 10",
            (normalize_code(code),),
        ).fetchall()



def qr_info_text(code: str) -> str:
    row = find_qr(code)
    code = normalize_code(code)
    if not row:
        return f"❌ QR не найден:\n<code>{esc(code)}</code>"
    if int(row["is_used"] or 0) == 1:
        return (
            f"🧾 <b>QR отсканирован</b>\n\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Баллы: <b>{row['points']}</b>\n"
            f"Кем: <b>{esc(row['used_by_full_name'] or '—')}</b> @{esc(row['used_by_username'] or '—')}\n"
            f"Telegram ID: <code>{esc(row['used_by'] or '—')}</code>\n"
            f"Дата: <b>{esc(format_time(row['used_at']))}</b>"
        )
    return f"✅ <b>QR ещё не сканировали</b>\n\nКод: <code>{esc(row['code'])}</code>\nБаллы: <b>{row['points']}</b>"

def format_time(ts: Optional[int]) -> str:
    if not ts:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts)))


def duplicate_scan(user_id: int, code: str) -> bool:
    now = time.time()
    for key, ts in list(RECENT_SCANS.items()):
        if now - ts > SCAN_TTL_SECONDS:
            RECENT_SCANS.pop(key, None)
    key = f"{user_id}:{code}"
    if key in RECENT_SCANS:
        return True
    RECENT_SCANS[key] = now
    return False


def redeem_code(message: Message, code: str) -> Tuple[str, str]:
    user = message.from_user
    if not user:
        return "error", "Ошибка пользователя."
    code = normalize_code(code)
    if not code:
        return "error", "Пустой код."

    user_id = int(user.id)
    username = normalize_username(user.username)
    full_name = " ".join([p for p in [(user.first_name or "").strip(), (user.last_name or "").strip()] if p]).strip()
    now = int(time.time())

    with closing(get_db_connection()) as conn:
        row = conn.execute("SELECT * FROM qr_codes WHERE code=?", (code,)).fetchone()
        if not row:
            conn.execute(
                "INSERT INTO scans(code, points, user_id, username, full_name, scanned_at, result) VALUES(?,?,?,?,?,?,?)",
                (code, 0, user_id, username, full_name, now, "invalid"),
            )
            conn.commit()
            return "invalid", f"❌ QR-код не найден в базе.\n\nКод: <code>{esc(code)}</code>\n\nАдмин должен добавить этот код через админ-панель или /syncqr."

        if int(row["is_used"] or 0) == 1:
            conn.execute(
                "INSERT INTO scans(code, points, user_id, username, full_name, scanned_at, result) VALUES(?,?,?,?,?,?,?)",
                (code, 0, user_id, username, full_name, now, "used"),
            )
            conn.commit()
            return "used", (
                f"⚠️ Этот QR-код уже был использован.\n\n"
                f"Код: <code>{esc(code)}</code>\n"
                f"Кем: <b>{esc(row['used_by_full_name'] or '—')}</b> @{esc(row['used_by_username'] or '—')}\n"
                f"Telegram ID: <code>{esc(row['used_by'] or '—')}</code>\n"
                f"Дата: <b>{esc(format_time(row['used_at']))}</b>"
            )

        points = int(row["points"] or 0)
        user_row = conn.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not user_row:
            conn.execute(
                "INSERT INTO users(user_id, username, full_name, language, balance, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
                (user_id, username, full_name, "ru", 0, now, now),
            )
            old_balance = 0
        else:
            old_balance = int(user_row["balance"] or 0)
        new_balance = old_balance + points
        conn.execute("UPDATE users SET balance=?, username=?, full_name=?, updated_at=? WHERE user_id=?", (new_balance, username, full_name, now, user_id))
        conn.execute(
            "UPDATE qr_codes SET is_used=1, used_by=?, used_by_username=?, used_by_full_name=?, used_at=? WHERE code=?",
            (user_id, username, full_name, now, code),
        )
        conn.execute(
            "INSERT INTO scans(code, points, user_id, username, full_name, scanned_at, result) VALUES(?,?,?,?,?,?,?)",
            (code, points, user_id, username, full_name, now, "success"),
        )
        conn.execute(
            "INSERT INTO bonus_transactions(user_id, amount, reason, admin_id, created_at) VALUES(?,?,?,?,?)",
            (user_id, points, f"QR {code}", None, now),
        )
        conn.commit()

    return "success", (
        f"✅ Бонусы начислены!\n\n"
        f"Код: <code>{esc(code)}</code>\n"
        f"Начислено: <b>{points}</b> баллов\n"
        f"Ваш баланс: <b>{new_balance}</b>\n"
        f"Ваш статус: <b>{esc(get_status(new_balance))}</b>"
    )


async def send_balance(message: Message) -> None:
    ensure_user(message)
    row = get_user_row(message.from_user.id)
    balance = int(row["balance"] or 0) if row else 0
    await message.answer(
        f"💰 Ваши баллы: <b>{balance}</b>\n🏅 Ваш статус: <b>{esc(get_status(balance))}</b>",
        reply_markup=main_keyboard(message.from_user.id),
    )


async def send_admin_stats(message: Message) -> None:
    stats = get_qr_stats()
    await message.answer(
        "📊 <b>Статистика Lux Aristokrat</b>\n\n"
        f"Всего QR в базе: <b>{stats['total']}</b>\n"
        f"Не отсканировано: <b>{stats['unused']}</b>\n"
        f"Уже отсканировано: <b>{stats['used']}</b>\n"
        f"Клиентов в базе: <b>{stats['users']}</b>\n"
        f"Успешных сканирований: <b>{stats['scans']}</b>\n"
        f"Всего баллов у клиентов: <b>{stats['points']}</b>",
        reply_markup=admin_keyboard(),
    )


@dp.message(CommandStart())
async def start_handler(message: Message):
    ensure_user(message)
    await message.answer(
        "Добро пожаловать в <b>Lux Aristokrat</b>!\n\n"
        "Здесь можно сканировать QR/Data Matrix, смотреть баллы, статус клиента и покупать товары за накопленные баллы.",
        reply_markup=main_keyboard(message.from_user.id),
    )


@dp.message(Command("buttons"))
async def buttons_handler(message: Message):
    ensure_user(message)
    await message.answer("✅ Кнопки обновлены. Если их не видно — нажмите значок клавиатуры рядом с полем ввода.", reply_markup=main_keyboard(message.from_user.id))


@dp.message(Command("help"))
async def help_handler(message: Message):
    text = (
        "ℹ️ <b>Команды</b>\n\n"
        "/start — открыть меню\n"
        "/id — узнать Telegram ID\n"
        "/balance — мои баллы\n"
        "/status — мой статус\n\n"
        "<b>Админ:</b>\n"
        "/admin — админ панель\n"
        "/addqr КОД 10 — добавить QR вручную\n"
        "/syncqr — импортировать QR из папки\n"
        "/remain — остатки неотсканированных QR\n"
        "/qrinfo КОД — проверить QR и кем был отсканирован\n"
        "/client @username или ID — проверить баллы и статус клиента\n"
        "/stats — статистика\n"
        "/unusedqr — показать неотсканированные QR"
    )
    await message.answer(text, reply_markup=main_keyboard(message.from_user.id))


@dp.message(Command("id"))
async def id_handler(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>\nAdmin: <b>{is_admin(message.from_user.id)}</b>")


@dp.message(Command("balance"))
async def balance_cmd(message: Message):
    await send_balance(message)


@dp.message(Command("status"))
async def status_cmd(message: Message):
    await send_balance(message)


@dp.message(Command("catalog"))
async def catalog_cmd(message: Message):
    ensure_user(message)
    await message.answer(catalog_text(), reply_markup=main_keyboard(message.from_user.id))


@dp.message(Command("buy"))
async def buy_cmd(message: Message):
    ensure_user(message)
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer("Использование: <code>/buy ID</code>\n\n" + catalog_text(), reply_markup=main_keyboard(message.from_user.id))
        return
    ok, text = buy_product(message.from_user.id, int(parts[1].strip()))
    await message.answer(text, reply_markup=main_keyboard(message.from_user.id))


@dp.message(Command("unusedqr"))
async def unusedqr_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    await message.answer(unused_qr_list(40), reply_markup=admin_keyboard())


@dp.message(Command("admin"))
async def admin_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    await message.answer("⚙️ <b>Админ панель</b>\nВыберите действие:", reply_markup=admin_keyboard())


@dp.message(Command("syncqr"))
async def syncqr_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    result = sync_qr_folder(created_by=message.from_user.id)
    if result["files"] == 0:
        await message.answer(
            f"❌ В папке <code>{esc(QR_CODES_DIR)}</code> не найдено TXT файлов.\n\n"
            "Загрузи файл с QR-кодами в папку <b>qr_codes</b> в репозитории/хостинге и перезапусти или нажми импорт снова.",
            reply_markup=admin_keyboard(),
        )
        return
    await message.answer(
        "✅ Импорт QR завершён.\n\n"
        f"Папка: <code>{esc(QR_CODES_DIR)}</code>\n"
        f"Файлов: <b>{result['files']}</b>\n"
        f"Строк с QR: <b>{result['total']}</b>\n"
        f"Добавлено: <b>{result['added']}</b>\n"
        f"Обновлено: <b>{result['updated']}</b>\n"
        f"Пропущено использованных: <b>{result['skipped']}</b>",
        reply_markup=admin_keyboard(),
    )


@dp.message(Command("addqr"))
async def addqr_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: <code>/addqr КОД 10</code>\nПример: <code>/addqr 086817670173442F9mUnO 10</code>")
        return
    tail = parts[1] + " " + parts[2]
    parsed = parse_qr_line(tail)
    if not parsed:
        await message.answer("❌ Неверный формат. Нужно: <code>/addqr КОД 10</code>")
        return
    code, points = parsed
    res = add_qr_manual(code, points, message.from_user.id)
    await message.answer(f"✅ QR {res}:\n<code>{esc(code)}</code>\nБаллы: <b>{points}</b>", reply_markup=admin_keyboard())


@dp.message(Command("remain"))
async def remain_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    stats = get_qr_stats()
    await message.answer(
        f"📦 Остаток неотсканированных QR: <b>{stats['unused']}</b>\n"
        f"Всего QR: <b>{stats['total']}</b>\n"
        f"Уже использовано: <b>{stats['used']}</b>",
        reply_markup=admin_keyboard(),
    )


@dp.message(Command("stats"))
async def stats_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    await send_admin_stats(message)


@dp.message(Command("client"))
async def client_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/client @username</code> или <code>/client 123456789</code>")
        return
    row = find_user_by_query(parts[1])
    if not row:
        await message.answer("❌ Клиент не найден. Клиент должен хотя бы один раз открыть бота.", reply_markup=admin_keyboard())
        return
    balance = int(row["balance"] or 0)
    await message.answer(
        "👤 <b>Клиент</b>\n\n"
        f"Имя: <b>{esc(row['full_name'] or '—')}</b>\n"
        f"Username: @{esc(row['username'] or '—')}\n"
        f"Telegram ID: <code>{esc(row['user_id'])}</code>\n"
        f"Баллы: <b>{balance}</b>\n"
        f"Статус: <b>{esc(get_status(balance))}</b>",
        reply_markup=admin_keyboard(),
    )


@dp.message(Command("qrinfo"))
async def qrinfo_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/qrinfo КОД</code>")
        return
    code = normalize_code(parts[1])
    await message.answer(qr_info_text(code), reply_markup=admin_keyboard())


@dp.message(F.web_app_data)
async def webapp_handler(message: Message):
    ensure_user(message)
    raw = message.web_app_data.data if message.web_app_data else ""
    try:
        data = json.loads(raw)
    except Exception:
        await message.answer("❌ Ошибка чтения данных от сканера.", reply_markup=main_keyboard(message.from_user.id))
        return
    action = data.get("action")
    code = normalize_code(data.get("code", ""))
    if action != "scan_qr":
        await message.answer("❌ Неизвестное действие сканера.", reply_markup=main_keyboard(message.from_user.id))
        return
    if duplicate_scan(message.from_user.id, code):
        return
    _, text = redeem_code(message, code)
    await message.answer(text, reply_markup=main_keyboard(message.from_user.id))


@dp.message(F.text == BTN_MY_BALANCE)
async def btn_balance(message: Message):
    await send_balance(message)


@dp.message(F.text == BTN_MY_STATUS)
async def btn_status(message: Message):
    await send_balance(message)


@dp.message(F.text == BTN_CATALOG)
async def btn_catalog(message: Message):
    await catalog_cmd(message)


@dp.message(F.text == BTN_BUY_POINTS)
async def btn_buy_points(message: Message):
    await catalog_cmd(message)


@dp.message(F.text == BTN_HELP)
async def btn_help(message: Message):
    await help_handler(message)


@dp.message(F.text == BTN_ADMIN)
async def btn_admin(message: Message):
    await admin_cmd(message)


@dp.message(F.text == BTN_ADMIN_BACK)
async def btn_back(message: Message):
    await message.answer("Главное меню", reply_markup=main_keyboard(message.from_user.id))


@dp.message(F.text == BTN_ADMIN_IMPORT)
async def btn_import(message: Message):
    await syncqr_cmd(message)


@dp.message(F.text == BTN_ADMIN_REMAIN)
async def btn_remain(message: Message):
    await remain_cmd(message)


@dp.message(F.text == BTN_ADMIN_UNUSED_LIST)
async def btn_unused_list(message: Message):
    await unusedqr_cmd(message)


@dp.message(F.text == BTN_ADMIN_STATS)
async def btn_stats(message: Message):
    await stats_cmd(message)


@dp.message(F.text == BTN_ADMIN_ADD)
async def btn_admin_add(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    ADMIN_STATES[message.from_user.id] = {"mode": "add_qr"}
    await message.answer(
        "➕ Отправьте QR и баллы в формате:\n\n<code>086817670173442F9mUnO 10</code>",
        reply_markup=cancel_keyboard(),
    )


@dp.message(F.text == BTN_ADMIN_FIND_QR)
async def btn_admin_find_qr(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    ADMIN_STATES[message.from_user.id] = {"mode": "find_qr"}
    await message.answer("🔎 Отправьте QR-код для проверки:", reply_markup=cancel_keyboard())


@dp.message(F.text == BTN_ADMIN_SCANNER)
async def btn_admin_scanner(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    ADMIN_STATES[message.from_user.id] = {"mode": "find_qr"}
    await message.answer("🧾 Отправьте QR-код — покажу кем он был отсканирован:", reply_markup=cancel_keyboard())


@dp.message(F.text == BTN_ADMIN_CLIENT)
async def btn_admin_client(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Доступ только для администратора.")
        return
    ADMIN_STATES[message.from_user.id] = {"mode": "client"}
    await message.answer("👤 Отправьте username клиента или Telegram ID:\nНапример: <code>@username</code>", reply_markup=cancel_keyboard())


@dp.message(F.text == BTN_CANCEL)
async def btn_cancel(message: Message):
    ADMIN_STATES.pop(message.from_user.id, None)
    await message.answer("Отменено.", reply_markup=admin_keyboard() if is_admin(message.from_user.id) else main_keyboard(message.from_user.id))


@dp.message(F.text)
async def text_handler(message: Message):
    ensure_user(message)
    user_id = message.from_user.id
    state = ADMIN_STATES.get(user_id)
    text = (message.text or "").strip()

    if state and is_admin(user_id):
        mode = state.get("mode")
        if mode == "add_qr":
            parsed = parse_qr_line(text)
            if not parsed:
                await message.answer("❌ Неверный формат. Отправьте так:\n<code>КОД 10</code>", reply_markup=cancel_keyboard())
                return
            code, points = parsed
            res = add_qr_manual(code, points, user_id)
            ADMIN_STATES.pop(user_id, None)
            await message.answer(f"✅ QR {res}:\n<code>{esc(code)}</code>\nБаллы: <b>{points}</b>", reply_markup=admin_keyboard())
            return

        if mode == "find_qr":
            ADMIN_STATES.pop(user_id, None)
            await message.answer(qr_info_text(text), reply_markup=admin_keyboard())
            return

        if mode == "client":
            ADMIN_STATES.pop(user_id, None)
            row = find_user_by_query(text)
            if not row:
                await message.answer("❌ Клиент не найден. Клиент должен хотя бы один раз открыть бота.", reply_markup=admin_keyboard())
                return
            balance = int(row["balance"] or 0)
            await message.answer(
                "👤 <b>Клиент</b>\n\n"
                f"Имя: <b>{esc(row['full_name'] or '—')}</b>\n"
                f"Username: @{esc(row['username'] or '—')}\n"
                f"Telegram ID: <code>{esc(row['user_id'])}</code>\n"
                f"Баллы: <b>{balance}</b>\n"
                f"Статус: <b>{esc(get_status(balance))}</b>",
                reply_markup=admin_keyboard(),
            )
            return

    await message.answer(
        "Выберите действие кнопкой меню. Для сканирования нажмите <b>📷 Сканировать QR</b>.",
        reply_markup=main_keyboard(user_id),
    )


async def set_menu_button():
    # Отключаем синюю кнопку Telegram Menu Button.
    # Все действия теперь находятся в обычных кнопках бота под полем ввода.
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonDefault())
        logger.info("Telegram blue menu button disabled")
    except Exception as e:
        logger.warning("Cannot disable menu button: %s", e)


async def on_startup():
    init_db()
    os.makedirs(QR_CODES_DIR, exist_ok=True)
    # Автоматически подгружаем TXT QR-коды при запуске, чтобы бот не показывал пустую базу.
    try:
        sync_result = sync_qr_folder(created_by=None)
        logger.info("Auto QR sync on startup: %s", sync_result)
    except Exception as e:
        logger.exception("Auto QR sync failed: %s", e)
    await set_menu_button()
    logger.info("Bot started successfully")


async def main():
    await on_startup()
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
