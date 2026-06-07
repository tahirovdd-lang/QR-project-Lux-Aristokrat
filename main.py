import os
import re
import time
import html
import json
import sqlite3
import logging
from contextlib import closing
from typing import Dict, Any, Optional, Tuple

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

# =========================
# Lux Aristokrat QR Bot
# Файл: main.py
# =========================

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
log = logging.getLogger("lux_aristokrat")

BOT_TOKEN = (
    os.getenv("BOT_TOKEN")
    or os.getenv("API_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or ""
).strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN / API_TOKEN не найден")

WEBAPP_URL = (
    os.getenv("WEBAPP_URL", "").strip()
    or "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/?v=1"
)

DB_PATH = os.getenv("DB_PATH", "/app/data/lux_aristokrat.db").strip() or "/app/data/lux_aristokrat.db"
QR_CODES_DIR = os.getenv("QR_CODES_DIR", "/app/qr_codes").strip() or "/app/qr_codes"

DEFAULT_ADMIN_IDS = {6013591658, 6292063248}
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()


def parse_admin_ids() -> set[int]:
    result = set(DEFAULT_ADMIN_IDS)

    try:
        if ADMIN_ID_RAW and int(ADMIN_ID_RAW) > 0:
            result.add(int(ADMIN_ID_RAW))
    except Exception:
        pass

    for part in ADMIN_IDS_RAW.split(","):
        part = part.strip()
        try:
            if part and int(part) > 0:
                result.add(int(part))
        except Exception:
            pass

    return result


ADMIN_IDS = parse_admin_ids()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# pending[user_id] = action
pending: Dict[int, str] = {}
recent_scans: Dict[str, float] = {}
RECENT_TTL = 8


# =========================
# DATABASE
# =========================

def ensure_dirs():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    os.makedirs(QR_CODES_DIR, exist_ok=True)


def db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            points INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS qr_codes (
            code TEXT PRIMARY KEY,
            points INTEGER NOT NULL DEFAULT 0,
            used INTEGER NOT NULL DEFAULT 0,
            used_by INTEGER,
            used_by_username TEXT,
            used_by_full_name TEXT,
            used_at INTEGER,
            created_by INTEGER,
            created_at INTEGER NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            points INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            scanned_at INTEGER NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT,
            item_name TEXT NOT NULL,
            price_points INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            price_points INTEGER NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        )
        """)

        cur.execute("SELECT COUNT(*) AS c FROM catalog")
        if int(cur.fetchone()["c"]) == 0:
            now = int(time.time())
            for name, price in [
                ("Скидка 10 000 сум", 50),
                ("Скидка 25 000 сум", 120),
                ("Подарок от Lux Aristokrat", 250),
                ("VIP скидка", 500),
            ]:
                cur.execute(
                    "INSERT INTO catalog(name, price_points, active, created_at) VALUES(?,?,1,?)",
                    (name, price, now)
                )

        # ── Миграция: добавляем недостающие колонки в старые базы ──
        def ensure_column(table: str, column: str, decl: str):
            cur.execute(f"PRAGMA table_info({table})")
            existing = {row["name"] for row in cur.fetchall()}
            if column not in existing:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
                log.info("Migration: added column %s.%s", table, column)

        ensure_column("qr_codes", "points", "INTEGER NOT NULL DEFAULT 0")
        ensure_column("qr_codes", "used", "INTEGER NOT NULL DEFAULT 0")
        ensure_column("qr_codes", "used_by", "INTEGER")
        ensure_column("qr_codes", "used_by_username", "TEXT")
        ensure_column("qr_codes", "used_by_full_name", "TEXT")
        ensure_column("qr_codes", "used_at", "INTEGER")
        ensure_column("qr_codes", "created_by", "INTEGER")

        ensure_column("users", "username", "TEXT")
        ensure_column("users", "full_name", "TEXT")
        ensure_column("users", "points", "INTEGER NOT NULL DEFAULT 0")

        # ── Чиним таблицу scans, если в ней есть лишние NOT NULL колонки (напр. result) ──
        cur.execute("PRAGMA table_info(scans)")
        scans_cols = cur.fetchall()
        bad_notnull = [
            r["name"] for r in scans_cols
            if int(r["notnull"]) == 1 and r["dflt_value"] is None
            and r["name"] not in ("id", "code", "points", "user_id", "scanned_at")
        ]
        if bad_notnull:
            log.info("Migration: rebuilding scans table, dropping bad NOT NULL columns: %s", bad_notnull)
            cur.execute("ALTER TABLE scans RENAME TO scans_old")
            cur.execute("""
            CREATE TABLE scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                points INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT,
                scanned_at INTEGER NOT NULL
            )
            """)
            try:
                cur.execute("""
                    INSERT INTO scans(id, code, points, user_id, username, full_name, scanned_at)
                    SELECT id, code, points, user_id, username, full_name, scanned_at FROM scans_old
                """)
            except Exception as e:
                log.warning("Could not copy old scans rows: %s", e)
            cur.execute("DROP TABLE scans_old")

        conn.commit()


# =========================
# HELPERS
# =========================

def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def now_ts() -> int:
    return int(time.time())


def is_admin(user_id: int) -> bool:
    return int(user_id) in ADMIN_IDS


def username_from_message(message: Message) -> str:
    if not message.from_user or not message.from_user.username:
        return ""
    return message.from_user.username.strip().lstrip("@").lower()


def full_name_from_message(message: Message) -> str:
    if not message.from_user:
        return ""
    return " ".join(
        p for p in [
            (message.from_user.first_name or "").strip(),
            (message.from_user.last_name or "").strip(),
        ] if p
    ).strip()


def save_user(message: Message):
    if not message.from_user:
        return

    user_id = int(message.from_user.id)
    username = username_from_message(message)
    full_name = full_name_from_message(message)
    now = now_ts()

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (user_id,))
        if cur.fetchone():
            cur.execute(
                "UPDATE users SET username=?, full_name=?, updated_at=? WHERE user_id=?",
                (username, full_name, now, user_id)
            )
        else:
            cur.execute(
                "INSERT INTO users(user_id, username, full_name, points, created_at, updated_at) VALUES(?,?,?,?,?,?)",
                (user_id, username, full_name, 0, now, now)
            )
        conn.commit()


def get_points(user_id: int) -> int:
    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT points FROM users WHERE user_id=?", (int(user_id),))
        row = cur.fetchone()
        return int(row["points"]) if row else 0


def client_status(points: int) -> str:
    points = int(points or 0)
    if points >= 500:
        return "Gold 🟡"
    if points >= 150:
        return "Silver ⚪"
    return "Bronze 🟤"


def main_keyboard(admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📷 Сканер QR", web_app=WebAppInfo(url=WEBAPP_URL))],
        [KeyboardButton(text="💰 Мои баллы"), KeyboardButton(text="🎁 Каталог")],
        [KeyboardButton(text="🏆 Мой статус")],
    ]
    if admin:
        rows.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить QR вручную"), KeyboardButton(text="📥 Импорт QR из папки")],
            [KeyboardButton(text="🗑 Очистить базу QR"), KeyboardButton(text="📦 Остаток QR")],
            [KeyboardButton(text="🔎 Проверить QR"), KeyboardButton(text="🧾 Кто сканировал QR")],
            [KeyboardButton(text="👤 Проверить клиента"), KeyboardButton(text="📋 Показать QR")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="◀️ Главное меню")],
        ],
        resize_keyboard=True,
    )


def parse_code_points(text: str) -> Optional[Tuple[str, int]]:
    """
    Правильный формат:
    КОД БАЛЛЫ

    Пример:
    086817670173442n)+>An 10

    Работает с символами внутри QR:
    > < ' " ) + = , . ; : / % & * ? _ -
    """
    raw = (text or "").strip()
    if not raw:
        return None

    m = re.match(
        r"^(.+?)\s+(\d+)\s*(?:балл|балла|баллов|points|point)?\s*$",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return None

    code = m.group(1).replace("\n", "").replace("\r", "").strip()
    points = int(m.group(2))

    if not code or points <= 0:
        return None

    return code, points


def add_qr_code(code: str, points: int, created_by: Optional[int] = None) -> Tuple[bool, str]:
    code = (code or "").strip()
    points = int(points or 0)

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT code FROM qr_codes WHERE code=?", (code,))
        if cur.fetchone():
            return False, "exists"

        cur.execute(
            "INSERT INTO qr_codes(code, points, used, created_by, created_at) VALUES(?,?,?,?,?)",
            (code, points, 0, created_by, now_ts())
        )
        conn.commit()
        return True, "ok"


def import_qr_from_folder() -> Tuple[int, int, int, int]:
    os.makedirs(QR_CODES_DIR, exist_ok=True)

    added = 0
    exists = 0
    skipped = 0
    files = 0

    for file_name in sorted(os.listdir(QR_CODES_DIR)):
        if not file_name.lower().endswith(".txt"):
            continue

        path = os.path.join(QR_CODES_DIR, file_name)
        if not os.path.isfile(path):
            continue

        files += 1

        try:
            raw = open(path, "r", encoding="utf-8").read()
        except UnicodeDecodeError:
            raw = open(path, "r", encoding="cp1251", errors="ignore").read()

        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parsed = parse_code_points(line)
            if not parsed:
                skipped += 1
                continue

            code, points = parsed
            ok, reason = add_qr_code(code, points, None)
            if ok:
                added += 1
            elif reason == "exists":
                exists += 1
            else:
                skipped += 1

    return added, exists, skipped, files


def clear_qr_database() -> Tuple[int, int]:
    with closing(db()) as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS c FROM qr_codes")
        qr_count = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM scans")
        scan_count = int(cur.fetchone()["c"])

        cur.execute("DELETE FROM qr_codes")
        cur.execute("DELETE FROM scans")

        # Баллы клиентов НЕ обнуляем, только QR базу и историю сканов.
        conn.commit()

    return qr_count, scan_count


def qr_stats() -> dict:
    with closing(db()) as conn:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS c FROM qr_codes")
        total = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM qr_codes WHERE used=0")
        unused = int(cur.fetchone()["c"])

        cur.execute("SELECT COUNT(*) AS c FROM qr_codes WHERE used=1")
        used = int(cur.fetchone()["c"])

        cur.execute("SELECT COALESCE(SUM(points),0) AS s FROM qr_codes WHERE used=0")
        unused_points = int(cur.fetchone()["s"] or 0)

        cur.execute("SELECT COALESCE(SUM(points),0) AS s FROM qr_codes WHERE used=1")
        used_points = int(cur.fetchone()["s"] or 0)

    return {
        "total": total,
        "unused": unused,
        "used": used,
        "unused_points": unused_points,
        "used_points": used_points,
    }


def find_user(text: str) -> Optional[sqlite3.Row]:
    value = (text or "").strip().lstrip("@").lower()
    if not value:
        return None

    with closing(db()) as conn:
        cur = conn.cursor()

        if value.isdigit():
            cur.execute("SELECT * FROM users WHERE user_id=?", (int(value),))
            row = cur.fetchone()
            if row:
                return row

        cur.execute("SELECT * FROM users WHERE lower(username)=?", (value,))
        return cur.fetchone()


def get_qr(code: str) -> Optional[sqlite3.Row]:
    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM qr_codes WHERE code=?", ((code or "").strip(),))
        return cur.fetchone()


def cleanup_recent():
    now = time.time()
    for key, ts in list(recent_scans.items()):
        if now - ts > RECENT_TTL:
            recent_scans.pop(key, None)


def duplicate_scan(user_id: int, code: str) -> bool:
    cleanup_recent()
    key = f"{user_id}:{code}"
    if key in recent_scans:
        return True
    recent_scans[key] = time.time()
    return False


def scan_code(message: Message, code: str) -> str:
    save_user(message)

    if not message.from_user:
        return "❌ Ошибка пользователя."

    user_id = int(message.from_user.id)
    username = username_from_message(message)
    full_name = full_name_from_message(message)
    code = (code or "").strip()

    if duplicate_scan(user_id, code):
        return "⏳ Этот код уже обрабатывается."

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM qr_codes WHERE code=?", (code,))
        qr = cur.fetchone()

        if not qr:
            return (
                "❌ QR не найден в базе.\n\n"
                f"Код: <code>{esc(code)}</code>\n\n"
                "Админ должен сначала добавить QR вручную или импортировать файл."
            )

        if int(qr["used"]) == 1:
            who = qr["used_by_username"] or qr["used_by_full_name"] or qr["used_by"] or "неизвестно"
            return (
                "⚠️ QR уже использован.\n\n"
                f"Код: <code>{esc(code)}</code>\n"
                f"Кем: <b>{esc(who)}</b>"
            )

        points = int(qr["points"])

        cur.execute(
            """
            UPDATE qr_codes
            SET used=1, used_by=?, used_by_username=?, used_by_full_name=?, used_at=?
            WHERE code=?
            """,
            (user_id, username, full_name, now_ts(), code)
        )

        cur.execute(
            "UPDATE users SET points = points + ?, username=?, full_name=?, updated_at=? WHERE user_id=?",
            (points, username, full_name, now_ts(), user_id)
        )

        cur.execute(
            "INSERT INTO scans(code, points, user_id, username, full_name, scanned_at) VALUES(?,?,?,?,?,?)",
            (code, points, user_id, username, full_name, now_ts())
        )

        conn.commit()

    balance = get_points(user_id)

    return (
        "✅ Баллы начислены!\n\n"
        f"Код: <code>{esc(code)}</code>\n"
        f"Начислено: <b>{points}</b>\n"
        f"Ваш баланс: <b>{balance}</b>\n"
        f"Статус: <b>{client_status(balance)}</b>"
    )


# =========================
# COMMANDS
# =========================

@dp.message(CommandStart())
async def start(message: Message):
    save_user(message)
    user_id = int(message.from_user.id) if message.from_user else 0

    await message.answer(
        "Добро пожаловать в <b>Lux Aristokrat</b>!\n\n"
        "Сканируйте QR/Data Matrix, копите баллы, покупайте товары из каталога "
        "и смотрите статус клиента.",
        reply_markup=main_keyboard(is_admin(user_id)),
    )


@dp.message(Command("buttons"))
async def buttons(message: Message):
    save_user(message)
    user_id = int(message.from_user.id) if message.from_user else 0
    await message.answer("Кнопки обновлены.", reply_markup=main_keyboard(is_admin(user_id)))


@dp.message(Command("id"))
async def cmd_id(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0
    await message.answer(f"Ваш Telegram ID: <code>{user_id}</code>\nAdmin: <b>{is_admin(user_id)}</b>")


@dp.message(Command("remove_menu"))
async def cmd_remove_menu(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    await bot.set_chat_menu_button(menu_button=MenuButtonDefault())
    await message.answer("✅ Синяя кнопка Telegram Menu Button отключена.")


@dp.message(Command("syncqr"))
async def cmd_syncqr(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    added, exists, skipped, files = import_qr_from_folder()
    await message.answer(
        "📥 <b>Импорт QR из папки завершён</b>\n\n"
        f"Папка: <code>{esc(QR_CODES_DIR)}</code>\n"
        f"TXT файлов: <b>{files}</b>\n"
        f"Добавлено: <b>{added}</b>\n"
        f"Уже были: <b>{exists}</b>\n"
        f"Пропущено: <b>{skipped}</b>"
    )


@dp.message(Command("clearqr"))
async def cmd_clearqr(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    text = (message.text or "").strip()
    if text != "/clearqr YES":
        await message.answer(
            "⚠️ Для очистки базы QR отправьте:\n"
            "<code>/clearqr YES</code>\n\n"
            "Это удалит QR-коды и историю сканов. Баллы клиентов не обнуляются."
        )
        return

    qr_count, scan_count = clear_qr_database()
    await message.answer(
        "🗑 <b>База QR очищена</b>\n\n"
        f"Удалено QR: <b>{qr_count}</b>\n"
        f"Удалено сканов: <b>{scan_count}</b>\n\n"
        "Теперь загрузите новый файл в <code>/app/qr_codes/qr_codes.txt</code> "
        "и нажмите <b>📥 Импорт QR из папки</b>."
    )


@dp.message(Command("addqr"))
async def cmd_addqr(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    arg = (message.text or "").replace("/addqr", "", 1).strip()
    parsed = parse_code_points(arg)

    if not parsed:
        await message.answer("❌ Формат:\n<code>/addqr 086817670173442n)+&gt;An 10</code>")
        return

    code, points = parsed
    ok, reason = add_qr_code(code, points, user_id)

    if ok:
        await message.answer(f"✅ QR добавлен:\n<code>{esc(code)}</code>\nБаллы: <b>{points}</b>")
    else:
        await message.answer(f"⚠️ QR уже есть в базе:\n<code>{esc(code)}</code>")


# =========================
# BUTTONS
# =========================

@dp.message(F.text == "◀️ Главное меню")
async def home(message: Message):
    save_user(message)
    user_id = int(message.from_user.id) if message.from_user else 0
    pending.pop(user_id, None)
    await message.answer("Главное меню.", reply_markup=main_keyboard(is_admin(user_id)))


@dp.message(F.text == "⚙️ Админ панель")
async def admin_panel(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    pending.pop(user_id, None)
    await message.answer("⚙️ <b>Админ панель</b>\nВыберите действие:", reply_markup=admin_keyboard())


@dp.message(F.text == "➕ Добавить QR вручную")
async def add_qr_wait(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    pending[user_id] = "add_qr"
    await message.answer(
        "➕ Отправьте QR и баллы одним сообщением.\n\n"
        "Формат:\n"
        "<code>086817670173442n)+&gt;An 10</code>\n\n"
        "Важно: не нужно писать «Готово». Просто отправьте код и баллы."
    )


@dp.message(F.text == "📥 Импорт QR из папки")
async def import_button(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    added, exists, skipped, files = import_qr_from_folder()

    await message.answer(
        "📥 <b>Импорт QR из папки завершён</b>\n\n"
        f"Папка: <code>{esc(QR_CODES_DIR)}</code>\n"
        f"TXT файлов: <b>{files}</b>\n"
        f"Добавлено: <b>{added}</b>\n"
        f"Уже были: <b>{exists}</b>\n"
        f"Пропущено: <b>{skipped}</b>\n\n"
        "Правильный формат строки:\n"
        "<code>086817670173442n)+&gt;An 10</code>"
    )


@dp.message(F.text == "🗑 Очистить базу QR")
async def clear_button(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    pending[user_id] = "confirm_clear_qr"
    await message.answer(
        "⚠️ Подтвердите очистку базы QR.\n\n"
        "Это удалит QR-коды и историю сканов.\n"
        "Баллы клиентов НЕ будут обнулены.\n\n"
        "Отправьте: <code>ОЧИСТИТЬ</code>"
    )


@dp.message(F.text == "📦 Остаток QR")
async def qr_left(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    s = qr_stats()

    await message.answer(
        "📦 <b>Остаток QR</b>\n\n"
        f"Всего QR: <b>{s['total']}</b>\n"
        f"Не отсканировано: <b>{s['unused']}</b>\n"
        f"Отсканировано: <b>{s['used']}</b>\n"
        f"Баллов в остатке: <b>{s['unused_points']}</b>\n"
        f"Баллов выдано: <b>{s['used_points']}</b>"
    )


@dp.message(F.text == "🔎 Проверить QR")
async def check_qr_wait(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    pending[user_id] = "check_qr"
    await message.answer("🔎 Отправьте QR-код для проверки.")


@dp.message(F.text == "🧾 Кто сканировал QR")
async def who_qr_wait(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    pending[user_id] = "who_qr"
    await message.answer("🧾 Отправьте QR-код, чтобы узнать кто его сканировал.")


@dp.message(F.text == "👤 Проверить клиента")
async def check_client_wait(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    pending[user_id] = "check_client"
    await message.answer("👤 Отправьте Telegram ID клиента или username без @.")


@dp.message(F.text == "📋 Показать QR")
async def show_qr(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT code, points, used FROM qr_codes ORDER BY created_at DESC LIMIT 30")
        rows = cur.fetchall()

    if not rows:
        await message.answer(
            "📋 QR-кодов в базе пока нет.\n\n"
            "Нажмите <b>📥 Импорт QR из папки</b> или <b>➕ Добавить QR вручную</b>."
        )
        return

    lines = []
    for row in rows:
        status = "✅ свободен" if int(row["used"]) == 0 else "⚠️ использован"
        lines.append(f"{status} | {row['points']} балл. | <code>{esc(row['code'])}</code>")

    await message.answer("📋 <b>Последние 30 QR:</b>\n\n" + "\n".join(lines))


@dp.message(F.text == "📊 Статистика")
async def stats(message: Message):
    user_id = int(message.from_user.id) if message.from_user else 0

    if not is_admin(user_id):
        await message.answer("❌ Только для администратора.")
        return

    s = qr_stats()

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM users")
        users = int(cur.fetchone()["c"])
        cur.execute("SELECT COUNT(*) AS c FROM purchases")
        purchases = int(cur.fetchone()["c"])
        cur.execute("SELECT COALESCE(SUM(price_points),0) AS s FROM purchases")
        spent = int(cur.fetchone()["s"] or 0)

    await message.answer(
        "📊 <b>Статистика</b>\n\n"
        f"Клиентов: <b>{users}</b>\n"
        f"Всего QR: <b>{s['total']}</b>\n"
        f"Свободно QR: <b>{s['unused']}</b>\n"
        f"Использовано QR: <b>{s['used']}</b>\n"
        f"Выдано баллов: <b>{s['used_points']}</b>\n"
        f"Покупок за баллы: <b>{purchases}</b>\n"
        f"Потрачено баллов: <b>{spent}</b>"
    )


@dp.message(F.text == "💰 Мои баллы")
async def my_points(message: Message):
    save_user(message)
    user_id = int(message.from_user.id) if message.from_user else 0
    points = get_points(user_id)

    await message.answer(
        f"💰 Ваши баллы: <b>{points}</b>\n"
        f"Статус: <b>{client_status(points)}</b>"
    )


@dp.message(F.text == "🏆 Мой статус")
async def my_status(message: Message):
    save_user(message)
    user_id = int(message.from_user.id) if message.from_user else 0
    points = get_points(user_id)

    await message.answer(
        f"🏆 Ваш статус: <b>{client_status(points)}</b>\n"
        f"Баллы: <b>{points}</b>"
    )


@dp.message(F.text == "🎁 Каталог")
async def catalog(message: Message):
    save_user(message)
    user_id = int(message.from_user.id) if message.from_user else 0
    points = get_points(user_id)

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM catalog WHERE active=1 ORDER BY price_points ASC")
        rows = cur.fetchall()

    if not rows:
        await message.answer("🎁 Каталог пока пуст.")
        return

    lines = [
        "🎁 <b>Каталог товаров за баллы</b>",
        f"Ваш баланс: <b>{points}</b>",
        "",
        "Чтобы купить, отправьте номер товара.",
        "",
    ]

    for row in rows:
        lines.append(f"{row['id']}. {esc(row['name'])} — <b>{row['price_points']}</b> баллов")

    pending[user_id] = "buy_item"
    await message.answer("\n".join(lines))


# =========================
# WEBAPP SCAN DATA
# =========================

@dp.message(F.web_app_data)
async def webapp_data(message: Message):
    save_user(message)

    data = ""
    try:
        data = message.web_app_data.data or ""
    except Exception:
        data = ""

    log.info("WEBAPP_DATA received: %r", data)

    code = data.strip()

    if code.startswith("{"):
        try:
            obj = json.loads(code)
            action_type = str(obj.get("action") or "").strip()

            # Каталог — отдельная обработка
            if action_type == "catalog_order":
                item_name = esc(obj.get("item") or "")
                category = esc(obj.get("category") or "")
                points_val = int(obj.get("points") or 0)
                await message.answer(
                    f"🛒 <b>Заявка на товар из каталога</b>\n\n"
                    f"Категория: <b>{category}</b>\n"
                    f"Товар: <b>{item_name}</b>\n"
                    f"Цена: <b>{points_val}</b> баллов\n\n"
                    "Обратитесь к администратору для оформления."
                )
                return

            code = str(obj.get("code") or obj.get("qr") or obj.get("data") or "").strip()
        except Exception:
            pass

    if not code:
        await message.answer("❌ QR пустой.")
        return

    await message.answer(scan_code(message, code))


# =========================
# UNIVERSAL TEXT HANDLER
# =========================

@dp.message(F.text)
async def universal_text(message: Message):
    save_user(message)

    user_id = int(message.from_user.id) if message.from_user else 0
    text = (message.text or "").strip()
    action = pending.get(user_id)

    # Подстраховка: если в сообщении есть web_app_data, но оно дошло сюда
    try:
        if getattr(message, "web_app_data", None) and message.web_app_data.data:
            wad = message.web_app_data.data.strip()
            log.info("WEBAPP_DATA via universal_text: %r", wad)
            c = wad
            if c.startswith("{"):
                try:
                    o = json.loads(c)
                    c = str(o.get("code") or o.get("qr") or o.get("data") or "").strip()
                except Exception:
                    pass
            if c:
                await message.answer(scan_code(message, c))
                return
    except Exception as e:
        log.warning("web_app_data check failed: %s", e)

    if action == "add_qr":
        if text.lower() in {"готово", "done", "ok", "ок"}:
            await message.answer(
                "Не нужно писать «Готово».\n\n"
                "Отправьте QR и баллы так:\n"
                "<code>086817670173442n)+&gt;An 10</code>"
            )
            return

        parsed = parse_code_points(text)
        if not parsed:
            await message.answer(
                "❌ Неверный формат.\n\n"
                "Отправьте ОДНИМ сообщением так:\n"
                "<code>086817670173442n)+&gt;An 10</code>\n\n"
                "Последнее число — количество баллов."
            )
            return

        code, points = parsed
        ok, reason = add_qr_code(code, points, user_id)
        pending.pop(user_id, None)

        if ok:
            await message.answer(
                "✅ QR успешно добавлен!\n\n"
                f"Код: <code>{esc(code)}</code>\n"
                f"Баллы: <b>{points}</b>",
                reply_markup=admin_keyboard()
            )
        else:
            await message.answer(
                "⚠️ Такой QR уже есть в базе.\n\n"
                f"Код: <code>{esc(code)}</code>",
                reply_markup=admin_keyboard()
            )
        return

    if action == "confirm_clear_qr":
        if text != "ОЧИСТИТЬ":
            await message.answer("Очистка отменена.", reply_markup=admin_keyboard())
            pending.pop(user_id, None)
            return

        qr_count, scan_count = clear_qr_database()
        pending.pop(user_id, None)
        await message.answer(
            "🗑 <b>База QR очищена</b>\n\n"
            f"Удалено QR: <b>{qr_count}</b>\n"
            f"Удалено сканов: <b>{scan_count}</b>\n\n"
            "Теперь загрузите файл <code>qr_codes.txt</code> в папку <code>/app/qr_codes</code> "
            "и нажмите <b>📥 Импорт QR из папки</b>.",
            reply_markup=admin_keyboard()
        )
        return

    if action == "check_qr":
        pending.pop(user_id, None)
        row = get_qr(text)

        if not row:
            await message.answer(f"❌ QR не найден:\n<code>{esc(text)}</code>", reply_markup=admin_keyboard())
            return

        status = "свободен ✅" if int(row["used"]) == 0 else "использован ⚠️"
        who = row["used_by_username"] or row["used_by_full_name"] or row["used_by"] or "-"

        await message.answer(
            "🔎 <b>Проверка QR</b>\n\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Баллы: <b>{row['points']}</b>\n"
            f"Статус: <b>{status}</b>\n"
            f"Кем использован: <b>{esc(who)}</b>",
            reply_markup=admin_keyboard()
        )
        return

    if action == "who_qr":
        pending.pop(user_id, None)
        row = get_qr(text)

        if not row:
            await message.answer(f"❌ QR не найден:\n<code>{esc(text)}</code>", reply_markup=admin_keyboard())
            return

        if int(row["used"]) == 0:
            await message.answer(
                f"✅ QR ещё не сканировали.\nКод: <code>{esc(row['code'])}</code>",
                reply_markup=admin_keyboard()
            )
            return

        await message.answer(
            "🧾 <b>Кто сканировал QR</b>\n\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Telegram ID: <code>{esc(row['used_by'])}</code>\n"
            f"Username: <b>@{esc(row['used_by_username'])}</b>\n"
            f"Имя: <b>{esc(row['used_by_full_name'])}</b>\n"
            f"Баллы: <b>{row['points']}</b>",
            reply_markup=admin_keyboard()
        )
        return

    if action == "check_client":
        pending.pop(user_id, None)
        row = find_user(text)

        if not row:
            await message.answer("❌ Клиент не найден. Клиент должен хотя бы один раз нажать /start.", reply_markup=admin_keyboard())
            return

        points = int(row["points"])

        await message.answer(
            "👤 <b>Клиент</b>\n\n"
            f"Имя: <b>{esc(row['full_name'])}</b>\n"
            f"Username: <b>@{esc(row['username'])}</b>\n"
            f"Telegram ID: <code>{row['user_id']}</code>\n"
            f"Баллы: <b>{points}</b>\n"
            f"Статус: <b>{client_status(points)}</b>",
            reply_markup=admin_keyboard()
        )
        return

    if action == "buy_item":
        if not text.isdigit():
            await message.answer("❌ Отправьте номер товара из каталога.")
            return

        item_id = int(text)

        with closing(db()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM catalog WHERE id=? AND active=1", (item_id,))
            item = cur.fetchone()

            if not item:
                await message.answer("❌ Товар не найден.")
                return

            balance = get_points(user_id)
            price = int(item["price_points"])

            if balance < price:
                await message.answer(
                    f"❌ Недостаточно баллов.\n"
                    f"Нужно: <b>{price}</b>\n"
                    f"У вас: <b>{balance}</b>"
                )
                return

            username = username_from_message(message)
            full_name = full_name_from_message(message)

            cur.execute("UPDATE users SET points = points - ?, updated_at=? WHERE user_id=?", (price, now_ts(), user_id))
            cur.execute(
                "INSERT INTO purchases(user_id, username, full_name, item_name, price_points, created_at) VALUES(?,?,?,?,?,?)",
                (user_id, username, full_name, item["name"], price, now_ts())
            )
            conn.commit()

        pending.pop(user_id, None)
        new_balance = get_points(user_id)

        await message.answer(
            "✅ Покупка оформлена!\n\n"
            f"Товар: <b>{esc(item['name'])}</b>\n"
            f"Списано: <b>{price}</b> баллов\n"
            f"Остаток: <b>{new_balance}</b>"
        )
        return

    # Если клиент прислал QR обычным текстом — пробуем засчитать как скан
    if len(text) >= 6 and not text.startswith("/"):
        await message.answer(scan_code(message, text))
        return

    await message.answer("Выберите действие кнопками ниже.", reply_markup=main_keyboard(is_admin(user_id)))


# =========================
# START
# =========================

async def on_startup():
    init_db()

    # Автоматический импорт QR-кодов из папки при старте
    added, exists, skipped, files = import_qr_from_folder()
    log.info("Auto-import QR on startup: files=%s added=%s exists=%s skipped=%s", files, added, exists, skipped)

    # Убирает синюю кнопку Telegram Menu Button / Open
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonDefault())
        log.info("Telegram blue menu button reset to default")
    except Exception as e:
        log.warning("Cannot reset menu button: %s", e)

    log.info("BOT STARTED")
    log.info("DB_PATH=%s", DB_PATH)
    log.info("QR_CODES_DIR=%s", QR_CODES_DIR)
    log.info("WEBAPP_URL=%s", WEBAPP_URL)
    log.info("ADMIN_IDS=%s", sorted(ADMIN_IDS))


async def main():
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
