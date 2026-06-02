import os
import re
import time
import html
import sqlite3
import logging
from contextlib import closing
from typing import Optional, Tuple, List, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    WebAppInfo,
    MenuButtonWebApp,
)

# =========================
# LUX ARISTOKRAT QR BOT
# main.py
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
    raise RuntimeError("BOT_TOKEN / API_TOKEN не найден в переменных окружения")

WEBAPP_URL = (
    os.getenv("WEBAPP_URL", "").strip()
    or "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/?v=1"
)

DB_PATH = os.getenv("DB_PATH", "/app/data/lux_aristokrat.db").strip() or "/app/data/lux_aristokrat.db"
QR_CODES_DIR = os.getenv("QR_CODES_DIR", "/app/qr_codes").strip() or "/app/qr_codes"

DEFAULT_ADMIN_IDS = {6013591658, 6292063248}

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_ID_RAW = os.getenv("ADMIN_ID", "").strip()


def parse_admins() -> set[int]:
    admins = set(DEFAULT_ADMIN_IDS)

    for value in [ADMIN_ID_RAW]:
        try:
            if value and int(value) > 0:
                admins.add(int(value))
        except Exception:
            pass

    if ADMIN_IDS_RAW:
        for part in ADMIN_IDS_RAW.split(","):
            part = part.strip()
            try:
                if part and int(part) > 0:
                    admins.add(int(part))
            except Exception:
                pass

    return admins


ADMIN_IDS = parse_admins()

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Простое состояние без FSM: работает даже на BotHost.
# pending[user_id] = action
pending: Dict[int, str] = {}

# Защита от повторного скана
recent_scans: Dict[str, float] = {}
RECENT_TTL = 8


# =========================
# DATABASE
# =========================

def ensure_dirs() -> None:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    os.makedirs(QR_CODES_DIR, exist_ok=True)


def db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
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
            items = [
                ("Скидка 10 000 сум", 50),
                ("Скидка 25 000 сум", 120),
                ("Подарок от Lux Aristokrat", 250),
                ("VIP скидка", 500),
            ]
            for name, price in items:
                cur.execute(
                    "INSERT INTO catalog(name, price_points, active, created_at) VALUES(?,?,1,?)",
                    (name, price, now)
                )

        conn.commit()


# =========================
# HELPERS
# =========================

def now_ts() -> int:
    return int(time.time())


def esc(value: Any) -> str:
    return html.escape(str(value or ""))


def is_admin(user_id: int) -> bool:
    return int(user_id) in ADMIN_IDS


def full_name_from_message(message: Message) -> str:
    u = message.from_user
    if not u:
        return ""
    return " ".join([x for x in [(u.first_name or "").strip(), (u.last_name or "").strip()] if x]).strip()


def username_from_message(message: Message) -> str:
    u = message.from_user
    if not u or not u.username:
        return ""
    return u.username.strip().lstrip("@").lower()


def save_user(message: Message) -> None:
    if not message.from_user:
        return

    uid = int(message.from_user.id)
    username = username_from_message(message)
    full_name = full_name_from_message(message)
    now = now_ts()

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE user_id=?", (uid,))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE users SET username=?, full_name=?, updated_at=? WHERE user_id=?",
                (username, full_name, now, uid)
            )
        else:
            cur.execute(
                "INSERT INTO users(user_id, username, full_name, points, created_at, updated_at) VALUES(?,?,?,?,?,?)",
                (uid, username, full_name, 0, now, now)
            )
        conn.commit()


def get_user_points(user_id: int) -> int:
    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT points FROM users WHERE user_id=?", (int(user_id),))
        row = cur.fetchone()
        return int(row["points"]) if row else 0


def status_by_points(points: int) -> str:
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
            [KeyboardButton(text="📦 Остаток QR"), KeyboardButton(text="🔎 Проверить QR")],
            [KeyboardButton(text="👤 Проверить клиента"), KeyboardButton(text="🧾 Кто сканировал QR")],
            [KeyboardButton(text="📋 Показать QR"), KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="◀️ Главное меню")],
        ],
        resize_keyboard=True,
    )


def parse_code_points(text: str) -> Optional[Tuple[str, int]]:
    """
    Принимает ЛЮБОЙ QR-код с символами: > < ' " ) + = и т.д.
    Главное: последние символы в сообщении должны быть числом баллов.
    Примеры:
    086817670173442n)+>An 10
    086817670173442n)+>An    10 балла
    """
    raw = (text or "").strip()

    # Убираем командные слова, если админ случайно написал "готово"
    if not raw:
        return None

    # Последнее число в конце строки = баллы
    m = re.match(r"^(.+?)\s+(\d+)\s*(?:балл|балла|баллов|points|point)?\s*$", raw, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None

    code = m.group(1).strip()
    points = int(m.group(2))

    if not code or points <= 0:
        return None

    # Не разрешаем переносы внутри одного кода
    code = code.replace("\n", "").replace("\r", "").strip()

    if len(code) < 3:
        return None

    return code, points


def add_qr_code(code: str, points: int, created_by: Optional[int] = None) -> Tuple[bool, str]:
    code = (code or "").strip()
    points = int(points or 0)

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT code FROM qr_codes WHERE code=?", (code,))
        exists = cur.fetchone()
        if exists:
            return False, "exists"

        cur.execute(
            "INSERT INTO qr_codes(code, points, used, created_by, created_at) VALUES(?,?,?,?,?)",
            (code, points, 0, created_by, now_ts())
        )
        conn.commit()
        return True, "ok"


def import_qr_from_folder() -> Tuple[int, int, int, List[str]]:
    os.makedirs(QR_CODES_DIR, exist_ok=True)

    added = 0
    exists = 0
    skipped = 0
    files = []

    for filename in sorted(os.listdir(QR_CODES_DIR)):
        if not filename.lower().endswith(".txt"):
            continue

        path = os.path.join(QR_CODES_DIR, filename)
        if not os.path.isfile(path):
            continue

        files.append(filename)

        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(path, "r", encoding="cp1251", errors="ignore") as f:
                lines = f.readlines()

        for line in lines:
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


def check_qr(code: str) -> Optional[sqlite3.Row]:
    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM qr_codes WHERE code=?", ((code or "").strip(),))
        return cur.fetchone()


def find_user_by_text(text: str) -> Optional[sqlite3.Row]:
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


def cleanup_recent() -> None:
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


def scan_code_for_user(message: Message, code: str) -> str:
    save_user(message)

    if not message.from_user:
        return "❌ Ошибка пользователя."

    user_id = int(message.from_user.id)
    username = username_from_message(message)
    full_name = full_name_from_message(message)
    code = (code or "").strip()

    if duplicate_scan(user_id, code):
        return "⏳ Этот код уже обрабатывается. Подождите несколько секунд."

    with closing(db()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM qr_codes WHERE code=?", (code,))
        qr = cur.fetchone()

        if not qr:
            return f"❌ QR не найден:\n<code>{esc(code)}</code>\n\nАдмин должен сначала добавить или импортировать этот QR."

        if int(qr["used"]) == 1:
            who = qr["used_by_username"] or qr["used_by_full_name"] or qr["used_by"] or "неизвестно"
            return f"⚠️ QR уже использован.\nКод: <code>{esc(code)}</code>\nКем: <b>{esc(who)}</b>"

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
            """
            UPDATE users
            SET points = points + ?, username=?, full_name=?, updated_at=?
            WHERE user_id=?
            """,
            (points, username, full_name, now_ts(), user_id)
        )

        cur.execute(
            """
            INSERT INTO scans(code, points, user_id, username, full_name, scanned_at)
            VALUES(?,?,?,?,?,?)
            """,
            (code, points, user_id, username, full_name, now_ts())
        )

        conn.commit()

    balance = get_user_points(user_id)
    return (
        "✅ Баллы начислены!\n\n"
        f"Код: <code>{esc(code)}</code>\n"
        f"Начислено: <b>{points}</b>\n"
        f"Ваш баланс: <b>{balance}</b>\n"
        f"Статус: <b>{status_by_points(balance)}</b>"
    )


# =========================
# COMMANDS
# =========================

@dp.message(CommandStart())
async def start(message: Message):
    save_user(message)
    uid = int(message.from_user.id) if message.from_user else 0
    text = (
        "Добро пожаловать в <b>Lux Aristokrat</b>!\n\n"
        "Здесь можно сканировать QR/Data Matrix, копить баллы, "
        "покупать товары из каталога и смотреть статус клиента."
    )
    await message.answer(text, reply_markup=main_keyboard(is_admin(uid)))


@dp.message(Command("buttons"))
async def buttons(message: Message):
    save_user(message)
    uid = int(message.from_user.id) if message.from_user else 0
    await message.answer("Кнопки обновлены.", reply_markup=main_keyboard(is_admin(uid)))


@dp.message(Command("id"))
async def cmd_id(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    await message.answer(f"Ваш Telegram ID: <code>{uid}</code>\nAdmin: <b>{is_admin(uid)}</b>")


@dp.message(Command("syncqr"))
async def cmd_syncqr(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return

    added, exists, skipped, files = import_qr_from_folder()
    await message.answer(
        "📥 Импорт QR из папки завершён.\n\n"
        f"Папка: <code>{esc(QR_CODES_DIR)}</code>\n"
        f"Файлы: <b>{len(files)}</b>\n"
        f"Добавлено: <b>{added}</b>\n"
        f"Уже были: <b>{exists}</b>\n"
        f"Пропущено: <b>{skipped}</b>"
    )


@dp.message(Command("addqr"))
async def cmd_addqr(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return

    arg = (message.text or "").replace("/addqr", "", 1).strip()
    parsed = parse_code_points(arg)
    if not parsed:
        await message.answer("❌ Формат:\n<code>/addqr 086817670173442n)+&gt;An 10</code>")
        return

    code, points = parsed
    ok, reason = add_qr_code(code, points, uid)
    if ok:
        await message.answer(f"✅ QR добавлен:\n<code>{esc(code)}</code>\nБаллы: <b>{points}</b>")
    else:
        await message.answer(f"⚠️ QR уже есть в базе:\n<code>{esc(code)}</code>")


# =========================
# BUTTON HANDLERS
# =========================

@dp.message(F.text == "◀️ Главное меню")
async def main_menu(message: Message):
    save_user(message)
    uid = int(message.from_user.id) if message.from_user else 0
    pending.pop(uid, None)
    await message.answer("Главное меню.", reply_markup=main_keyboard(is_admin(uid)))


@dp.message(F.text == "⚙️ Админ панель")
async def admin_panel(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return
    pending.pop(uid, None)
    await message.answer("⚙️ <b>Админ панель</b>\nВыберите действие:", reply_markup=admin_keyboard())


@dp.message(F.text == "➕ Добавить QR вручную")
async def add_qr_wait(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return
    pending[uid] = "add_qr"
    await message.answer(
        "➕ Отправьте QR и баллы одним сообщением.\n\n"
        "Формат:\n"
        "<code>086817670173442n)+&gt;An 10</code>\n\n"
        "Важно: не нужно писать «Готово». Просто отправьте код и баллы."
    )


@dp.message(F.text == "📥 Импорт QR из папки")
async def import_folder_button(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return

    added, exists, skipped, files = import_qr_from_folder()
    await message.answer(
        "📥 <b>Импорт QR из папки завершён</b>\n\n"
        f"Папка: <code>{esc(QR_CODES_DIR)}</code>\n"
        f"Найдено TXT файлов: <b>{len(files)}</b>\n"
        f"Добавлено: <b>{added}</b>\n"
        f"Уже были в базе: <b>{exists}</b>\n"
        f"Пропущено строк: <b>{skipped}</b>\n\n"
        "Файл должен лежать в папке <code>/app/qr_codes</code> и строки должны быть так:\n"
        "<code>КОД 10</code>"
    )


@dp.message(F.text == "📦 Остаток QR")
async def qr_left(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return

    s = qr_stats()
    await message.answer(
        "📦 <b>Остаток QR</b>\n\n"
        f"Всего QR: <b>{s['total']}</b>\n"
        f"Не отсканировано: <b>{s['unused']}</b>\n"
        f"Отсканировано: <b>{s['used']}</b>\n"
        f"Баллов в остатке: <b>{s['unused_points']}</b>\n"
        f"Баллов уже выдано: <b>{s['used_points']}</b>"
    )


@dp.message(F.text == "🔎 Проверить QR")
async def check_qr_wait(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return
    pending[uid] = "check_qr"
    await message.answer("🔎 Отправьте QR-код для проверки.")


@dp.message(F.text == "🧾 Кто сканировал QR")
async def who_qr_wait(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return
    pending[uid] = "who_qr"
    await message.answer("🧾 Отправьте QR-код, чтобы узнать кто его сканировал.")


@dp.message(F.text == "👤 Проверить клиента")
async def check_client_wait(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
        await message.answer("❌ Только для администратора.")
        return
    pending[uid] = "check_client"
    await message.answer("👤 Отправьте Telegram ID клиента или username без @.")


@dp.message(F.text == "📋 Показать QR")
async def show_qr(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
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
    for r in rows:
        status = "✅ свободен" if int(r["used"]) == 0 else "⚠️ использован"
        lines.append(f"{status} | {r['points']} балл. | <code>{esc(r['code'])}</code>")

    await message.answer("📋 <b>Последние 30 QR:</b>\n\n" + "\n".join(lines))


@dp.message(F.text == "📊 Статистика")
async def stats(message: Message):
    uid = int(message.from_user.id) if message.from_user else 0
    if not is_admin(uid):
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
    uid = int(message.from_user.id) if message.from_user else 0
    points = get_user_points(uid)
    await message.answer(
        f"💰 Ваши баллы: <b>{points}</b>\n"
        f"Статус: <b>{status_by_points(points)}</b>"
    )


@dp.message(F.text == "🏆 Мой статус")
async def my_status(message: Message):
    save_user(message)
    uid = int(message.from_user.id) if message.from_user else 0
    points = get_user_points(uid)
    await message.answer(
        f"🏆 Ваш статус: <b>{status_by_points(points)}</b>\n"
        f"Баллы: <b>{points}</b>"
    )


@dp.message(F.text == "🎁 Каталог")
async def catalog(message: Message):
    save_user(message)
    uid = int(message.from_user.id) if message.from_user else 0
    points = get_user_points(uid)

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
        ""
    ]

    for r in rows:
        lines.append(f"{r['id']}. {esc(r['name'])} — <b>{r['price_points']}</b> баллов")

    pending[uid] = "buy_item"
    await message.answer("\n".join(lines))


# =========================
# WEBAPP DATA / SCAN
# =========================

@dp.message(F.web_app_data)
async def webapp_data(message: Message):
    save_user(message)
    data = ""
    try:
        data = message.web_app_data.data or ""
    except Exception:
        data = ""

    code = data.strip()
    if code.startswith("{"):
        # Примитивная поддержка JSON без жесткой зависимости.
        import json
        try:
            obj = json.loads(code)
            code = str(obj.get("code") or obj.get("qr") or obj.get("data") or "").strip()
        except Exception:
            pass

    if not code:
        await message.answer("❌ QR пустой.")
        return

    result = scan_code_for_user(message, code)
    await message.answer(result)


# =========================
# UNIVERSAL TEXT HANDLER
# =========================

@dp.message(F.text)
async def universal_text(message: Message):
    save_user(message)

    uid = int(message.from_user.id) if message.from_user else 0
    text = (message.text or "").strip()

    # Игнорируем системные кнопки, если они уже обработаны выше
    if text in {
        "📷 Сканер QR", "💰 Мои баллы", "🎁 Каталог", "🏆 Мой статус",
        "⚙️ Админ панель", "◀️ Главное меню",
        "➕ Добавить QR вручную", "📥 Импорт QR из папки", "📦 Остаток QR",
        "🔎 Проверить QR", "👤 Проверить клиента", "🧾 Кто сканировал QR",
        "📋 Показать QR", "📊 Статистика",
    }:
        return

    action = pending.get(uid)

    if action == "add_qr":
        parsed = parse_code_points(text)
        if not parsed:
            await message.answer(
                "❌ Неверный формат.\n\n"
                "Отправьте ОДНИМ сообщением так:\n"
                "<code>086817670173442n)+&gt;An 10</code>\n\n"
                "Последнее число — это количество баллов."
            )
            return

        code, points = parsed
        ok, reason = add_qr_code(code, points, uid)

        # ВАЖНО: состояние сбрасываем только после успешного распознавания,
        # поэтому не нужно писать «Готово».
        pending.pop(uid, None)

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

    if action == "check_qr":
        pending.pop(uid, None)
        row = check_qr(text)
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
        pending.pop(uid, None)
        row = check_qr(text)
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
        pending.pop(uid, None)
        row = find_user_by_text(text)
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
            f"Статус: <b>{status_by_points(points)}</b>",
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

            points = get_user_points(uid)
            price = int(item["price_points"])

            if points < price:
                await message.answer(
                    f"❌ Недостаточно баллов.\n"
                    f"Нужно: <b>{price}</b>\n"
                    f"У вас: <b>{points}</b>"
                )
                return

            username = username_from_message(message)
            full_name = full_name_from_message(message)

            cur.execute("UPDATE users SET points = points - ?, updated_at=? WHERE user_id=?", (price, now_ts(), uid))
            cur.execute(
                """
                INSERT INTO purchases(user_id, username, full_name, item_name, price_points, created_at)
                VALUES(?,?,?,?,?,?)
                """,
                (uid, username, full_name, item["name"], price, now_ts())
            )
            conn.commit()

        pending.pop(uid, None)
        new_points = get_user_points(uid)
        await message.answer(
            "✅ Покупка оформлена!\n\n"
            f"Товар: <b>{esc(item['name'])}</b>\n"
            f"Списано: <b>{price}</b> баллов\n"
            f"Остаток: <b>{new_points}</b>"
        )
        return

    # Если клиент отправил QR текстом — попробуем отсканировать без WebApp
    # Это удобно для проверки.
    if len(text) >= 6 and not text.startswith("/"):
        result = scan_code_for_user(message, text)
        await message.answer(result)
        return

    await message.answer("Выберите действие кнопками ниже.", reply_markup=main_keyboard(is_admin(uid)))


# =========================
# STARTUP
# =========================

async def on_startup() -> None:
    init_db()
    log.info("BOT STARTED")
    log.info("DB_PATH=%s", DB_PATH)
    log.info("QR_CODES_DIR=%s", QR_CODES_DIR)
    log.info("WEBAPP_URL=%s", WEBAPP_URL)
    log.info("ADMIN_IDS=%s", sorted(ADMIN_IDS))

    try:
        await bot.set_chat_menu_button(menu_button=None)
        log.info("Blue menu button disabled")
    except Exception as e:
        log.warning("Cannot disable blue menu button: %s", e)


async def main() -> None:
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
