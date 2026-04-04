print("=== LUX ARISTOKRAT MULTILANG ADMIN VERSION FIXED ===")

import asyncio
import csv
import json
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
        WebAppInfo,
        MenuButtonWebApp,
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
        WebAppInfo,
        MenuButtonWebApp,
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
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://tahirovdd-lang.github.io/QR-project-Lux-Aristokrat/").strip()
DB_PATH = os.getenv("DB_PATH", "lux_aristokrat.db")
DATA_DIR = os.getenv("DATA_DIR", "/app/data")

ADMIN_ID = int(os.getenv("ADMIN_ID", "0") or "0")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()

ADMIN_IDS = set()
if ADMIN_ID:
    ADMIN_IDS.add(ADMIN_ID)
if ADMIN_IDS_RAW:
    for x in ADMIN_IDS_RAW.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))

logging.info("ADMIN_ID from env: %s", ADMIN_ID)
logging.info("ADMIN_IDS from env: %s", sorted(list(ADMIN_IDS)))

os.makedirs(DATA_DIR, exist_ok=True)
QR_DIR = os.path.join(DATA_DIR, "generated_qr")
EXPORT_DIR = os.path.join(DATA_DIR, "exports")
os.makedirs(QR_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

DB_FILE = os.path.join(DATA_DIR, DB_PATH) if not os.path.isabs(DB_PATH) else DB_PATH

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

admin_states: dict[int, dict] = {}

LANG_BUTTONS = {
    "🇷🇺 RU": "ru",
    "🇺🇿 UZ": "uz",
    "🇹🇯 TJ": "tj",
    "🇬🇧 EN": "en",
}

BACK_TEXT = {"ru": "🔙 Назад", "uz": "🔙 Orqaga", "tj": "🔙 Бозгашт", "en": "🔙 Back"}
CANCEL_TEXT = {"ru": "❌ Отмена", "uz": "❌ Bekor qilish", "tj": "❌ Бекор кардан", "en": "❌ Cancel"}

SCAN_TEXT = {"ru": "📷 Сканировать QR", "uz": "📷 QR skanerlash", "tj": "📷 QR-ро скан кардан", "en": "📷 Scan QR"}
POINTS_TEXT = {"ru": "💎 Мои баллы", "uz": "💎 Mening ballarim", "tj": "💎 Баллҳои ман", "en": "💎 My Points"}
HISTORY_TEXT = {"ru": "📜 История", "uz": "📜 Tarix", "tj": "📜 Таърих", "en": "📜 History"}
LEVEL_TEXT = {"ru": "🏆 Мой уровень", "uz": "🏆 Mening darajam", "tj": "🏆 Сатҳи ман", "en": "🏆 My Level"}
HOW_TEXT = {"ru": "ℹ️ Как получить баллы", "uz": "ℹ️ Ballarni qanday olish mumkin", "tj": "ℹ️ Чӣ тавр балл гирифтан мумкин", "en": "ℹ️ How to Get Points"}

ADD_QR_TEXT = {"ru": "➕ Добавить QR", "uz": "➕ QR qo‘shish", "tj": "➕ Иловаи QR", "en": "➕ Add QR"}
BULK_QR_TEXT = {"ru": "📥 Bulk QR", "uz": "📥 Bulk QR", "tj": "📥 Bulk QR", "en": "📥 Bulk QR"}
LIST_QR_TEXT = {"ru": "📋 Список QR", "uz": "📋 QR ro‘yxati", "tj": "📋 Рӯйхати QR", "en": "📋 QR List"}
PNG_QR_TEXT = {"ru": "🧾 Сделать QR PNG", "uz": "🧾 QR PNG yaratish", "tj": "🧾 Сохтани QR PNG", "en": "🧾 Make QR PNG"}
DISABLE_QR_TEXT = {"ru": "⛔️ Отключить QR", "uz": "⛔️ QR o‘chirish", "tj": "⛔️ QR-ро хомӯш кардан", "en": "⛔️ Disable QR"}
ENABLE_QR_TEXT = {"ru": "✅ Включить QR", "uz": "✅ QR yoqish", "tj": "✅ QR-ро фаъол кардан", "en": "✅ Enable QR"}
DELETE_QR_TEXT = {"ru": "🗑 Удалить QR", "uz": "🗑 QR o‘chirish", "tj": "🗑 Ҳазфи QR", "en": "🗑 Delete QR"}
FIND_USER_TEXT = {"ru": "👤 Найти пользователя", "uz": "👤 Foydalanuvchini topish", "tj": "👤 Ёфтани корбар", "en": "👤 Find User"}
ADD_POINTS_TEXT = {"ru": "🎁 Начислить баллы", "uz": "🎁 Ball qo‘shish", "tj": "🎁 Иловаи балл", "en": "🎁 Add Points"}
REMOVE_POINTS_TEXT = {"ru": "➖ Списать баллы", "uz": "➖ Ball yechish", "tj": "➖ Кам кардани балл", "en": "➖ Remove Points"}
STATS_TEXT = {"ru": "📊 Статистика", "uz": "📊 Statistika", "tj": "📊 Омор", "en": "📊 Statistics"}
TOP_TEXT = {"ru": "🥇 Топ пользователей", "uz": "🥇 Top foydalanuvchilar", "tj": "🥇 Беҳтарин корбарон", "en": "🥇 Top Users"}
EXPORT_USERS_TEXT = {"ru": "📤 Экспорт users", "uz": "📤 users eksport", "tj": "📤 users экспорт", "en": "📤 Export users"}
EXPORT_SCANS_TEXT = {"ru": "📤 Экспорт scans", "uz": "📤 scans eksport", "tj": "📤 scans экспорт", "en": "📤 Export scans"}

UI = {
    "ru": {
        "choose_lang": "Выберите язык / Tilni tanlang / Забонро интихоб кунед / Choose language",
        "welcome": "✨ <b>Lux Aristokrat</b>\n\nЗдравствуйте, {name}!\nДобро пожаловать в бонусную систему.\n\nЗдесь вы можете:\n• сканировать QR-коды\n• получать бонусные баллы\n• смотреть историю начислений\n• отслеживать свой уровень\n{admin}\n\nНажмите <b>«📷 Сканировать QR»</b>.",
        "admin_note": "\n\n<b>Вы вошли как администратор.</b> Вам доступны загрузка и управление QR.",
        "my_points": "💎 Ваш баланс: <b>{points}</b>\n🏆 Ваш уровень: <b>{level}</b>",
        "my_level": "🏆 Ваш уровень: <b>{level}</b>\nБаллы: <b>{points}</b>\n\nУровни:\n• Silver: 0–299\n• Gold: 300–999\n• VIP: 1000+",
        "history_empty": "📜 История пока пустая.",
        "history_title": "📜 <b>Ваша история</b>\n",
        "how": "ℹ️ <b>Как получить баллы</b>\n\n1. Нажмите кнопку «📷 Сканировать QR»\n2. Откроется mini app с камерой\n3. Наведите камеру на код\n4. Баллы начислятся автоматически\n\nОдин и тот же QR-код одному пользователю засчитывается только один раз.",
        "unknown_menu": "Выберите действие через меню.",
        "webapp_bad": "❌ Ошибка данных из WebApp.",
        "webapp_unknown": "⚠️ Неизвестное действие WebApp.",
        "bad_code": "❌ Пустой или некорректный QR-код.",
        "qr_not_found": "❌ Такой QR-код не найден.",
        "qr_disabled": "⚠️ QR-код <b>{title}</b> отключён.",
        "already_scanned": "⚠️ Вы уже сканировали этот QR-код.\n\nQR: <b>{title}</b>\nБаланс: <b>{points}</b>\nУровень: <b>{level}</b>",
        "points_added": "✅ <b>Баллы начислены!</b>\n\nQR: <b>{title}</b>\nКод: <code>{code}</code>\nНачислено: <b>+{added}</b>\nБаланс: <b>{points}</b>\nУровень: <b>{level}</b>",
        "add_qr_prompt": "➕ <b>Добавление QR</b>\n\nОтправьте:\n<code>Название|Баллы</code>\nили\n<code>CODE|Название|Баллы</code>",
        "bulk_prompt": "📥 <b>Массовое добавление QR</b>\n\nОтправьте много строк одним сообщением.\n\nФорматы строк:\n<code>CODE</code>\n<code>CODE|Баллы</code>\n<code>CODE|Название|Баллы</code>",
        "qr_list_empty": "Список QR пуст.",
        "qr_list_title": "📋 <b>Список QR</b>\n",
        "disable_prompt": "Отправьте <b>ID QR</b>, который нужно отключить.",
        "enable_prompt": "Отправьте <b>ID QR</b>, который нужно включить.",
        "delete_prompt": "Отправьте <b>ID QR</b>, который нужно удалить.",
        "png_prompt": "Отправьте <b>ID QR</b>, для которого нужно сделать PNG.",
        "add_points_prompt": "Отправьте:\n<code>user_id|баллы</code>",
        "remove_points_prompt": "Отправьте:\n<code>user_id|баллы</code>",
        "find_user_prompt": "Отправьте <b>ID пользователя</b>.",
        "stats_title": "📊 <b>Статистика</b>\n",
        "top_title": "🥇 <b>Топ пользователей</b>\n",
        "users_none": "Пользователей пока нет.",
        "cancelled": "Отменено.",
        "backed": "Возврат в главное меню.",
        "bulk_done": "📥 <b>Bulk QR завершён</b>",
        "bulk_created": "✅ Создано: <b>{n}</b>",
        "bulk_skipped": "⚠️ Пропущено: <b>{n}</b>",
        "bulk_created_title": "<b>Созданные QR:</b>",
        "bulk_skipped_title": "<b>Пропущенные строки:</b>",
        "wrong_format": "❌ Неверный формат.",
        "code_exists": "❌ Такой код уже существует.",
        "points_number": "❌ Баллы должны быть числом больше 0.",
        "id_number": "❌ ID должен быть числом.",
        "qr_missing": "❌ QR не найден.",
        "qr_disabled_ok": "⛔️ QR <b>{title}</b> отключён.",
        "qr_enabled_ok": "✅ QR <b>{title}</b> включён.",
        "qr_deleted_ok": "🗑 QR <b>{title}</b> удалён.",
        "user_not_found": "❌ Пользователь не найден.",
        "add_points_ok": "✅ Начислено <b>+{added}</b> пользователю <code>{user_id}</code>\nБаланс: <b>{points}</b>\nУровень: <b>{level}</b>",
        "remove_points_ok": "➖ Списано <b>{removed}</b> у пользователя <code>{user_id}</code>\nБаланс: <b>{points}</b>\nУровень: <b>{level}</b>",
        "notify_add": "🎁 Вам начислено <b>+{added}</b> баллов.\nБаланс: <b>{points}</b>\nУровень: <b>{level}</b>",
        "notify_remove": "➖ У вас списано <b>{removed}</b> баллов.\nБаланс: <b>{points}</b>\nУровень: <b>{level}</b>",
        "find_user_title": "👤 <b>Пользователь</b>\n",
        "last_scans": "<b>Последние сканы:</b>",
        "history_none": "История пуста.",
        "export_users_done": "📤 Экспорт users готов.",
        "export_scans_done": "📤 Экспорт scans готов.",
        "myid": "Ваш Telegram ID: <code>{id}</code>\nAdmin access: <b>{admin}</b>\nADMIN_ID env: <code>{admin_id}</code>\nADMIN_IDS env: <code>{admin_ids}</code>",
        "lang_saved": "✅ Язык сохранён: Русский",
    },
    "uz": {
        "choose_lang": "Выберите язык / Tilni tanlang / Забонро интихоб кунед / Choose language",
        "welcome": "✨ <b>Lux Aristokrat</b>\n\nSalom, {name}!\nBonus tizimiga xush kelibsiz.\n\nBu yerda siz:\n• QR-kodlarni skaner qilishingiz\n• bonus ballar olishingiz\n• tarixni ko‘rishingiz\n• darajangizni kuzatishingiz mumkin\n{admin}\n\n<b>«📷 QR skanerlash»</b> tugmasini bosing.",
        "admin_note": "\n\n<b>Siz administrator sifatida kirdingiz.</b> QR yuklash va boshqarish imkoniyati mavjud.",
        "my_points": "💎 Sizning balansingiz: <b>{points}</b>\n🏆 Darajangiz: <b>{level}</b>",
        "my_level": "🏆 Sizning darajangiz: <b>{level}</b>\nBallar: <b>{points}</b>\n\nDarajalar:\n• Silver: 0–299\n• Gold: 300–999\n• VIP: 1000+",
        "history_empty": "📜 Tarix hozircha bo‘sh.",
        "history_title": "📜 <b>Sizning tarixingiz</b>\n",
        "how": "ℹ️ <b>Ballarni qanday olish mumkin</b>\n\n1. «📷 QR skanerlash» tugmasini bosing\n2. Kamera bilan mini app ochiladi\n3. Kamerani QR-kodga yo‘naltiring\n4. Ballar avtomatik qo‘shiladi\n\nBir xil QR-kod bitta foydalanuvchiga faqat bir marta hisoblanadi.",
        "unknown_menu": "Menyudan amalni tanlang.",
        "webapp_bad": "❌ WebApp ma’lumotlarida xatolik.",
        "webapp_unknown": "⚠️ Noma’lum WebApp harakati.",
        "bad_code": "❌ Bo‘sh yoki noto‘g‘ri QR-kod.",
        "qr_not_found": "❌ Bunday QR-kod topilmadi.",
        "qr_disabled": "⚠️ <b>{title}</b> QR-kodi o‘chirilgan.",
        "already_scanned": "⚠️ Siz bu QR-kodni avval skaner qilgansiz.\n\nQR: <b>{title}</b>\nBalans: <b>{points}</b>\nDaraja: <b>{level}</b>",
        "points_added": "✅ <b>Ballar qo‘shildi!</b>\n\nQR: <b>{title}</b>\nKod: <code>{code}</code>\nQo‘shildi: <b>+{added}</b>\nBalans: <b>{points}</b>\nDaraja: <b>{level}</b>",
        "add_qr_prompt": "➕ <b>QR qo‘shish</b>\n\nYuboring:\n<code>Nomi|Ball</code>\nyoki\n<code>CODE|Nomi|Ball</code>",
        "bulk_prompt": "📥 <b>QRlarni ommaviy qo‘shish</b>\n\nBir xabarda ko‘p satr yuboring.\n\nFormatlar:\n<code>CODE</code>\n<code>CODE|Ball</code>\n<code>CODE|Nomi|Ball</code>",
        "qr_list_empty": "QR ro‘yxati bo‘sh.",
        "qr_list_title": "📋 <b>QR ro‘yxati</b>\n",
        "disable_prompt": "O‘chirish uchun <b>QR ID</b> yuboring.",
        "enable_prompt": "Yoqish uchun <b>QR ID</b> yuboring.",
        "delete_prompt": "O‘chirish uchun <b>QR ID</b> yuboring.",
        "png_prompt": "<b>QR ID</b> yuboring, PNG tayyorlanadi.",
        "add_points_prompt": "Yuboring:\n<code>user_id|ball</code>",
        "remove_points_prompt": "Yuboring:\n<code>user_id|ball</code>",
        "find_user_prompt": "<b>Foydalanuvchi ID</b> yuboring.",
        "stats_title": "📊 <b>Statistika</b>\n",
        "top_title": "🥇 <b>Top foydalanuvchilar</b>\n",
        "users_none": "Foydalanuvchilar hali yo‘q.",
        "cancelled": "Bekor qilindi.",
        "backed": "Asosiy menyuga qaytildi.",
        "bulk_done": "📥 <b>Bulk QR tugadi</b>",
        "bulk_created": "✅ Yaratildi: <b>{n}</b>",
        "bulk_skipped": "⚠️ O‘tkazib yuborildi: <b>{n}</b>",
        "bulk_created_title": "<b>Yaratilgan QRlar:</b>",
        "bulk_skipped_title": "<b>O‘tkazib yuborilgan satrlar:</b>",
        "wrong_format": "❌ Noto‘g‘ri format.",
        "code_exists": "❌ Bunday kod allaqachon mavjud.",
        "points_number": "❌ Ball son bo‘lishi va 0 dan katta bo‘lishi kerak.",
        "id_number": "❌ ID son bo‘lishi kerak.",
        "qr_missing": "❌ QR topilmadi.",
        "qr_disabled_ok": "⛔️ <b>{title}</b> QR o‘chirildi.",
        "qr_enabled_ok": "✅ <b>{title}</b> QR yoqildi.",
        "qr_deleted_ok": "🗑 <b>{title}</b> QR o‘chirildi.",
        "user_not_found": "❌ Foydalanuvchi topilmadi.",
        "add_points_ok": "✅ <code>{user_id}</code> foydalanuvchiga <b>+{added}</b> ball qo‘shildi\nBalans: <b>{points}</b>\nDaraja: <b>{level}</b>",
        "remove_points_ok": "➖ <code>{user_id}</code> foydalanuvchidan <b>{removed}</b> ball yechildi\nBalans: <b>{points}</b>\nDaraja: <b>{level}</b>",
        "notify_add": "🎁 Sizga <b>+{added}</b> ball qo‘shildi.\nBalans: <b>{points}</b>\nDaraja: <b>{level}</b>",
        "notify_remove": "➖ Sizdan <b>{removed}</b> ball yechildi.\nBalans: <b>{points}</b>\nDaraja: <b>{level}</b>",
        "find_user_title": "👤 <b>Foydalanuvchi</b>\n",
        "last_scans": "<b>Oxirgi skanlar:</b>",
        "history_none": "Tarix bo‘sh.",
        "export_users_done": "📤 users eksport tayyor.",
        "export_scans_done": "📤 scans eksport tayyor.",
        "myid": "Sizning Telegram ID: <code>{id}</code>\nAdmin access: <b>{admin}</b>\nADMIN_ID env: <code>{admin_id}</code>\nADMIN_IDS env: <code>{admin_ids}</code>",
        "lang_saved": "✅ Til saqlandi: O‘zbekcha"
    },
    "tj": {
        "choose_lang": "Выберите язык / Tilni tanlang / Забонро интихоб кунед / Choose language",
        "welcome": "✨ <b>Lux Aristokrat</b>\n\nСалом, {name}!\nБа системаи бонусӣ хуш омадед.\n\nДар ин ҷо шумо метавонед:\n• QR-кодҳоро скан кунед\n• баллҳои бонусӣ гиред\n• таърихро бинед\n• сатҳи худро назорат кунед\n{admin}\n\nТугмаи <b>«📷 QR-ро скан кардан»</b>-ро пахш кунед.",
        "admin_note": "\n\n<b>Шумо ҳамчун администратор ворид шудед.</b> Ба шумо боркунӣ ва идоракунии QR дастрас аст.",
        "my_points": "💎 Баланси шумо: <b>{points}</b>\n🏆 Сатҳи шумо: <b>{level}</b>",
        "my_level": "🏆 Сатҳи шумо: <b>{level}</b>\nБаллҳо: <b>{points}</b>\n\nСатҳҳо:\n• Silver: 0–299\n• Gold: 300–999\n• VIP: 1000+",
        "history_empty": "📜 Таърих ҳоло холӣ аст.",
        "history_title": "📜 <b>Таърихи шумо</b>\n",
        "how": "ℹ️ <b>Чӣ тавр балл гирифтан мумкин</b>\n\n1. Тугмаи «📷 QR-ро скан кардан»-ро пахш кунед\n2. Mini app бо камера кушода мешавад\n3. Камераро ба QR равона кунед\n4. Баллҳо худкор илова мешаванд\n\nЯк QR-код барои як корбар танҳо як бор ҳисоб карда мешавад.",
        "unknown_menu": "Аз меню амалро интихоб кунед.",
        "webapp_bad": "❌ Хатогии маълумоти WebApp.",
        "webapp_unknown": "⚠️ Амали номаълуми WebApp.",
        "bad_code": "❌ QR-коди холӣ ё нодуруст.",
        "qr_not_found": "❌ Чунин QR-код ёфт нашуд.",
        "qr_disabled": "⚠️ QR-коди <b>{title}</b> хомӯш аст.",
        "already_scanned": "⚠️ Шумо ин QR-кодро аллакай скан кардаед.\n\nQR: <b>{title}</b>\nБаланс: <b>{points}</b>\nСатҳ: <b>{level}</b>",
        "points_added": "✅ <b>Баллҳо илова шуданд!</b>\n\nQR: <b>{title}</b>\nКод: <code>{code}</code>\nИлова шуд: <b>+{added}</b>\nБаланс: <b>{points}</b>\nСатҳ: <b>{level}</b>",
        "add_qr_prompt": "➕ <b>Иловаи QR</b>\n\nФиристед:\n<code>Ном|Балл</code>\nё\n<code>CODE|Ном|Балл</code>",
        "bulk_prompt": "📥 <b>Иловаи гурӯҳии QR</b>\n\nЯк паём бо чанд сатр фиристед.\n\nФорматҳо:\n<code>CODE</code>\n<code>CODE|Балл</code>\n<code>CODE|Ном|Балл</code>",
        "qr_list_empty": "Рӯйхати QR холӣ аст.",
        "qr_list_title": "📋 <b>Рӯйхати QR</b>\n",
        "disable_prompt": "<b>ID QR</b>-ро барои хомӯш кардан фиристед.",
        "enable_prompt": "<b>ID QR</b>-ро барои фаъол кардан фиристед.",
        "delete_prompt": "<b>ID QR</b>-ро барои ҳазф кардан фиристед.",
        "png_prompt": "<b>ID QR</b>-ро фиристед, PNG сохта мешавад.",
        "add_points_prompt": "Фиристед:\n<code>user_id|балл</code>",
        "remove_points_prompt": "Фиристед:\n<code>user_id|балл</code>",
        "find_user_prompt": "<b>ID корбар</b>-ро фиристед.",
        "stats_title": "📊 <b>Омор</b>\n",
        "top_title": "🥇 <b>Беҳтарин корбарон</b>\n",
        "users_none": "Ҳоло корбарон нестанд.",
        "cancelled": "Бекор карда шуд.",
        "backed": "Бозгашт ба менюи асосӣ.",
        "bulk_done": "📥 <b>Bulk QR анҷом ёфт</b>",
        "bulk_created": "✅ Сохта шуд: <b>{n}</b>",
        "bulk_skipped": "⚠️ Гузаронида шуд: <b>{n}</b>",
        "bulk_created_title": "<b>QR-ҳои сохташуда:</b>",
        "bulk_skipped_title": "<b>Сатрҳои гузаронидашуда:</b>",
        "wrong_format": "❌ Формати нодуруст.",
        "code_exists": "❌ Чунин код аллакай ҳаст.",
        "points_number": "❌ Балл бояд рақам ва аз 0 зиёд бошад.",
        "id_number": "❌ ID бояд рақам бошад.",
        "qr_missing": "❌ QR ёфт нашуд.",
        "qr_disabled_ok": "⛔️ QR <b>{title}</b> хомӯш шуд.",
        "qr_enabled_ok": "✅ QR <b>{title}</b> фаъол шуд.",
        "qr_deleted_ok": "🗑 QR <b>{title}</b> ҳазф шуд.",
        "user_not_found": "❌ Корбар ёфт нашуд.",
        "add_points_ok": "✅ Ба корбар <code>{user_id}</code> <b>+{added}</b> балл илова шуд\nБаланс: <b>{points}</b>\nСатҳ: <b>{level}</b>",
        "remove_points_ok": "➖ Аз корбар <code>{user_id}</code> <b>{removed}</b> балл кам шуд\nБаланс: <b>{points}</b>\nСатҳ: <b>{level}</b>",
        "notify_add": "🎁 Ба шумо <b>+{added}</b> балл илова шуд.\nБаланс: <b>{points}</b>\nСатҳ: <b>{level}</b>",
        "notify_remove": "➖ Аз шумо <b>{removed}</b> балл кам шуд.\nБаланс: <b>{points}</b>\nСатҳ: <b>{level}</b>",
        "find_user_title": "👤 <b>Корбар</b>\n",
        "last_scans": "<b>Сканҳои охирин:</b>",
        "history_none": "Таърих холӣ аст.",
        "export_users_done": "📤 users экспорт тайёр.",
        "export_scans_done": "📤 scans экспорт тайёр.",
        "myid": "Telegram ID-и шумо: <code>{id}</code>\nAdmin access: <b>{admin}</b>\nADMIN_ID env: <code>{admin_id}</code>\nADMIN_IDS env: <code>{admin_ids}</code>",
        "lang_saved": "✅ Забон нигоҳ дошта шуд: Тоҷикӣ"
    },
    "en": {
        "choose_lang": "Выберите язык / Tilni tanlang / Забонро интихоб кунед / Choose language",
        "welcome": "✨ <b>Lux Aristokrat</b>\n\nHello, {name}!\nWelcome to the bonus system.\n\nHere you can:\n• scan QR codes\n• receive bonus points\n• view your history\n• track your level\n{admin}\n\nPress <b>“📷 Scan QR”</b>.",
        "admin_note": "\n\n<b>You are logged in as administrator.</b> QR upload and management are available.",
        "my_points": "💎 Your balance: <b>{points}</b>\n🏆 Your level: <b>{level}</b>",
        "my_level": "🏆 Your level: <b>{level}</b>\nPoints: <b>{points}</b>\n\nLevels:\n• Silver: 0–299\n• Gold: 300–999\n• VIP: 1000+",
        "history_empty": "📜 History is empty.",
        "history_title": "📜 <b>Your History</b>\n",
        "how": "ℹ️ <b>How to Get Points</b>\n\n1. Press “📷 Scan QR”\n2. A mini app with camera will open\n3. Point the camera at the code\n4. Points will be credited automatically\n\nThe same QR code is counted only once per user.",
        "unknown_menu": "Choose an action from the menu.",
        "webapp_bad": "❌ WebApp data error.",
        "webapp_unknown": "⚠️ Unknown WebApp action.",
        "bad_code": "❌ Empty or invalid QR code.",
        "qr_not_found": "❌ QR code not found.",
        "qr_disabled": "⚠️ QR code <b>{title}</b> is disabled.",
        "already_scanned": "⚠️ You have already scanned this QR code.\n\nQR: <b>{title}</b>\nBalance: <b>{points}</b>\nLevel: <b>{level}</b>",
        "points_added": "✅ <b>Points credited!</b>\n\nQR: <b>{title}</b>\nCode: <code>{code}</code>\nAdded: <b>+{added}</b>\nBalance: <b>{points}</b>\nLevel: <b>{level}</b>",
        "add_qr_prompt": "➕ <b>Add QR</b>\n\nSend:\n<code>Title|Points</code>\nor\n<code>CODE|Title|Points</code>",
        "bulk_prompt": "📥 <b>Bulk QR upload</b>\n\nSend multiple lines in one message.\n\nFormats:\n<code>CODE</code>\n<code>CODE|Points</code>\n<code>CODE|Title|Points</code>",
        "qr_list_empty": "QR list is empty.",
        "qr_list_title": "📋 <b>QR List</b>\n",
        "disable_prompt": "Send the <b>QR ID</b> to disable.",
        "enable_prompt": "Send the <b>QR ID</b> to enable.",
        "delete_prompt": "Send the <b>QR ID</b> to delete.",
        "png_prompt": "Send the <b>QR ID</b> to generate PNG.",
        "add_points_prompt": "Send:\n<code>user_id|points</code>",
        "remove_points_prompt": "Send:\n<code>user_id|points</code>",
        "find_user_prompt": "Send the <b>user ID</b>.",
        "stats_title": "📊 <b>Statistics</b>\n",
        "top_title": "🥇 <b>Top Users</b>\n",
        "users_none": "No users yet.",
        "cancelled": "Cancelled.",
        "backed": "Returned to main menu.",
        "bulk_done": "📥 <b>Bulk QR completed</b>",
        "bulk_created": "✅ Created: <b>{n}</b>",
        "bulk_skipped": "⚠️ Skipped: <b>{n}</b>",
        "bulk_created_title": "<b>Created QR codes:</b>",
        "bulk_skipped_title": "<b>Skipped lines:</b>",
        "wrong_format": "❌ Wrong format.",
        "code_exists": "❌ This code already exists.",
        "points_number": "❌ Points must be a number greater than 0.",
        "id_number": "❌ ID must be a number.",
        "qr_missing": "❌ QR not found.",
        "qr_disabled_ok": "⛔️ QR <b>{title}</b> disabled.",
        "qr_enabled_ok": "✅ QR <b>{title}</b> enabled.",
        "qr_deleted_ok": "🗑 QR <b>{title}</b> deleted.",
        "user_not_found": "❌ User not found.",
        "add_points_ok": "✅ Added <b>+{added}</b> points to user <code>{user_id}</code>\nBalance: <b>{points}</b>\nLevel: <b>{level}</b>",
        "remove_points_ok": "➖ Removed <b>{removed}</b> points from user <code>{user_id}</code>\nBalance: <b>{points}</b>\nLevel: <b>{level}</b>",
        "notify_add": "🎁 <b>+{added}</b> points have been added to your account.\nBalance: <b>{points}</b>\nLevel: <b>{level}</b>",
        "notify_remove": "➖ <b>{removed}</b> points have been removed from your account.\nBalance: <b>{points}</b>\nLevel: <b>{level}</b>",
        "find_user_title": "👤 <b>User</b>\n",
        "last_scans": "<b>Last scans:</b>",
        "history_none": "History is empty.",
        "export_users_done": "📤 users export is ready.",
        "export_scans_done": "📤 scans export is ready.",
        "myid": "Your Telegram ID: <code>{id}</code>\nAdmin access: <b>{admin}</b>\nADMIN_ID env: <code>{admin_id}</code>\nADMIN_IDS env: <code>{admin_ids}</code>",
        "lang_saved": "✅ Language saved: English"
    },
}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def esc(value) -> str:
    if value is None:
        return ""
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def normalize_code(raw: str) -> str:
    raw = (raw or "").strip().upper()
    raw = re.sub(r"[^A-Z0-9_-]+", "", raw)
    return raw


def normalize_bulk_code(raw: str) -> str:
    raw = (raw or "").strip()
    replacements = {
        ">": "_", "<": "_", "!": "_", "&": "_", "'": "_", "\"": "_",
        ".": "_", ",": "_", " ": "_", "/": "_", "\\": "_", ":": "_",
        ";": "_", "?": "_", "=": "_", "+": "_", "#": "_", "%": "_",
        "@": "_", "*": "_", "(": "_", ")": "_", "[": "_", "]": "_",
        "{": "_", "}": "_"
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


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    for row in rows:
        try:
            if row["name"] == column_name:
                return True
        except Exception:
            if len(row) > 1 and row[1] == column_name:
                return True
    return False


def add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str):
    if not column_exists(conn, table_name, column_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


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
            language TEXT DEFAULT 'ru',
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

    add_column_if_missing(conn, "users", "username", "TEXT")
    add_column_if_missing(conn, "users", "full_name", "TEXT")
    add_column_if_missing(conn, "users", "phone", "TEXT")
    add_column_if_missing(conn, "users", "points", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "users", "language", "TEXT DEFAULT 'ru'")
    add_column_if_missing(conn, "users", "created_at", "TEXT NOT NULL DEFAULT ''")
    add_column_if_missing(conn, "users", "updated_at", "TEXT NOT NULL DEFAULT ''")

    add_column_if_missing(conn, "qr_codes", "is_active", "INTEGER NOT NULL DEFAULT 1")
    add_column_if_missing(conn, "qr_codes", "created_by", "INTEGER")
    add_column_if_missing(conn, "qr_codes", "created_at", "TEXT NOT NULL DEFAULT ''")

    ts = now_str()
    cur.execute("UPDATE users SET language = 'ru' WHERE language IS NULL OR TRIM(language) = ''")
    cur.execute("UPDATE users SET points = 0 WHERE points IS NULL")
    cur.execute("UPDATE users SET created_at = ? WHERE created_at IS NULL OR TRIM(created_at) = ''", (ts,))
    cur.execute("UPDATE users SET updated_at = ? WHERE updated_at IS NULL OR TRIM(updated_at) = ''", (ts,))
    cur.execute("UPDATE qr_codes SET is_active = 1 WHERE is_active IS NULL")
    cur.execute("UPDATE qr_codes SET created_at = ? WHERE created_at IS NULL OR TRIM(created_at) = ''", (ts,))
    cur.execute("UPDATE scans SET scanned_at = ? WHERE scanned_at IS NULL OR TRIM(scanned_at) = ''", (ts,))

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
            INSERT INTO users (user_id, username, full_name, phone, points, language, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, 'ru', ?, ?)
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


def get_lang(user_id: int) -> str:
    row = get_user_by_id(user_id)
    if not row:
        return "ru"
    try:
        lang = row["language"]
    except (IndexError, KeyError, TypeError):
        return "ru"
    if lang in ("ru", "uz", "tj", "en"):
        return lang
    return "ru"


def set_lang(user_id: int, language: str):
    if language not in ("ru", "uz", "tj", "en"):
        language = "ru"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET language = ?, updated_at = ?
        WHERE user_id = ?
    """, (language, now_str(), user_id))
    conn.commit()
    conn.close()


def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_lang(user_id)
    text = UI.get(lang, UI["ru"]).get(key, "")
    return text.format(**kwargs)


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
    cur.execute("SELECT * FROM qr_codes ORDER BY id DESC LIMIT ?", (limit,))
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
    cur.execute("SELECT id FROM scans WHERE user_id = ? AND qr_code_id = ?", (user_id, qr_code_id))
    row = cur.fetchone()
    conn.close()
    return row is not None


def register_scan(user_id: int, qr_code_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scans (user_id, qr_code_id, scanned_at) VALUES (?, ?, ?)",
        (user_id, qr_code_id, now_str())
    )
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
    cur.execute("SELECT * FROM users ORDER BY points DESC, updated_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def export_users_csv() -> str:
    path = os.path.join(EXPORT_DIR, f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, username, full_name, phone, points, language, created_at, updated_at
        FROM users
        ORDER BY points DESC, updated_at DESC
    """)
    rows = cur.fetchall()
    conn.close()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["user_id", "username", "full_name", "phone", "points", "level", "language", "created_at", "updated_at"])
        for row in rows:
            points = int(row["points"])
            writer.writerow([
                row["user_id"], row["username"], row["full_name"], row["phone"],
                points, get_level(points), row["language"], row["created_at"], row["updated_at"]
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
                row["id"], row["user_id"], row["username"], row["full_name"],
                row["code"], row["title"], row["points"], row["scanned_at"]
            ])
    return path


def lang_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇷🇺 RU"), KeyboardButton(text="🇺🇿 UZ")],
            [KeyboardButton(text="🇹🇯 TJ"), KeyboardButton(text="🇬🇧 EN")],
        ],
        resize_keyboard=True
    )


def admin_mode_kb(user_id: int) -> ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BACK_TEXT[lang]), KeyboardButton(text=CANCEL_TEXT[lang])]
        ],
        resize_keyboard=True
    )


def main_kb(user_id: int) -> ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    rows = [[KeyboardButton(text=SCAN_TEXT[lang], web_app=WebAppInfo(url=WEBAPP_URL))]]

    if is_admin(user_id):
        rows.extend([
            [KeyboardButton(text=ADD_QR_TEXT[lang]), KeyboardButton(text=BULK_QR_TEXT[lang])],
            [KeyboardButton(text=LIST_QR_TEXT[lang]), KeyboardButton(text=PNG_QR_TEXT[lang])],
            [KeyboardButton(text=DISABLE_QR_TEXT[lang]), KeyboardButton(text=ENABLE_QR_TEXT[lang])],
            [KeyboardButton(text=DELETE_QR_TEXT[lang]), KeyboardButton(text=FIND_USER_TEXT[lang])],
            [KeyboardButton(text=ADD_POINTS_TEXT[lang]), KeyboardButton(text=REMOVE_POINTS_TEXT[lang])],
            [KeyboardButton(text=STATS_TEXT[lang]), KeyboardButton(text=TOP_TEXT[lang])],
            [KeyboardButton(text=EXPORT_USERS_TEXT[lang]), KeyboardButton(text=EXPORT_SCANS_TEXT[lang])],
        ])

    rows.extend([
        [KeyboardButton(text=POINTS_TEXT[lang]), KeyboardButton(text=HISTORY_TEXT[lang])],
        [KeyboardButton(text=LEVEL_TEXT[lang]), KeyboardButton(text=HOW_TEXT[lang])],
        [KeyboardButton(text="🌐 Language")],
    ])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def qr_link_kb(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔗 Open QR Link", url=build_qr_link(code))]]
    )


async def setup_menu_button():
    try:
        await bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="Сканер QR",
                web_app=WebAppInfo(url=WEBAPP_URL)
            )
        )
        logging.info("Menu button WebApp set successfully")
    except Exception as e:
        logging.warning("Menu button setup failed: %s", e)


@dp.message(CommandStart())
async def start_handler(message: Message, command: CommandStart):
    ensure_user_in_db(message.from_user)

    try:
        await bot.set_chat_menu_button(
            chat_id=message.chat.id,
            menu_button=MenuButtonWebApp(
                text="Сканер QR",
                web_app=WebAppInfo(url=WEBAPP_URL)
            )
        )
    except Exception as e:
        logging.warning("Per-chat menu button setup failed: %s", e)

    deep_arg = command.args
    if deep_arg and deep_arg.startswith("qr_"):
        code = normalize_code(deep_arg[3:])
        await process_qr_scan(message, code)
        return

    await message.answer(t(message.from_user.id, "choose_lang"), reply_markup=lang_kb())


@dp.message(F.text.in_(list(LANG_BUTTONS.keys())))
async def choose_language_handler(message: Message):
    ensure_user_in_db(message.from_user)
    language = LANG_BUTTONS[message.text]
    set_lang(message.from_user.id, language)
    admin_note = UI[language]["admin_note"] if is_admin(message.from_user.id) else ""
    welcome = UI[language]["welcome"].format(name=esc(message.from_user.full_name), admin=admin_note)
    await message.answer(UI[language]["lang_saved"])
    await message.answer(welcome, reply_markup=main_kb(message.from_user.id))


@dp.message(F.text == "🌐 Language")
async def language_change_handler(message: Message):
    ensure_user_in_db(message.from_user)
    await message.answer(t(message.from_user.id, "choose_lang"), reply_markup=lang_kb())


@dp.message(Command("myid"))
async def myid_handler(message: Message):
    ensure_user_in_db(message.from_user)
    await message.answer(
        t(
            message.from_user.id,
            "myid",
            id=message.from_user.id,
            admin="YES" if is_admin(message.from_user.id) else "NO",
            admin_id=ADMIN_ID,
            admin_ids=",".join(str(x) for x in sorted(ADMIN_IDS)) or "EMPTY"
        ),
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.web_app_data)
async def webapp_data_handler(message: Message):
    ensure_user_in_db(message.from_user)
    raw = message.web_app_data.data
    logging.info("WEB_APP_DATA RAW: %s", raw)

    try:
        data = json.loads(raw)
    except Exception:
        await message.answer(t(message.from_user.id, "webapp_bad"), reply_markup=main_kb(message.from_user.id))
        return

    action = str(data.get("action", "")).strip().lower()
    if action == "scan_qr":
        code = normalize_bulk_code(str(data.get("code", "")))
        if not code:
            await message.answer(t(message.from_user.id, "bad_code"), reply_markup=main_kb(message.from_user.id))
            return
        await process_qr_scan(message, code)
        return

    await message.answer(t(message.from_user.id, "webapp_unknown"), reply_markup=main_kb(message.from_user.id))


async def process_qr_scan(message: Message, code: str):
    ensure_user_in_db(message.from_user)
    qr_row = get_qr_by_code(code)

    if not qr_row:
        await message.answer(t(message.from_user.id, "qr_not_found"), reply_markup=main_kb(message.from_user.id))
        return

    if int(qr_row["is_active"]) != 1:
        await message.answer(
            t(message.from_user.id, "qr_disabled", title=esc(qr_row["title"])),
            reply_markup=main_kb(message.from_user.id)
        )
        return

    if has_scan(message.from_user.id, qr_row["id"]):
        points = get_user_points(message.from_user.id)
        await message.answer(
            t(
                message.from_user.id,
                "already_scanned",
                title=esc(qr_row["title"]),
                points=points,
                level=get_level(points)
            ),
            reply_markup=main_kb(message.from_user.id)
        )
        return

    register_scan(message.from_user.id, qr_row["id"])
    change_user_points(message.from_user.id, int(qr_row["points"]))
    new_points = get_user_points(message.from_user.id)
    level = get_level(new_points)

    await message.answer(
        t(
            message.from_user.id,
            "points_added",
            title=esc(qr_row["title"]),
            code=esc(qr_row["code"]),
            added=int(qr_row["points"]),
            points=new_points,
            level=level
        ),
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text.in_(set(POINTS_TEXT.values())))
async def my_points_handler(message: Message):
    ensure_user_in_db(message.from_user)
    points = get_user_points(message.from_user.id)
    await message.answer(
        t(message.from_user.id, "my_points", points=points, level=get_level(points)),
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text.in_(set(LEVEL_TEXT.values())))
async def my_level_handler(message: Message):
    ensure_user_in_db(message.from_user)
    points = get_user_points(message.from_user.id)
    await message.answer(
        t(message.from_user.id, "my_level", points=points, level=get_level(points)),
        reply_markup=main_kb(message.from_user.id)
    )


@dp.message(F.text.in_(set(HISTORY_TEXT.values())))
async def history_handler(message: Message):
    ensure_user_in_db(message.from_user)
    rows = get_user_history(message.from_user.id, 20)

    if not rows:
        await message.answer(t(message.from_user.id, "history_empty"), reply_markup=main_kb(message.from_user.id))
        return

    lines = [t(message.from_user.id, "history_title")]
    for row in rows:
        lines.append(
            f"• <b>{esc(row['title'])}</b>\n"
            f"  Код: <code>{esc(row['code'])}</code>\n"
            f"  Баллы: +{int(row['points'])}\n"
            f"  Дата: {esc(row['scanned_at'])}\n"
        )
    await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(HOW_TEXT.values())))
async def how_to_get_handler(message: Message):
    ensure_user_in_db(message.from_user)
    await message.answer(t(message.from_user.id, "how"), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(ADD_QR_TEXT.values())))
@dp.message(Command("add_qr"))
async def add_qr_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "add_qr"}
    await message.answer(t(message.from_user.id, "add_qr_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(BULK_QR_TEXT.values())))
@dp.message(Command("bulk_qr"))
async def bulk_qr_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "bulk_qr"}
    await message.answer(t(message.from_user.id, "bulk_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(LIST_QR_TEXT.values())))
@dp.message(Command("list_qr"))
async def list_qr_handler(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return

    rows = list_qr(50)
    if not rows:
        await message.answer(t(message.from_user.id, "qr_list_empty"), reply_markup=main_kb(message.from_user.id))
        return

    lines = [t(message.from_user.id, "qr_list_title")]
    for row in rows:
        status = "✅" if int(row["is_active"]) == 1 else "⛔️"
        lines.append(
            f"{status} ID: <code>{row['id']}</code>\n"
            f"Название: <b>{esc(row['title'])}</b>\n"
            f"Код: <code>{esc(row['code'])}</code>\n"
            f"Баллы: <b>{int(row['points'])}</b>\n"
        )
    await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(DISABLE_QR_TEXT.values())))
async def disable_qr_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "disable_qr"}
    await message.answer(t(message.from_user.id, "disable_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(ENABLE_QR_TEXT.values())))
async def enable_qr_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "enable_qr"}
    await message.answer(t(message.from_user.id, "enable_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(DELETE_QR_TEXT.values())))
async def delete_qr_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "delete_qr"}
    await message.answer(t(message.from_user.id, "delete_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(PNG_QR_TEXT.values())))
async def png_qr_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "make_png"}
    await message.answer(t(message.from_user.id, "png_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(ADD_POINTS_TEXT.values())))
async def add_points_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "add_points"}
    await message.answer(t(message.from_user.id, "add_points_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(REMOVE_POINTS_TEXT.values())))
async def remove_points_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "remove_points"}
    await message.answer(t(message.from_user.id, "remove_points_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(FIND_USER_TEXT.values())))
async def find_user_enter(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    admin_states[message.from_user.id] = {"mode": "find_user"}
    await message.answer(t(message.from_user.id, "find_user_prompt"), reply_markup=admin_mode_kb(message.from_user.id))


@dp.message(F.text.in_(set(STATS_TEXT.values())))
async def stats_handler(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return

    top = get_top_users(5)
    lines = [
        t(message.from_user.id, "stats_title"),
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
    await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(EXPORT_USERS_TEXT.values())))
async def export_users_handler(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    path = export_users_csv()
    await message.answer_document(FSInputFile(path), caption=t(message.from_user.id, "export_users_done"), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(EXPORT_SCANS_TEXT.values())))
async def export_scans_handler(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return
    path = export_scans_csv()
    await message.answer_document(FSInputFile(path), caption=t(message.from_user.id, "export_scans_done"), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(TOP_TEXT.values())))
async def top_users_handler(message: Message):
    ensure_user_in_db(message.from_user)
    if not is_admin(message.from_user.id):
        return

    rows = get_top_users(20)
    if not rows:
        await message.answer(t(message.from_user.id, "users_none"), reply_markup=main_kb(message.from_user.id))
        return

    lines = [t(message.from_user.id, "top_title")]
    for i, row in enumerate(rows, start=1):
        name = row["full_name"] or row["username"] or row["user_id"]
        points = int(row["points"])
        lines.append(
            f"{i}. {esc(name)}\n"
            f"   ID: <code>{row['user_id']}</code>\n"
            f"   Баллы: <b>{points}</b>\n"
            f"   Уровень: <b>{get_level(points)}</b>\n"
        )
    await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))


@dp.message(Command("cancel"))
async def cancel_handler(message: Message):
    ensure_user_in_db(message.from_user)
    admin_states.pop(message.from_user.id, None)
    await message.answer(t(message.from_user.id, "cancelled"), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(BACK_TEXT.values())))
async def back_handler(message: Message):
    ensure_user_in_db(message.from_user)
    admin_states.pop(message.from_user.id, None)
    await message.answer(t(message.from_user.id, "backed"), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text.in_(set(CANCEL_TEXT.values())))
async def cancel_button_handler(message: Message):
    ensure_user_in_db(message.from_user)
    admin_states.pop(message.from_user.id, None)
    await message.answer(t(message.from_user.id, "cancelled"), reply_markup=main_kb(message.from_user.id))


@dp.message(F.text)
async def text_router(message: Message):
    ensure_user_in_db(message.from_user)

    if not is_admin(message.from_user.id):
        await message.answer(t(message.from_user.id, "unknown_menu"), reply_markup=main_kb(message.from_user.id))
        return

    state = admin_states.get(message.from_user.id)
    if not state:
        await message.answer(t(message.from_user.id, "unknown_menu"), reply_markup=main_kb(message.from_user.id))
        return

    mode = state.get("mode")
    text = (message.text or "").strip()

    if mode == "bulk_qr":
        raw_lines = [line.strip() for line in text.splitlines() if line.strip()]
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
                    skipped.append(f"{idx}. {esc(line)}")
                    continue

                if not code or points <= 0:
                    skipped.append(f"{idx}. {esc(line)}")
                    continue

                create_qr(title=title, points=points, created_by=message.from_user.id, custom_code=code)
                created.append((code, title, points))
            except Exception:
                skipped.append(f"{idx}. {esc(line)}")

        admin_states.pop(message.from_user.id, None)

        lines = [
            t(message.from_user.id, "bulk_done"),
            "",
            t(message.from_user.id, "bulk_created", n=len(created)),
            t(message.from_user.id, "bulk_skipped", n=len(skipped))
        ]

        if created:
            lines.append("")
            lines.append(t(message.from_user.id, "bulk_created_title"))
            for i, (code, title, points) in enumerate(created[:20], start=1):
                lines.append(f"{i}. <code>{esc(code)}</code> — {esc(title)} — <b>{points}</b>")

        if skipped:
            lines.append("")
            lines.append(t(message.from_user.id, "bulk_skipped_title"))
            for item in skipped[:20]:
                lines.append(item)

        await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))
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
                await message.answer(t(message.from_user.id, "wrong_format"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            row = get_qr_by_code(code)
            png = save_qr_png(code)
            admin_states.pop(message.from_user.id, None)

            await message.answer_photo(
                photo=FSInputFile(png),
                caption=(
                    f"✅ <b>QR created</b>\n\n"
                    f"ID: <code>{row['id']}</code>\n"
                    f"Title: <b>{esc(row['title'])}</b>\n"
                    f"Code: <code>{esc(row['code'])}</code>\n"
                    f"Points: <b>{int(row['points'])}</b>\n"
                    f"Link:\n<code>{esc(build_qr_link(code))}</code>"
                ),
                reply_markup=qr_link_kb(code)
            )
            await message.answer("✅ Готово", reply_markup=main_kb(message.from_user.id))
            return
        except sqlite3.IntegrityError:
            await message.answer(t(message.from_user.id, "code_exists"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        except (ValueError, TypeError):
            await message.answer(t(message.from_user.id, "points_number"), reply_markup=admin_mode_kb(message.from_user.id))
            return

    if mode == "disable_qr":
        if not text.isdigit():
            await message.answer(t(message.from_user.id, "id_number"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer(t(message.from_user.id, "qr_missing"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        set_qr_active(int(text), False)
        admin_states.pop(message.from_user.id, None)
        await message.answer(t(message.from_user.id, "qr_disabled_ok", title=esc(row["title"])), reply_markup=main_kb(message.from_user.id))
        return

    if mode == "enable_qr":
        if not text.isdigit():
            await message.answer(t(message.from_user.id, "id_number"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer(t(message.from_user.id, "qr_missing"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        set_qr_active(int(text), True)
        admin_states.pop(message.from_user.id, None)
        await message.answer(t(message.from_user.id, "qr_enabled_ok", title=esc(row["title"])), reply_markup=main_kb(message.from_user.id))
        return

    if mode == "delete_qr":
        if not text.isdigit():
            await message.answer(t(message.from_user.id, "id_number"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer(t(message.from_user.id, "qr_missing"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        delete_qr(int(text))
        admin_states.pop(message.from_user.id, None)
        await message.answer(t(message.from_user.id, "qr_deleted_ok", title=esc(row["title"])), reply_markup=main_kb(message.from_user.id))
        return

    if mode == "make_png":
        if not text.isdigit():
            await message.answer(t(message.from_user.id, "id_number"), reply_markup=admin_mode_kb(message.from_user.id))
            return
        row = get_qr_by_id(int(text))
        if not row:
            await message.answer(t(message.from_user.id, "qr_missing"), reply_markup=admin_mode_kb(message.from_user.id))
            return

        png = save_qr_png(row["code"])
        admin_states.pop(message.from_user.id, None)

        await message.answer_photo(
            photo=FSInputFile(png),
            caption=(
                f"🧾 <b>QR PNG</b>\n\n"
                f"ID: <code>{row['id']}</code>\n"
                f"Title: <b>{esc(row['title'])}</b>\n"
                f"Code: <code>{esc(row['code'])}</code>\n"
                f"Points: <b>{int(row['points'])}</b>\n"
                f"Link:\n<code>{esc(build_qr_link(row['code']))}</code>"
            ),
            reply_markup=qr_link_kb(row["code"])
        )
        await message.answer("✅ Готово", reply_markup=main_kb(message.from_user.id))
        return

    if mode == "add_points":
        parts = [p.strip() for p in text.split("|")]
        try:
            if len(parts) != 2 or not parts[0].isdigit():
                await message.answer(t(message.from_user.id, "wrong_format"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            user_id = int(parts[0])
            points = int(parts[1])

            if points <= 0:
                await message.answer(t(message.from_user.id, "points_number"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            user_row = get_user_by_id(user_id)
            if not user_row:
                await message.answer(t(message.from_user.id, "user_not_found"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            change_user_points(user_id, points)
            new_points = get_user_points(user_id)
            admin_states.pop(message.from_user.id, None)

            await message.answer(
                t(message.from_user.id, "add_points_ok", user_id=user_id, added=points, points=new_points, level=get_level(new_points)),
                reply_markup=main_kb(message.from_user.id)
            )
            try:
                await bot.send_message(
                    user_id,
                    t(user_id, "notify_add", added=points, points=new_points, level=get_level(new_points))
                )
            except Exception:
                pass
            return
        except (ValueError, TypeError):
            await message.answer(t(message.from_user.id, "wrong_format"), reply_markup=admin_mode_kb(message.from_user.id))
            return

    if mode == "remove_points":
        parts = [p.strip() for p in text.split("|")]
        try:
            if len(parts) != 2 or not parts[0].isdigit():
                await message.answer(t(message.from_user.id, "wrong_format"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            user_id = int(parts[0])
            points = int(parts[1])

            if points <= 0:
                await message.answer(t(message.from_user.id, "points_number"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            user_row = get_user_by_id(user_id)
            if not user_row:
                await message.answer(t(message.from_user.id, "user_not_found"), reply_markup=admin_mode_kb(message.from_user.id))
                return

            change_user_points(user_id, -points)
            new_points = get_user_points(user_id)
            admin_states.pop(message.from_user.id, None)

            await message.answer(
                t(message.from_user.id, "remove_points_ok", user_id=user_id, removed=points, points=new_points, level=get_level(new_points)),
                reply_markup=main_kb(message.from_user.id)
            )
            try:
                await bot.send_message(
                    user_id,
                    t(user_id, "notify_remove", removed=points, points=new_points, level=get_level(new_points))
                )
            except Exception:
                pass
            return
        except (ValueError, TypeError):
            await message.answer(t(message.from_user.id, "wrong_format"), reply_markup=admin_mode_kb(message.from_user.id))
            return

    if mode == "find_user":
        if not text.isdigit():
            await message.answer(t(message.from_user.id, "id_number"), reply_markup=admin_mode_kb(message.from_user.id))
            return

        user_row = get_user_by_id(int(text))
        if not user_row:
            await message.answer(t(message.from_user.id, "user_not_found"), reply_markup=admin_mode_kb(message.from_user.id))
            return

        history = get_user_history(int(text), 5)
        points = int(user_row["points"])
        username_text = f"@{esc(user_row['username'])}" if user_row["username"] else "—"

        lines = [
            t(message.from_user.id, "find_user_title"),
            f"ID: <code>{user_row['user_id']}</code>",
            f"Username: {username_text}",
            f"Name: <b>{esc(user_row['full_name']) or '—'}</b>",
            f"Phone: <b>{esc(user_row['phone']) or '—'}</b>",
            f"Points: <b>{points}</b>",
            f"Level: <b>{get_level(points)}</b>",
            "",
            t(message.from_user.id, "last_scans")
        ]

        if history:
            for h in history:
                lines.append(f"• {esc(h['title'])} | +{int(h['points'])} | {esc(h['scanned_at'])}")
        else:
            lines.append(t(message.from_user.id, "history_none"))

        admin_states.pop(message.from_user.id, None)
        await message.answer("\n".join(lines), reply_markup=main_kb(message.from_user.id))
        return

    await message.answer(t(message.from_user.id, "unknown_menu"), reply_markup=main_kb(message.from_user.id))


async def main():
    init_db()
    await setup_menu_button()
    logging.info("Bot started successfully")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
