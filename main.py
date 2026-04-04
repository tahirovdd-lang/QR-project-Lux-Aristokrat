import os
import logging

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("Переменная BOT_TOKEN не найдена в .env")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer(
        "✅ Бот успешно запущен\n\n"
        "Команды:\n"
        "/start — запуск\n"
        "/help — помощь\n"
        "/id — твой Telegram ID"
    )


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.answer(
        "ℹ️ Это тестовый бот на aiogram 2.25.1\n"
        "Если хочешь — дальше можно вставить твой основной код."
    )


@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")


@dp.message_handler(content_types=types.ContentType.TEXT)
async def echo_message(message: types.Message):
    await message.answer(f"Вы написали:\n<code>{message.text}</code>")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
