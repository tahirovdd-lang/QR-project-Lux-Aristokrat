import asyncio
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO)


def ensure_package(package: str):
    try:
        __import__(package)
        logging.info("Package %s already installed", package)
    except ModuleNotFoundError:
        logging.warning("Package %s not found. Installing...", package)
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "--no-cache-dir", "aiogram==3.22.0"
        ])


ensure_package("aiogram")

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found")

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()


@dp.message(CommandStart())
async def start_handler(message: Message):
    await message.answer("✅ Бот Lux Aristokrat работает")


async def main():
    logging.info("Bot started successfully")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
