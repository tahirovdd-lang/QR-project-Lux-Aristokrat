print("=== NEW MAIN.PY VERSION 2 ===")

import asyncio
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO)

try:
    from aiogram import Bot, Dispatcher
    from aiogram.client.default import DefaultBotProperties
    from aiogram.filters import CommandStart
    from aiogram.types import Message
except ModuleNotFoundError:
    print("=== AIROGRAM NOT FOUND, INSTALLING... ===")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "--no-cache-dir", "aiogram==3.22.0"
    ])
    from aiogram import Bot, Dispatcher
    from aiogram.client.default import DefaultBotProperties
    from aiogram.filters import CommandStart
    from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
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
