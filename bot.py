import asyncio
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command

from config import BOT_TOKEN
from services import get_or_create_user

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Сканировать QR")],
        [KeyboardButton(text="Мои баллы")]
    ],
    resize_keyboard=True
)

@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    get_or_create_user(user_id)

    await message.answer(
        "Добро пожаловать 👋\n"
        "Нажми 'Сканировать QR' чтобы получить баллы",
        reply_markup=kb
    )

@dp.message(F.text == "Мои баллы")
async def points(message: Message):
    user_id = message.from_user.id
    points = get_or_create_user(user_id)

    await message.answer(f"Ваш баланс: {points} баллов")

@dp.message(F.text == "Сканировать QR")
async def scan(message: Message):
    await message.answer("⚠️ Сканер подключим на следующем этапе")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
