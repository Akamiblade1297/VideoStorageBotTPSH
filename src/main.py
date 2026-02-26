import asyncio
import os

import psycopg2

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

DB_USER = os.getenv('POSTGRES_RO_USER')
DB_PASS = os.getenv('POSTGRES_RO_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB      = os.getenv('POSTGRES_DB')

BOT_API = os.getenv('BOT_API') or '';

tg_bot = Bot(token=BOT_API)
tg_dp  = Dispatcher()

db_conn = psycopg2.connect(
    database=DB,
    host=DB_HOST,
    port=5432,
    user=DB_USER,
    password=DB_PASS
)
db_cur = db_conn.cursor()

@tg_dp.message()
async def message_handler(message: Message) -> None:
    db_cur.execute(message.text or '')
    await message.answer(str((db_cur.fetchone() or ('Не получилось распознать запрос',))[0]))

async def main():
    await tg_dp.start_polling(tg_bot)

if __name__ == "__main__":
    asyncio.run(main())
