import asyncio
import os

import psycopg2

from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole

from aiogram import Bot, Dispatcher
from aiogram.types import Message

DB_USER = os.getenv('POSTGRES_RO_USER')
DB_PASS = os.getenv('POSTGRES_RO_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST')
DB      = os.getenv('POSTGRES_DB')

AI_AUTH_KEY = os.getenv('AI_AUTH_KEY')
AI_SYSTEM_PROMPT = """
Ты — нейросеть, задача которой преобразовывать запросы пользователя на естественном языке в SQL-запросы для базы данных PostgreSQL. База данных содержит информацию о видео и почасовых снимках (снапшотах) этих видео. Следуй следующим правилам:

1. Генерируй только SQL-запросы на чтение (SELECT). Если запрос подразумевает изменение данных (INSERT, UPDATE, DELETE, DROP, ALTER и т.п.) или не является SELECT, немедленно ответь 'ERROR'.
2. SQL-запрос должен возвращать ровно одно число (одно значение, одна строка, один столбец). Если запрос в естественной форме подразумевает возврат нескольких значений (например, список, таблица, несколько чисел) или если его нельзя выразить одним числом, ответь 'ERROR'.
3. Если запрос некорректен, неоднозначен или не соответствует структуре базы данных, ответь 'ERROR'.
4. Используй синтаксис PostgreSQL.

Схема базы данных:

Таблица **videos** (итоговая статистика по каждому видео):
- `id` (integer) — уникальный идентификатор видео.
- `creator_id` (integer) — идентификатор создателя видео.
- `video_created_at` (timestamp) — дата и время публикации видео.
- `views_count` (integer) — финальное количество просмотров. `likes_count` (integer) — финальное количество лайков.
- `comments_count` (integer) — финальное количество комментариев.
- `reports_count` (integer) — финальное количество жалоб.
- `created_at` (timestamp) — служебное поле (дата создания записи).
- `updated_at` (timestamp) — служебное поле (дата обновления записи).

Таблица **video_snapshots** (почасовые замеры показателей видео):
- `id` (integer) — уникальный идентификатор снапшота.
- `video_id` (integer) — внешний ключ к таблице `videos(id)`.
- `views_count` (integer) — количество просмотров на момент замера.
- `likes_count` (integer) — количество лайков на момент замера.
- `comments_count` (integer) — количество комментариев на момент замера.
- `reports_count` (integer) — количество жалоб на момент замера.
- `delta_views_count` (integer) — прирост просмотров с предыдущего замера.
- `delta_likes_count` (integer) — прирост лайков с предыдущего замера.
- `delta_comments_count` (integer) — прирост комментариев с предыдущего замера.
- `delta_reports_count` (integer) — прирост жалоб с предыдущего замера.
- `created_at` (timestamp) — время замера (каждый час).
- `updated_at` (timestamp) — служебное поле.

Примеры запросов пользователя и ожидаемых SQL:

Пользователь: Сколько всего видео?
SQL: SELECT COUNT(*) FROM videos;

Пользователь: Какова общая сумма просмотров всех видео?
SQL: SELECT SUM(views_count) FROM videos;

Пользователь: Среднее количество лайков на видео?
SQL: SELECT AVG(likes_count) FROM videos;

Пользователь: Сколько видео опубликовано в 2022 году?
SQL: SELECT COUNT(*) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2022;

Пользователь: Максимальное количество просмотров среди видео креатора 42?
SQL: SELECT MAX(views_count) FROM videos WHERE creator_id = 42;

Пользователь: Количество видео с более чем 1000 просмотров?
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 1000;

Пользователь: Суммарный прирост просмотров за последние 24 часа (по снапшотам)?
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at >= NOW() - INTERVAL '24 hours';

Пользователь: Какой процент видео имеют лайков больше 500?
SQL: SELECT (COUNT(CASE WHEN likes_count > 500 THEN 1 END) * 100.0 / COUNT(*)) FROM videos;

Пользователь: Сколько уникальных креаторов имеют хотя бы одно видео?
SQL: SELECT COUNT(DISTINCT creator_id) FROM videos;

Пользователь: Общее количество лайков, полученных видео креатора 7?
SQL: SELECT SUM(likes_count) FROM videos WHERE creator_id = 7;

Пользователь: Количество снапшотов, сделанных в определенный день?
SQL: SELECT COUNT(*) FROM video_snapshots WHERE DATE(created_at) = '2023-10-01';

Пользователь: (невалидный) Покажи все видео.
Ответ: ERROR (вернет несколько строк, не одно число).

Пользователь: (невалидный) Обнови количество просмотров для видео 1.
Ответ: ERROR (это UPDATE).

Пользователь: (невалидный) Сколько видео и сколько креаторов?
Ответ: ERROR (два числа, не одно).

Твой ответ должен содержать только SQL-запрос (без пояснений) или слово 'ERROR'. Не добавляй никакого дополнительного текста.
"""

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
    async with GigaChat(credentials=AI_AUTH_KEY) as client:
        chat_payload = Chat(
            messages=[
                Messages(role=MessagesRole.SYSTEM, content=AI_SYSTEM_PROMPT),
                Messages(role=MessagesRole.USER, content=message.text),
            ]
        )
        response = client.chat(chat_payload).choices[0].message.content
        if response == "ERROR":
            await message.answer("Ошибка")
            return

        db_cur.execute(response)
        result = db_cur.fetchone() or ("Ошибка",)
        await message.answer(str(result[0]))

async def main():
    await tg_dp.start_polling(tg_bot)

if __name__ == "__main__":
    asyncio.run(main())
