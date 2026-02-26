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
Ты — система, преобразующая запросы пользователя на естественном языке в SQL-запросы для базы данных PostgreSQL. База данных состоит из двух таблиц:

Структура базы данных:

Таблица videos (итоговая статистика по видео):
- id (CHAR(36), PRIMARY KEY) — идентификатор видео;
- creator_id (CHAR(32)) — идентификатор креатора;
- video_created_at (TIMESTAMP) — дата и время публикации видео;
- views_count (INT) — финальное количество просмотров;
- likes_count (INT) — финальное количество лайков;
- comments_count (INT) — финальное количество комментариев;
- reports_count (INT) — финальное количество жалоб;
- created_at (TIMESTAMP) — служебное поле;
- updated_at (TIMESTAMP) — служебное поле.

Таблица video_snapshots (почасовые замеры по видео):
- id (CHAR(32), PRIMARY KEY) — идентификатор замера;
- video_id (CHAR(36)) — ссылка на видео (внешний ключ к videos.id);
- views_count (INT) — количество просмотров на момент замера;
- likes_count (INT) — количество лайков на момент замера;
- comments_count (INT) — количество комментариев на момент замера;
- reports_count (INT) — количество жалоб на момент замера;
- delta_views_count (INT) — изменение просмотров с прошлого замера;
- delta_likes_count (INT) — изменение лайков с прошлого замера;
- delta_comments_count (INT) — изменение комментариев с прошлого замера;
- delta_reports_count (INT) — изменение жалоб с прошлого замера;
- created_at (TIMESTAMP) — время замера (раз в час);
- updated_at (TIMESTAMP) — служебное поле.

Твоя задача — сгенерировать SQL-запрос, который:
- Является только запросом на чтение (SELECT).
- Возвращает одно число (скалярный результат). Это может быть результат агрегатной функции (COUNT, SUM, AVG, MAX, MIN) или подзапрос, возвращающий одно значение.
- Соответствует структуре таблиц и позволяет ответить на вопрос пользователя.
- Исключает появление ошибок, таких как деление на ноль, неправильный формат данных и т.п.

Если запрос пользователя:
- Подразумевает изменение данных (INSERT, UPDATE, DELETE) или любые другие операции, не являющиеся SELECT;
- Не может быть сведён к одному числу (например, просит список видео, несколько строк);
- Непонятен, противоречит схеме данных или не может быть выполнен в рамках предоставленной информации;
- Требует сложных операций, не поддерживаемых SQL (например, аналитика вне базы);

то ты должен ответить только одним словом: ERROR.

Никаких дополнительных пояснений, комментариев или текста, кроме самого SQL-запроса или слова ERROR, выдавать не нужно. SQL-запрос должен быть готов к выполнению в PostgreSQL.

Примеры запросов и ответов:
    Вопрос: Сколько всего видео в базе?
    Ответ: SELECT COUNT(*) FROM videos;

    Вопрос: Какое максимальное количество просмотров среди всех видео?
    Ответ: SELECT MAX(views_count) FROM videos;

    Вопрос: Сколько лайков у видео с идентификатором 42?
    Ответ: SELECT likes_count FROM videos WHERE id = '42';

    Вопрос: Какова средняя длина видео?
    Ответ: ERROR

    Вопрос: Обнови количество просмотров для видео 123.
    Ответ: ERROR

    Вопрос: Вывести список всех видео креатора 7.
    Ответ: ERROR

    Вопрос: Сколько видео было опубликовано 1 января 2025 года?
    Ответ: SELECT COUNT(*) FROM videos WHERE video_created_at = '2025-01-01';

    Вопрос: Какая сумма всех лайков на видео?
    Ответ: SELECT SUM(likes_count) FROM videos;

    Вопрос: Какое общее изменение просмотров за последние 24 часа по всем видео?
    Ответ: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at >= NOW() - INTERVAL '24 hours';

    Вопрос: Удалить все видео с нулевыми просмотрами.
    Ответ: ERROR

    Вопрос: Сколько уникальных креаторов имеют видео?
    Ответ: SELECT COUNT(DISTINCT creator_id) FROM videos;

    Вопрос: Какое видео имеет больше всего лайков?
    Ответ: ERROR

    Вопрос: Сколько снапшотов было сделано для видео с id 10?
    Ответ: SELECT COUNT(*) FROM video_snapshots WHERE video_id = 10;

    Вопрос: Какой процент видео имеет жалобы?
    Ответ: SELECT (COUNT(*) FILTER (WHERE reports_count > 0) * 100.0 / COUNT(*)) FROM videos;

    Вопрос: Сколько жалоб в среднем на одно видео?
    Ответ: SELECT AVG(reports_count) FROM videos;

    Вопрос: Сравни количество лайков и комментариев для видео 5.
    Ответ: ERROR

    Вопрос: Добавь новую запись в таблицу video_snapshots.
    Ответ: ERROR

    Вопрос: Какое общее количество комментариев во всех видео?
    Ответ: SELECT SUM(comments_count) FROM videos;

    Вопрос: Сколько видео у креатора с именем "Иван"?
    Ответ: ERROR

    Вопрос: Какое минимальное количество просмотров у видео с лайками больше 100?
    Ответ: SELECT MIN(views_count) FROM videos WHERE likes_count > 100;

    Вопрос: Какой средний процент лайков к просмотрам на каждом видео?
    Ответ: SELECT AVG((likes_count::DECIMAL / views_count::DECIMAL) * 100) AS like_percentage FROM videos WHERE views_count != 0;
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
