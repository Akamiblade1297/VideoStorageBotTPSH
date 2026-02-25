import psycopg2
import json
import os

from psycopg2 import sql

DB   = os.getenv("POSTGRES_DB") or "postgres"
USER = os.getenv("POSTGRES_USER")
PASS = os.getenv("POSTGRES_PASSWORD")
HOST = os.getenv("POSTGRES_HOST")

conn = psycopg2.connect(
    database=DB,
    host=HOST,
    port=5432,
    user=USER,
    password=PASS
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS videos (
    id CHAR(36) PRIMARY KEY,
    creator_id CHAR(32),
    video_created_at TIMESTAMP,
    views_count INT,
    likes_count INT,
    comments_count INT,
    reports_count INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS video_snapshots (
    id CHAR(32) PRIMARY KEY,
    video_id CHAR(36),
    views_count INT,
    likes_count INT,
    comments_count INT,
    reports_count INT,
    delta_views_count INT,
    delta_likes_count INT,
    delta_comments_count INT,
    delta_reports_count INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);""")

conn.commit()

with open('db_init.json') as file:
    data = json.load(file)

    cur.execute("SELECT * FROM videos WHERE id = %s", (data['videos'][0]['id'],) )
    if cur.fetchone() != None:
        for video in data['videos']:
            cur.execute("""
                INSERT INTO videos (
                    id, creator_id, video_created_at,
                    views_count, likes_count, comments_count, reports_count,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING""", (
                    video['id'],
                    video['creator_id'],
                    video['video_created_at'],
                    video['views_count'],
                    video['likes_count'],
                    video['comments_count'],
                    video['reports_count'],
                    video['created_at'],
                    video['updated_at']
                )
            )
            
            for snapshot in video['snapshots']:
                cur.execute("""
                    INSERT INTO video_snapshots (
                        id, video_id, views_count, likes_count, comments_count, reports_count,
                        delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""", (
                    snapshot['id'],
                    snapshot['video_id'],
                    snapshot['views_count'],
                    snapshot['likes_count'],
                    snapshot['comments_count'],
                    snapshot['reports_count'],
                    snapshot['delta_views_count'],
                    snapshot['delta_likes_count'],
                    snapshot['delta_comments_count'],
                    snapshot['delta_reports_count'],
                    snapshot['created_at'],
                    snapshot['updated_at']
                    )
                )

        conn.commit()

RO_USER = os.getenv("POSTGRES_RO_USER") or "rouser"
RO_PASS = os.getenv("POSTGRES_RO_PASSWORD")

cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (RO_USER,))
exists = cur.fetchone()
if not exists:
    cur.execute(
        sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
            sql.Identifier(RO_USER)
        ),
        (RO_PASS,)
    )

cur.execute(
    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
        sql.Identifier(DB),
        sql.Identifier(RO_USER)
    )
)

# 3. Grant USAGE on the schema (usually 'public')
cur.execute(
    sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
        sql.Identifier(RO_USER)
    )
)

# 4. Grant SELECT on all existing tables in the schema
cur.execute(
    sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(
        sql.Identifier(RO_USER)
    )
)

# 5. Set default privileges for future tables created by any role
#    (If you know which role will create tables, you can specify it with 'FOR USER')
cur.execute(
    sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}").format(
        sql.Identifier(RO_USER)
    )
)

# 6. (Optional) Force read-only transactions for this user
cur.execute(
    sql.SQL("ALTER USER {} SET default_transaction_read_only = on").format(
        sql.Identifier(RO_USER)
    )
)

conn.commit()
conn.close()
