import psycopg2
import json
import os

DB   = os.getenv("POSTGRES_DB")
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

conn.close()
