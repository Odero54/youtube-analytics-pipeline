import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Read credential from .env
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# SQL to create the table
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS youtube_video_data (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    description TEXT,
    published_at TIMESTAMP,
    view_count INTEGER,
    like_count INTEGER,
    comment_count INTEGER,
    duration INTERVAL,
    tags TEXT[],
    category_id TEXT,
    channel_title TEXT,
    engagement_rate FLOAT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def create_table():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("✅ Table 'youtube_video_data' created successfully (or already exists).")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_table()
