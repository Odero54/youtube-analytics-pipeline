import os
import psycopg2
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Database credentials
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def test_table_exists():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'youtube_video_data'
            );
        """)
        exists = cursor.fetchone()[0]
        if exists:
            print("✅ Table 'youtube_video_data' exists.")
        else:
            print("❌ Table 'youtube_video_data' does NOT exist.")
    except Exception as e:
        print(f"❌ Error checking table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    test_table_exists()

