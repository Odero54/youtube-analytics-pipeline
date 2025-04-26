import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging
from urllib.parse import quote_plus
os.environ['SQLALCHEMY_SILENCE_UBER_WARNING'] = '1'

# ========== SETUP ==========
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load DB credentials
PARQUET_PATH = os.getenv("PARQUET_PATH")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")

encoded_password = quote_plus(DB_PASSWORD)

# Debug line
print(f"📁 Parquet path being used: {PARQUET_PATH}")

# ========== MAIN ==========
def main():
    logging.info("🔄 Loading transformed YouTube data into PostgreSQL...")

    try:
        # Load Parquet file
        df = pd.read_parquet(PARQUET_PATH)
        if df.empty:
            logging.warning("⚠️ Parquet file is empty. Exiting.")
            return

        # Create engine (modern SQLAlchemy 2.0 style)
        db_url = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db_url)

        # Get unique timestamps
        fetched_at_values = df["fetched_at"].dropna().unique()
        
        for fetched_at in fetched_at_values:
            # Convert timestamp to string for comparison
            fetched_at_str = str(fetched_at)
            
            # Check for existing data (modern SQLAlchemy 2.0 way)
            with engine.connect() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM youtube_video_data WHERE fetched_at = :fetched_at LIMIT 1"),
                    {"fetched_at": fetched_at_str}
                ).scalar() is not None

            if exists:
                logging.info(f"⏭️ Skipping existing data for {fetched_at_str}")
                continue

            # Filter and load data
            batch_df = df[df["fetched_at"] == fetched_at]
            
            # THE KEY FIX: Use engine directly for to_sql()
            batch_df.to_sql(
                name="youtube_video_data",
                con=engine,  # This is the critical change
                if_exists="append",
                index=False,
                method="multi"
            )
            logging.info(f"✅ Loaded {len(batch_df)} records for {fetched_at_str}")

    except FileNotFoundError:
        logging.error(f"❌ Parquet file not found: {PARQUET_PATH}")
    except SQLAlchemyError as e:
        logging.error(f"❌ Database error: {e}")
    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}")

# ========== ENTRY POINT ==========
if __name__ == "__main__":
    main()
