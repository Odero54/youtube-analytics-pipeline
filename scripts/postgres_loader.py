import psycopg2
import pandas as pd

def load_to_postgres(csv_file, db_config):
    df = pd.read_csv(csv_file)
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO youtube_video_metrics (video_id, title, published_at, views, likes, comments, engagement_rate)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO NOTHING;
        """, tuple(row))
    conn.commit()
    cursor.close()
    conn.close()

