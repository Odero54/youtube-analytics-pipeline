CREATE TABLE IF NOT EXISTS youtube_video_metrics (
  video_id TEXT PRIMARY KEY,
  title TEXT,
  published_at TIMESTAMP,
  views INTEGER,
  likes INTEGER,
  comments INTEGER,
  engagement_rate FLOAT
);
