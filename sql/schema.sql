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
