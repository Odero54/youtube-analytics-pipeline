import os
from googleapiclient.discovery import build

def get_youtube_service():
    api_key = os.getenv("YOUTUBE_API_KEY")
    return build('youtube', 'v3', developerKey=api_key)

def get_channel_videos(channel_id, max_results=50):
    service = get_youtube_service()
    req = service.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=max_results,
        order="date"
    )
    res = req.execute()
    video_ids = [item['id']['videoId'] for item in res['items'] if item['id']['kind'] == 'youtube#video']
    return video_ids
