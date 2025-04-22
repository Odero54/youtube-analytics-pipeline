import os
import requests
from dotenv import load_dotenv
from datetime import datetime
import json

# Load .env
load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")
BASE_URL = "https://www.googleapis.com/youtube/v3"

def get_uploads_playlist_id():
    url = f"{BASE_URL}/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}"
    response = requests.get(url).json()
    return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

def get_all_video_ids(playlist_id):
    video_ids = []
    next_page_token = None

    while True:
        url = f"{BASE_URL}/playlistItems?part=contentDetails&playlistId={playlist_id}&maxResults=50&key={API_KEY}"
        if next_page_token:
            url += f"&pageToken={next_page_token}"
        
        res = requests.get(url).json()
        for item in res["items"]:
            video_ids.append(item["contentDetails"]["videoId"])
        
        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids

def get_video_details(video_ids):
    videos_data = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        ids_str = ",".join(chunk)
        url = f"{BASE_URL}/videos?part=snippet,statistics,contentDetails&id={ids_str}&key={API_KEY}"
        response = requests.get(url).json()
        videos_data.extend(response["items"])
    return videos_data

def save_raw_data(videos_data, filename=f"/home/george/data_engineering/youtube-analytics-pipeline/data/raw/raw_youtube_data.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(videos_data, f, indent=2)
    print(f"✅ Raw data saved to {filename}")

if __name__ == "__main__":
    try:
        playlist_id = get_uploads_playlist_id()
        print("📋 Uploads Playlist ID fetched.")

        video_ids = get_all_video_ids(playlist_id)
        print(f"🎞️ Total videos found: {len(video_ids)}")

        video_data = get_video_details(video_ids)
        print(f"📦 Fetched metadata for {len(video_data)} videos.")

        save_raw_data(video_data)
    except Exception as e:
        print(f"❌ Error fetching data: {e}")

