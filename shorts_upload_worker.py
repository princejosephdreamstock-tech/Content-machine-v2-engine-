import psycopg2
import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

DB_URL = os.environ["PIPELINE_DB_URL"]
CHANNELS_DIR = os.path.expanduser("~/channels")

DEFAULT_GUIDE_LINK = "https://dreamprince.gumroad.com/l/YouTubeautomation"
DEFAULT_CTA_TEXT = "Get the Content Machine guide"
DEFAULT_TAGS = [
    "youtube automation", "passive income", "faceless channel", "content automation",
    "youtube automation ai", "ai youtube automation", "n8n youtube automation",
    "youtube automation 2026", "free youtube automation", "start youtube automation",
    "youtube automation guide", "youtube automation niche", "what is youtube automation",
    "youtube automation course", "youtube automation with ai", "youtube automation channel",
    "faceless youtube channel"
]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT run_id, video_path, script, channel_id FROM shorts_runs
    WHERE status = 'UPLOAD_PENDING'
    LIMIT 1
""")
result = cur.fetchone()
if not result:
    print("No pending uploads")
    conn.close()
    exit()

run_id, video_path, script, channel_id = result

cur.execute("SELECT config_json FROM channels WHERE channel_id = %s", (channel_id,))
cfg_row = cur.fetchone()
cfg = cfg_row[0] if cfg_row and cfg_row[0] else {}

guide_link = cfg.get("guide_link") or DEFAULT_GUIDE_LINK
cta_text = cfg.get("cta_text") or DEFAULT_CTA_TEXT
tags = cfg.get("fixed_tags") or DEFAULT_TAGS
print(f"Channel: {channel_id}, guide_link: {guide_link}")

token_path = os.path.join(CHANNELS_DIR, channel_id, "token.json")
creds = Credentials.from_authorized_user_file(token_path)
youtube = build("youtube", "v3", credentials=creds)

from pipeline_common import call_nvidia_with_rotation
import json as _json

def generate_seo_metadata(script, guide_link, cta_text):
    prompt = f"""Based on this video script, write a YouTube title and description optimized for SEO and click-through.

SCRIPT:
{script}

Return ONLY valid JSON, no preamble, no markdown fences:
{{"title": "...", "description": "..."}}

Title: under 100 characters, curiosity-driven, no clickbait lies, must reflect the actual script content.
Description: 2-3 sentences summarizing the video's actual hook/value, then include exactly this line on its own: "{cta_text}: {guide_link}" then "Full setup guide in comments below."
"""
    def _parse(raw):
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.startswith("json"):
                raw = raw[4:].strip()
        data = _json.loads(raw)
        return data["title"], data["description"]

    primary_payload = {
        "model": "nvidia/nemotron-3.5-lightning-30b-a3b",
        "chat_template_kwargs": {"enable_thinking": False},
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 400,
        "temperature": 0.8
    }
    try:
        resp = call_nvidia_with_rotation("https://integrate.api.nvidia.com/v1/chat/completions", primary_payload, timeout=60, dedup_key=None)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        return _parse(raw)
    except Exception as e:
        print(f"[fallback] primary model failed ({e}), trying minimax-m3")
        fallback_payload = {
            "model": "minimaxai/minimax-m3",
            "chat_template_kwargs": {"thinking_mode": "disabled"},
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 400,
            "temperature": 0.8
        }
        resp = call_nvidia_with_rotation("https://integrate.api.nvidia.com/v1/chat/completions", fallback_payload, timeout=60, dedup_key=None)
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        return _parse(raw)

try:
    title, description = generate_seo_metadata(script, guide_link, cta_text)
except Exception as e:
    print(f"SEO metadata generation failed, falling back to default: {e}")
    title = "YouTube Automation Done Right"
    description = f"{cta_text}: {guide_link}\n\nFull setup guide in comments below."

body = {
    "snippet": {
        "title": title,
        "description": description,
        "tags": tags,
        "categoryId": "22"
    },
    "status": {"privacyStatus": "public"}
}

try:
    media = MediaFileUpload(video_path, chunksize=-1, resumable=True, mimetype="video/mp4")
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)

    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"Upload progress: {int(status.progress() * 100)}%")

    video_id = response["id"]

    comment_text = f"\U0001f4d6 Full setup guide & complete blueprint:\n{guide_link}\n\nCopy & paste if link isn't clickable above"
    comment_body = {
        "snippet": {
            "videoId": video_id,
            "topLevelComment": {
                "snippet": {
                    "textDisplay": comment_text,
                    "textOriginal": comment_text
                }
            }
        }
    }
    try:
        youtube.commentThreads().insert(part="snippet", body=comment_body).execute()
        print("Comment added")
    except Exception as comment_e:
        print(f"Comment posting FAILED (non-fatal): {comment_e}")

    cur.execute("""
        UPDATE shorts_runs
        SET youtube_video_id = %s, status = 'UPLOADED', updated_at = now()
        WHERE run_id = %s
    """, (video_id, run_id))
    conn.commit()
    print(f"\u2713 LIVE: https://youtu.be/{video_id}")
    cur.close()
    conn.close()
except HttpError as e:
    if "quotaExceeded" in str(e) or "uploadLimitExceeded" in str(e):
        print(f"  run {run_id} hit YouTube limit, leaving as UPLOAD_PENDING for next pass")
        conn.rollback()
        conn.close()
        exit()
    cur.execute("UPDATE shorts_runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e)[:1000], run_id))
    conn.commit()
    print(f"  run {run_id} FAILED: {e}")
    conn.close()
    exit()
except Exception as e:
    cur.execute("UPDATE shorts_runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e)[:1000], run_id))
    conn.commit()
    print(f"  run {run_id} FAILED: {e}")
    conn.close()
    exit()
