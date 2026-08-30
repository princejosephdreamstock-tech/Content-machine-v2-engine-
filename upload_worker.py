import os
import json
import datetime
import requests
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from engine_common import retry_with_backoff, push_failed_job, push_job, notify_discord, get_db
import random
import socket
socket.setdefaulttimeout(120)  # prevent indefinite hangs on any blocking network call

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
STATE_FILE = os.path.expanduser("~/.q_upload_lastid")
CHANNELS_DIR = os.path.expanduser("~/channels")
DAILY_UPLOAD_LIMIT = 6

CATEGORY_MAP = {"Howto & Style": "26", "Pets & Animals": "15", "Education": "27"}


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.upload/({last_id}/+", headers=headers, timeout=30)
    return r.json().get("result", [])


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def get_youtube_client(channel_id):
    token_path = os.path.join(CHANNELS_DIR, channel_id, "token.json")
    creds = Credentials.from_authorized_user_file(token_path)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    return build("youtube", "v3", credentials=creds)


def upload_video(youtube, video_path, metadata, scheduled_for_time=None):
    category_id = CATEGORY_MAP.get(metadata.get("category"), "26")
    status_obj = {"privacyStatus": "private"}

    if scheduled_for_time:
        if scheduled_for_time.tzinfo:
            utc_time = scheduled_for_time.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        else:
            utc_time = scheduled_for_time
        status_obj["publishAt"] = utc_time.isoformat() + "Z"

    body = {
        "snippet": {
            "title": metadata["title"],
            "description": metadata["description"],
            "tags": metadata.get("tags", []),
            "categoryId": category_id,
        },
        "status": status_obj
    }
    media = MediaFileUpload(video_path, chunksize=-1, resumable=True, mimetype="video/mp4")
    request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"    upload progress: {int(status.progress() * 100)}%")
    return response["id"]


def set_thumbnail(youtube, video_id, thumbnail_path):
    youtube.thumbnails().set(
        videoId=video_id,
        media_body=MediaFileUpload(thumbnail_path, mimetype="image/jpeg")
    ).execute()


def check_and_reserve_quota_db(cur, channel_id, daily_cap=6):
    cur.execute(
        "SELECT COUNT(*) FROM runs WHERE channel_id = %s AND status IN ('UPLOADED', 'BACKED_UP') "
        "AND updated_at >= date_trunc('day', now())",
        (channel_id,)
    )
    count = cur.fetchone()[0]
    return count < daily_cap


def compute_next_publish_time(cur, channel_id):
    cur.execute(
        "SELECT MAX(scheduled_for_time) FROM runs WHERE channel_id = %s AND scheduled_for_time > now()",
        (channel_id,)
    )
    last_slot = cur.fetchone()[0]
    base = last_slot if last_slot else datetime.datetime.now(datetime.timezone.utc)
    gap_hours = random.uniform(20, 28)
    return base + datetime.timedelta(hours=gap_hours)


class QuotaExceeded(Exception):
    def __init__(self, channel_id):
        self.channel_id = channel_id


def process_job(run_id, channel_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT video_path, thumbnail_path, metadata_json, youtube_video_id, scheduled_for_time FROM runs WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row is None or not row[0] or not row[2]:
        print(f"  run {run_id} missing video_path/metadata_json, skipping")
        conn.rollback()
        return

    video_path, thumbnail_path, metadata, existing_video_id, scheduled_for_time = row

    if scheduled_for_time:
        cur.execute("SELECT now()")
        now = cur.fetchone()[0]
        if now.tzinfo is None:
            now = now.replace(tzinfo=datetime.timezone.utc)
        if scheduled_for_time.tzinfo is None:
            scheduled_for_time = scheduled_for_time.replace(tzinfo=datetime.timezone.utc)

        if now < scheduled_for_time:
            print(f"  run {run_id} scheduled for {scheduled_for_time}, not ready yet, skipping (will be picked up by dispatcher when due)")
            conn.rollback()
            return

    if existing_video_id:
        print(f"  run {run_id} already uploaded ({existing_video_id}), forwarding to q.backup")
        conn.commit()
        push_job("q.backup", run_id, channel_id)
        return

    if channel_id == "demo_shorts":
        daily_cap = 1
    else:
        daily_cap = 999  # unlimited for other channels

    if not check_and_reserve_quota_db(cur, channel_id, daily_cap=daily_cap):
        print(f"  run {run_id} quota reached, re-queueing")
        conn.rollback()
        push_job("q.upload", run_id, channel_id)
        return

    publish_time = compute_next_publish_time(cur, channel_id)
    cur.execute(
        "UPDATE runs SET scheduled_for_time = %s WHERE run_id = %s",
        (publish_time, run_id)
    )
    conn.commit()

    try:
        youtube = get_youtube_client(channel_id)
        print("  uploading video...")
        video_id = retry_with_backoff(lambda: upload_video(youtube, video_path, metadata, publish_time), max_attempts=3, should_retry=lambda e: 'uploadLimitExceeded' not in str(e))
        print(f"  uploaded: https://youtu.be/{video_id}")
        if thumbnail_path and os.path.exists(thumbnail_path):
            print("  setting thumbnail...")
            retry_with_backoff(lambda: set_thumbnail(youtube, video_id, thumbnail_path), max_attempts=3)
    except HttpError as e:
        if e.resp.status == 403 and "quotaExceeded" in str(e):
            print(f"  run {run_id} hit YouTube API quota, leaving in queue for next pass and stopping this batch")
            notify_discord(f"\u23f8\ufe0f {channel_id} hit YouTube quota, resuming next pass")
            cur.execute("UPDATE runs SET scheduled_for_time = NULL WHERE run_id=%s", (run_id,))
            conn.commit()
            push_job("q.upload", run_id, channel_id)
            raise QuotaExceeded(channel_id)
        if "uploadLimitExceeded" in str(e):
            print(f"  run {run_id} hit YouTube upload limit, requeueing for next pass")
            notify_discord(f"\u23f8\ufe0f {channel_id} hit upload limit, resuming next pass")
            cur.execute("UPDATE runs SET scheduled_for_time = NULL WHERE run_id=%s", (run_id,))
            conn.commit()
            push_job("q.upload", run_id, channel_id)
            raise QuotaExceeded(channel_id)
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e), run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'upload',%s)", (run_id, str(e)))
        conn.commit()
        push_failed_job(run_id, channel_id, "upload", e)
        print(f"  run {run_id} FAILED: {e}")
        return
    except Exception as e:
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e), run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'upload',%s)", (run_id, str(e)))
        conn.commit()
        push_failed_job(run_id, channel_id, "upload", e)
        print(f"  run {run_id} FAILED: {e}")
        return

    cur.execute("UPDATE runs SET youtube_video_id=%s, status='UPLOADED', updated_at=now() WHERE run_id=%s", (video_id, run_id))
    cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'upload',%s)", (run_id, f"uploaded -> https://youtu.be/{video_id}"))
    conn.commit()
    print(f"  run {run_id} UPLOADED -> https://youtu.be/{video_id}")
    push_job("q.backup", run_id, channel_id)


def main():
    entries = read_new_jobs()
    if not entries:
        print("no new jobs on q.upload")
        return
    for entry_id, fields in entries:
        field_dict = dict(zip(fields[::2], fields[1::2]))
        payload = json.loads(field_dict["job"])
        run_id = payload["run_id"]
        channel_id = payload["channel_id"]
        print(f"processing job {entry_id}: run_id={run_id} channel_id={channel_id}")
        try:
            process_job(run_id, channel_id)
        except QuotaExceeded as qe:
            print(f"  YouTube quota exceeded for channel {qe.channel_id}, stopping this pass, will resume next pass")
            save_last_id(entry_id)
            break
        except Exception as e:
            print(f"  UNCAUGHT error processing {run_id} ({channel_id}): {e}")
            save_last_id(entry_id)
            continue
        save_last_id(entry_id)

if __name__ == "__main__":
    main()
