import os
import json
import time
import hashlib
import shutil
import requests
from engine_common import get_db, notify_discord, push_failed_job, redis_headers, REDIS_URL

STATE_FILE = os.path.expanduser("~/engine_v2/.q_backup_lastid")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.backup/({last_id}/+", headers=redis_headers, timeout=30)
    return r.json().get("result", []), last_id


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def process_job(run_id, channel_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT video_path, video_sha256 FROM runs WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row is None or not row[0]:
        print(f"  run {run_id} missing video_path, skipping")
        conn.rollback()
        cur.close()
        conn.close()
        return

    video_path, existing_hash = row
    if existing_hash:
        print(f"  run {run_id} already backed up ({existing_hash[:12]}...), nothing to do")
        conn.commit()
        cur.close()
        conn.close()
        return

    if not os.path.exists(video_path):
        cur.execute(
            "UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s",
            (f"video file missing at {video_path}", run_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"  run {run_id} FAILED: video file missing")
        return

    print("  hashing video...")
    digest = sha256_file(video_path)

    cur.execute(
        "UPDATE runs SET video_sha256=%s, status='BACKED_UP', updated_at=now() WHERE run_id=%s",
        (digest, run_id),
    )
    conn.commit()
    print(f"  run {run_id} BACKED_UP -> {digest}")

    # NOTE: paths match engine_v2's own _v2-suffixed dirs (image_worker/tts_worker),
    # not production's ~/pipeline_images / ~/pipeline_audio.
    for local_dir in [
        os.path.expanduser(f"~/pipeline_images_v2/{run_id}"),
        os.path.expanduser(f"~/pipeline_audio_v2/{run_id}"),
    ]:
        if os.path.exists(local_dir):
            try:
                shutil.rmtree(local_dir)
                print(f"  cleaned up local temp dir: {local_dir}")
            except Exception as e:
                print(f"  [cleanup failed for {local_dir}: {e}]")

    cur.execute("SELECT metadata_json, youtube_video_id FROM runs WHERE run_id=%s", (run_id,))
    meta_row = cur.fetchone()
    cur.close()
    conn.close()
    if meta_row:
        metadata_json, youtube_video_id = meta_row
        title = (metadata_json or {}).get("title", "Untitled")
        yt_url = f"https://youtu.be/{youtube_video_id}" if youtube_video_id else "(no video id)"
        notify_discord(f"Posted [{channel_id}]: {title}\n{yt_url}")


def main_loop():
    while True:
        entries, last_id = read_new_jobs()
        for entry_id, fields in entries:
            job = {}
            try:
                field_dict = dict(zip(fields[::2], fields[1::2]))
                job = json.loads(field_dict.get("job", "{}"))
                run_id = job["run_id"]
                channel_id = job["channel_id"]
                process_job(run_id, channel_id)
            except Exception as e:
                push_failed_job(job.get("run_id"), job.get("channel_id"), "backup", e, payload=fields)
            save_last_id(entry_id)
        time.sleep(5)


if __name__ == "__main__":
    main_loop()
