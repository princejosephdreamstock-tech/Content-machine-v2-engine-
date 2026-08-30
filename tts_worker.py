import os
import json
import asyncio
import subprocess
import psycopg2
import requests
import edge_tts
from engine_common import retry_with_backoff, push_failed_job, push_job, rclone_upload, get_channel_config
import socket
socket.setdefaulttimeout(120)  # force any hanging network call to fail after 120s instead of blocking until the 900s kill

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
STATE_FILE = os.path.expanduser("~/.q_tts_lastid")
AUDIO_DIR = os.path.expanduser("~/pipeline_audio_v2")
DEFAULT_EDGE_VOICE = "en-US-AvaMultilingualNeural"

os.makedirs(AUDIO_DIR, exist_ok=True)


async def _synthesize_async(text, out_path, voice):
    communicate = edge_tts.Communicate(text, voice)
    await communicate.save(out_path)


def synthesize(text, out_path, voice):
    asyncio.run(asyncio.wait_for(_synthesize_async(text, out_path, voice), timeout=20))


def get_duration(path):
    out = subprocess.run(
        ["/usr/local/bin/ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True, check=True
    )
    return float(out.stdout.strip())


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.tts/({last_id}/+", headers=headers, timeout=30)
    return r.json().get("result", [])


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def get_conn():
    return psycopg2.connect(DB_URL)


def process_job(run_id, channel_id):
    conn = get_conn()
    config = get_channel_config(channel_id)
    voice = config.get("tts_voice", DEFAULT_EDGE_VOICE)

    cur = conn.cursor()
    cur.execute("SELECT script_json FROM runs WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row is None or not row[0]:
        print(f"  run {run_id} missing script_json, skipping")
        conn.rollback()
        return

    sections = row[0]
    if all("audio_path" in s for s in sections):
        print(f"  run {run_id} already has audio, forwarding to q.image")
        conn.commit()
        push_job("q.image", run_id, channel_id)
        return

    run_audio_dir = os.path.join(AUDIO_DIR, run_id)
    os.makedirs(run_audio_dir, exist_ok=True)

    try:
        for idx, section in enumerate(sections):
            if "audio_path" in section:
                continue
            out_path = os.path.join(run_audio_dir, f"sec_{idx}.mp3")
            print(f"  synthesizing section {idx}...")
            retry_with_backoff(lambda: synthesize(section["voiceover_text"], out_path, voice), max_attempts=3)
            duration = get_duration(out_path)
            section["audio_path"] = out_path
            section["duration"] = duration

    except Exception as e:
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e), run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'tts',%s)", (run_id, str(e)))
        conn.commit()
        push_failed_job(run_id, channel_id, "tts", e)
        print(f"  run {run_id} FAILED after retries, sent to deadletter: {e}")
        return

    cur.execute(
        "UPDATE runs SET script_json=%s, status='ASSETS_GENERATED', updated_at=now() WHERE run_id=%s",
        (json.dumps(sections), run_id)
    )
    cur.execute(
        "INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'tts',%s)",
        (run_id, f"synthesized {len(sections)} sections")
    )
    conn.commit()
    print(f"  run {run_id} audio done -> {run_audio_dir}")
    push_job("q.image", run_id, channel_id)


def main():
    entries = read_new_jobs()
    if not entries:
        print("no new jobs on q.tts")
        return
    for entry_id, fields in entries:
        field_dict = dict(zip(fields[::2], fields[1::2]))
        payload = json.loads(field_dict["job"])
        run_id = payload["run_id"]
        channel_id = payload["channel_id"]
        print(f"processing job {entry_id}: run_id={run_id} channel_id={channel_id}")
        try:
            get_channel_config(channel_id)  # raises ValueError if this VM's DB doesn't own channel_id
        except ValueError as e:
            print(f"  SKIPPING job not owned by this VM's channel DB: {e}")
            save_last_id(entry_id)
            continue
        try:
            process_job(run_id, channel_id)
        except Exception as e:
            print(f"  UNCAUGHT error processing {run_id} ({channel_id}): {e}")
        finally:
            save_last_id(entry_id)

if __name__ == "__main__":
    main()
