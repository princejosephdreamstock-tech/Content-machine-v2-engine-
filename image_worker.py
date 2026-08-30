import socket
socket.setdefaulttimeout(120)
import os
os.environ.setdefault('PGSQL_STATEMENT_TIMEOUT', '60000')
import json
import base64
import base64 as b64lib
import random
import time
import requests
import psycopg2
from engine_common import (
    retry_with_backoff, push_failed_job, push_job, rclone_upload,
    get_channel_config, call_nvidia_with_rotation, generate_image_with_fallback,
    GatewayRequeueSignal
)

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
STATE_FILE = os.path.expanduser("~/.q_image_lastid")
IMAGE_DIR = os.path.expanduser("~/pipeline_images_v2")
FLUX_URL = "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-dev"
VLM_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

os.makedirs(IMAGE_DIR, exist_ok=True)

DEFAULT_UNSAFE_WORD_MAP = {}


def sanitize_prompt(prompt, unsafe_word_map):
    result = prompt
    for unsafe, safe in unsafe_word_map.items():
        for variant in (unsafe, unsafe.capitalize()):
            result = result.replace(variant, safe)
    return result


def generate_image(prompt, out_path):
    generate_image_with_fallback(prompt, out_path, width=1024, height=1024, seed=random.randint(1, 2**31 - 1))


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.image/({last_id}/+", headers=headers, timeout=30)
    return r.json().get("result", [])


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def get_conn():
    return psycopg2.connect(DB_URL)


def process_job(run_id, channel_id):
    conn = get_conn()
    config = get_channel_config(channel_id)
    unsafe_word_map = config.get("image_safety_word_map", DEFAULT_UNSAFE_WORD_MAP)

    cur = conn.cursor()
    cur.execute("SELECT script_json FROM runs WHERE run_id = %s ", (run_id,))
    row = cur.fetchone()
    if row is None or not row[0]:
        print(f"  run {run_id} missing script_json, skipping")
        conn.rollback()
        return

    sections = row[0]
    if all("image_path" in s for s in sections):
        print(f"  run {run_id} already has images, forwarding to q.assembly")
        conn.commit()
        push_job("q.assembly", run_id, channel_id)
        return

    run_image_dir = os.path.join(IMAGE_DIR, run_id)
    os.makedirs(run_image_dir, exist_ok=True)

    try:
        for idx, section in enumerate(sections):
            if "image_path" in section:
                continue
            out_path = os.path.join(run_image_dir, f"sec_{idx}.jpg")
            print(f"  generating image {idx}...")
            sanitized_prompt = sanitize_prompt(section["image_prompt"], unsafe_word_map)
            retry_with_backoff(
                lambda: generate_image(sanitized_prompt, out_path),
                max_attempts=3,
                should_retry=lambda e: not isinstance(e, GatewayRequeueSignal)
            )
            section["image_path"] = out_path

            cur.execute(
                "UPDATE runs SET script_json=%s, updated_at=now() WHERE run_id=%s",
                (json.dumps(sections), run_id)
            )
            conn.commit()

            time.sleep(3)
    except GatewayRequeueSignal as e:
        conn.rollback()
        push_job("q.image", run_id, channel_id, extra={"note": "requeued after gateway backoff"})
        print(f"  run {run_id} gateway requested requeue ({e.retry_after}s hint), pushed back to q.image")
        return
    except Exception as e:
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e), run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'image',%s)", (run_id, str(e)))
        conn.commit()
        push_failed_job(run_id, channel_id, "image", e)
        print(f"  run {run_id} FAILED after retries, sent to deadletter: {e}")
        return

    cur.execute(
        "UPDATE runs SET script_json=%s, updated_at=now() WHERE run_id=%s",
        (json.dumps(sections), run_id)
    )
    cur.execute(
        "INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'image',%s)",
        (run_id, f"generated {len(sections)} images")
    )
    conn.commit()
    print(f"  run {run_id} images done -> {run_image_dir}")
    push_job("q.assembly", run_id, channel_id)


def main():
    entries = read_new_jobs()
    if not entries:
        print("no new jobs on q.image")
        return
    for entry_id, fields in entries:
        run_id = None
        channel_id = None
        try:
            field_dict = dict(zip(fields[::2], fields[1::2]))
            payload = json.loads(field_dict["job"])
            run_id = payload["run_id"]
            channel_id = payload["channel_id"]
            print(f"processing job {entry_id}: run_id={run_id} channel_id={channel_id}")
            process_job(run_id, channel_id)
        except Exception as e:
            print(f"  UNCAUGHT error processing entry {entry_id} run_id={run_id} channel_id={channel_id}: {e}")
        finally:
            save_last_id(entry_id)

if __name__ == "__main__":
    main()
