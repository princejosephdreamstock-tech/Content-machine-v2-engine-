import os
import json
import base64
import requests
from PIL import Image, ImageDraw, ImageFont
from engine_common import retry_with_backoff, push_failed_job, push_job, rclone_upload, call_nvidia_with_rotation, get_channel_config, get_db, generate_image_with_fallback
import socket
socket.setdefaulttimeout(120)  # prevent indefinite hangs on any blocking network call

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
STATE_FILE = os.path.expanduser("~/.q_thumbnail_lastid")
THUMB_DIR = os.path.expanduser("~/pipeline_thumbnails_v2")
FLUX_URL = "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-dev"
NVIDIA_CHAT_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
FONT_PATH = "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Bold.ttf"

os.makedirs(THUMB_DIR, exist_ok=True)


class MissingChannelConfigError(Exception):
    pass


def require_config(config, key, channel_id):
    val = config.get(key)
    if not val:
        raise MissingChannelConfigError(
            f"channel '{channel_id}' has no '{key}' set in config_json — refusing to fall back to a generic default"
        )
    return val


def call_nvidia_concept(title, config, channel_id):
    instructions = require_config(config, "thumbnail_concept_instructions", channel_id)
    prompt = instructions.format(title=title)
    def _parse(raw):
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        data = json.loads(raw)
        return data["image_prompt"], data["thumbnail_text"]

    primary_payload = {
        "model": "nvidia/nemotron-3.5-lightning-30b-a3b",
        "chat_template_kwargs": {"enable_thinking": False},
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.8
    }
    try:
        resp = call_nvidia_with_rotation(NVIDIA_CHAT_URL, primary_payload, timeout=60, dedup_key=None, max_attempts=3)
        raw = resp.json()["choices"][0]["message"]["content"]
        return _parse(raw)
    except Exception as e:
        print(f"[fallback] primary model failed ({e}), trying minimax-m3")
        fallback_payload = {
            "model": "minimaxai/minimax-m3",
            "chat_template_kwargs": {"thinking_mode": "disabled"},
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.8
        }
        resp = call_nvidia_with_rotation(NVIDIA_CHAT_URL, fallback_payload, timeout=60, dedup_key=None, max_attempts=3)
        raw = resp.json()["choices"][0]["message"]["content"]
        return _parse(raw)


def generate_base_image(prompt, out_path):
    generate_image_with_fallback(prompt, out_path, width=1280, height=768, seed=0)


def overlay_text(base_path, text, out_path):
    img = Image.open(base_path).convert("RGB")
    draw = ImageDraw.Draw(img)

    font_size = 100
    font = ImageFont.truetype(FONT_PATH, font_size)
    max_width = img.width - 100

    def wrap(font):
        words = text.upper().split()
        lines, current = [], ""
        for word in words:
            test = (current + " " + word).strip()
            bbox = draw.textbbox((0, 0), test, font=font)
            if bbox[2] - bbox[0] > max_width and current:
                lines.append(current)
                current = word
            else:
                current = test
        if current:
            lines.append(current)
        return lines

    lines = wrap(font)
    while len(lines) > 2 and font_size > 50:
        font_size -= 10
        font = ImageFont.truetype(FONT_PATH, font_size)
        lines = wrap(font)
    lines = lines[:2]

    line_height = font_size + 16
    total_height = line_height * len(lines)
    y = (img.height - total_height) / 2

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        x = (img.width - (bbox[2] - bbox[0])) / 2
        draw.text((x, y), line, font=font, fill="white",
                   stroke_width=7, stroke_fill="black")
        y += line_height

    img.save(out_path, quality=92)


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.thumbnail/({last_id}/+", headers=headers, timeout=30)
    return r.json().get("result", [])


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def process_job(run_id, channel_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT metadata_json, thumbnail_path FROM runs WHERE run_id = %s",
        (run_id,)
    )
    row = cur.fetchone()
    if row is None or not row[0]:
        print(f"  run {run_id} missing metadata_json, skipping")
        conn.rollback()
        return

    metadata, existing_thumb = row
    if existing_thumb:
        print(f"  run {run_id} already has thumbnail, forwarding to q.upload")
        conn.commit()
        push_job("q.upload", run_id, channel_id)
        return

    config = get_channel_config(channel_id)
    title = metadata["title"]
    run_thumb_dir = os.path.join(THUMB_DIR, run_id)
    os.makedirs(run_thumb_dir, exist_ok=True)
    base_path = os.path.join(run_thumb_dir, "base.jpg")
    final_path = os.path.join(run_thumb_dir, "thumbnail.jpg")

    try:
        print("  generating concept + hook text...")
        concept, hook_text = retry_with_backoff(lambda: call_nvidia_concept(title, config, channel_id), max_attempts=5)
        print(f"  concept: {concept}")
        print(f"  hook text: {hook_text}")
        print("  rendering base image...")
        retry_with_backoff(lambda: generate_base_image(concept, base_path), max_attempts=5)
        print("  overlaying text...")
        overlay_text(base_path, hook_text, final_path)
        if config.get("upload_to_s3", False):
            s3_key = f"{channel_id}/{run_id}/thumbnail.jpg"
            retry_with_backoff(lambda: rclone_upload(final_path, s3_key), max_attempts=3)
        else:
            print("  upload_to_s3 not enabled for this channel, skipping S3 upload (local only)")
    except Exception as e:
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e), run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'thumbnail',%s)", (run_id, str(e)))
        conn.commit()
        push_failed_job(run_id, channel_id, "thumbnail", e)
        print(f"  run {run_id} FAILED after retries, sent to deadletter: {e}")
        return

    cur.execute(
        "UPDATE runs SET thumbnail_path=%s, status='THUMBNAIL_READY', updated_at=now() WHERE run_id=%s",
        (final_path, run_id)
    )
    cur.execute(
        "INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'thumbnail',%s)",
        (run_id, f"generated thumbnail (local storage, S3 pending) -> {final_path}")
    )
    conn.commit()
    print(f"  run {run_id} THUMBNAIL_READY -> {final_path}")
    push_job("q.upload", run_id, channel_id)


def main():
    entries = read_new_jobs()
    if not entries:
        print("no new jobs on q.thumbnail")
        return
    for entry_id, fields in entries:
        field_dict = dict(zip(fields[::2], fields[1::2]))
        payload = json.loads(field_dict["job"])
        run_id = payload["run_id"]
        channel_id = payload["channel_id"]
        print(f"processing job {entry_id}: run_id={run_id} channel_id={channel_id}")
        try:
            process_job(run_id, channel_id)
        except Exception as e:
            print(f"  UNCAUGHT error processing {run_id} ({channel_id}): {e}")
        finally:
            save_last_id(entry_id)

if __name__ == "__main__":
    main()
