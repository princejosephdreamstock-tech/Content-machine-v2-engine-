import os
import json
import ast
import requests
from engine_common import retry_with_backoff, push_failed_job, push_job, call_nvidia_with_rotation, get_channel_config, get_db
import socket
socket.setdefaulttimeout(120)  # prevent indefinite hangs on any blocking network call

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
STATE_FILE = os.path.expanduser("~/.q_metadata_lastid")

NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"


class MissingChannelConfigError(Exception):
    pass


def require_config(config, key, channel_id):
    val = config.get(key)
    if not val:
        raise MissingChannelConfigError(
            f"channel '{channel_id}' has no '{key}' set in config_json — refusing to fall back to a generic default"
        )
    return val


def call_nvidia(sections, config, channel_id, run_id):
    section_text = "\n".join(
        f"- {s['caption']}: {s['voiceover_text']}" for s in sections
    )
    instructions = require_config(config, "channel_instructions", channel_id)
    prompt = instructions + section_text
    
    primary_payload = {
        "model": "nvidia/nemotron-3.5-lightning-30b-a3b",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 500,
        "temperature": 0.7,
        "chat_template_kwargs": {"enable_thinking": False}
    }
    try:
        resp = call_nvidia_with_rotation(NVIDIA_URL, primary_payload, timeout=60, dedup_key=f"{run_id}:metadata")
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"[fallback] primary model failed ({e}), trying minimax-m3")
        fallback_payload = {
            "model": "minimaxai/minimax-m3",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 500,
            "temperature": 0.7,
            "chat_template_kwargs": {"thinking_mode": "disabled"}
        }
        resp = call_nvidia_with_rotation(NVIDIA_URL, fallback_payload, timeout=60, dedup_key=f"{run_id}:metadata")
        return resp.json()["choices"][0]["message"]["content"]


def parse_metadata(raw_text, config, channel_id):
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    try:
        metadata = json.loads(text)
    except json.JSONDecodeError:
        metadata = ast.literal_eval(text)
    for key in ("title", "description", "tags", "category"):
        if key not in metadata:
            raise ValueError(f"Missing required key: {key}")

    guide_link = require_config(config, "guide_link", channel_id)
    cta_text = require_config(config, "cta_text", channel_id)
    original_description = metadata["description"]
    metadata["description"] = f"{cta_text}: " + guide_link + "\n\n" + original_description
    return metadata


def _generate_once(sections, config, channel_id, run_id):
    raw = call_nvidia(sections, config, channel_id, run_id)
    return parse_metadata(raw, config, channel_id)


def generate_metadata(sections, config, channel_id, run_id):
    return retry_with_backoff(lambda: _generate_once(sections, config, channel_id, run_id), max_attempts=5)


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.metadata/({last_id}/+", headers=headers, timeout=30)
    return r.json().get("result", [])


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def process_job(run_id, channel_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT script_json, metadata_json FROM runs WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row is None or not row[0]:
        print(f"  run {run_id} missing script_json, skipping")
        conn.rollback()
        return

    sections, existing_metadata = row
    if existing_metadata:
        print(f"  run {run_id} already has metadata, forwarding to q.thumbnail")
        conn.commit()
        push_job("q.thumbnail", run_id, channel_id)
        return

    config = get_channel_config(channel_id)

    try:
        metadata = generate_metadata(sections, config, channel_id, run_id)
    except Exception as e:
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e), run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'metadata',%s)", (run_id, str(e)))
        conn.commit()
        push_failed_job(run_id, channel_id, "metadata", e)
        print(f"  run {run_id} FAILED after retries, sent to deadletter: {e}")
        return

    cur.execute(
        "UPDATE runs SET metadata_json=%s, status='METADATA_READY', updated_at=now() WHERE run_id=%s",
        (json.dumps(metadata), run_id)
    )
    cur.execute(
        "INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'metadata',%s)",
        (run_id, f"generated metadata: {metadata['title']}")
    )
    conn.commit()
    print(f"  run {run_id} METADATA_READY: {metadata['title']}")
    push_job("q.thumbnail", run_id, channel_id)


def main():
    entries = read_new_jobs()
    if not entries:
        print("no new jobs on q.metadata")
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
