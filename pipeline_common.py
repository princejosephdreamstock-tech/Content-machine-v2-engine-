import os
import time
import json
import random
import urllib.parse
import requests

REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]
redis_headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}

NVIDIA_KEYS = [os.environ["NVIDIA_API_KEY"]]
if os.environ.get("NVIDIA_API_KEY_2"):
    NVIDIA_KEYS.append(os.environ["NVIDIA_API_KEY_2"])

def get_nvidia_key(attempt=0):
    """Alternates NVIDIA API keys across retry attempts so a rate limit
    on one key falls back to the other instead of failing outright."""
    return NVIDIA_KEYS[attempt % len(NVIDIA_KEYS)]


class RateLimitError(Exception):
    """Raised when an API call is rate-limited (HTTP 429). Carries the
    server-suggested wait time (seconds) if one was provided."""
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


def push_job(queue_name, run_id, channel_id, attempt=0):
    payload = json.dumps({"run_id": run_id, "channel_id": channel_id, "attempt": attempt})
    r = requests.post(f"{REDIS_URL}/xadd/{queue_name}/*/job/{payload}", headers=redis_headers)
    return r.json()


def push_deadletter(run_id, channel_id, stage, error):
    payload = json.dumps({
        "run_id": run_id, "channel_id": channel_id,
        "stage": stage, "error": str(error)[:500],
    })
    r = requests.post(f"{REDIS_URL}/xadd/q.deadletter/*/job/{payload}", headers=redis_headers)
    return r.json()


def retry_with_backoff(fn, max_attempts=5, base_delay=2, max_delay=60, should_retry=None):
    """Calls fn() with no args. Retries on any Exception. Raises the last
    exception if all attempts are exhausted. Pass should_retry=lambda e: bool
    to skip retrying on specific exceptions."""
    last_exc = None
    for attempt in range(max_attempts):
        try:
            result = fn()
            if attempt > 0:
                notify_discord(f"recovered after retry (attempt {attempt+1}/{max_attempts}), continuing normally")
            return result
        except Exception as e:
            last_exc = e
            if attempt == max_attempts - 1:
                break
            if should_retry is not None and not should_retry(e):
                raise e
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None:
                sleep_time = min(retry_after + random.uniform(0.5, 2.0), max_delay * 4)
            else:
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = delay * random.uniform(-0.2, 0.2)
                sleep_time = max(0.5, delay + jitter)
            print(f"    attempt {attempt+1}/{max_attempts} failed ({e}), retrying in {sleep_time:.1f}s...")
            notify_discord(f"hit a snag ({e}), retrying (attempt {attempt+2}/{max_attempts})...")
            time.sleep(sleep_time)
    notify_discord(f"retries exhausted after {max_attempts} attempts, giving up ({last_exc})")
    raise last_exc

import subprocess

RCLONE_REMOTE = "yes:pipeline-backup"


def rclone_upload(local_path, remote_key):
    dest = f"{RCLONE_REMOTE}/{remote_key}"
    subprocess.run(
        ["rclone", "copyto", local_path, dest],
        check=True, capture_output=True, text=True
    )
    return remote_key

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")
VM_NAME = os.environ.get("VM_NAME", "unknown-vm")

def notify_discord(message):
    try:
        tagged_message = f"[{VM_NAME}] {message}"
        requests.post(DISCORD_WEBHOOK_URL, json={"content": tagged_message}, timeout=10)
    except Exception as e:
        print(f"  (discord notify failed, continuing: {e})")

def call_nvidia_with_rotation(url, payload, timeout=120, max_attempts=30, max_backoff=60, dedup_key=None):
    lock_acquired = False
    if dedup_key:
        lock_resp = requests.post(
            f"{REDIS_URL}/set/nvcall:{urllib.parse.quote(dedup_key, safe='')}/1/EX/180/NX",
            headers=redis_headers,
        )
        if lock_resp.json().get("result") is None:
            raise Exception(f"Duplicate NVIDIA call suppressed for {dedup_key} (already in flight or completed in last 180s)")
        lock_acquired = True
    import time as _time
    keys = NVIDIA_KEYS
    if not keys:
        raise ValueError("No NVIDIA_KEYS configured")
    last_error = None
    for attempt in range(max_attempts):
        key = keys[attempt % len(keys)]
        rotation_lap = (attempt // len(keys)) + 1
        print(f"    [nvidia rotation] attempt {attempt+1}/{max_attempts} (lap {rotation_lap}) using key ending ...{key[-4:]}")
        h = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
        try:
            resp = requests.post(url, headers=h, json=payload, timeout=timeout)
            if resp.status_code == 200:
                return resp
            last_error = Exception(f"NVIDIA {resp.status_code}: {resp.text[:200]}")
            print(f"    [nvidia rotation] attempt {attempt+1} failed: {last_error}")
        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"    [nvidia rotation] attempt {attempt+1} failed: {e}")
        if attempt < max_attempts - 1:
            backoff = min(2 ** (attempt % 6), max_backoff)
            print(f"    [nvidia rotation] retrying in {backoff}s...")
            _time.sleep(backoff)
    if dedup_key and lock_acquired:
        requests.post(f"{REDIS_URL}/del/nvcall:{urllib.parse.quote(dedup_key, safe='')}", headers=redis_headers)
    raise last_error


def generate_image_with_fallback(prompt, out_path, width=1024, height=1024, cfg_scale=3.5, steps=50, seed=None):
    import base64
    import random as _random

    flux_url = "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-dev"
    nvidia_payload = {
        "prompt": prompt, "mode": "base", "cfg_scale": cfg_scale,
        "width": width, "height": height,
        "seed": seed if seed is not None else _random.randint(1, 2**31 - 1),
        "steps": steps,
    }
    try:
        resp = call_nvidia_with_rotation(flux_url, nvidia_payload, timeout=30, dedup_key=None, max_attempts=1)
        data = resp.json()
        img_b64 = data["artifacts"][0]["base64"]
        with open(out_path, "wb") as f:
            f.write(base64.b64decode(img_b64))
        return "nvidia"
    except Exception as e:
        print(f"    [image fallback] NVIDIA exhausted ({e}), falling back to Cloudflare")
        from cloudflare_pool import generate_cloudflare_image
        return generate_cloudflare_image(prompt, width, height, out_path)
