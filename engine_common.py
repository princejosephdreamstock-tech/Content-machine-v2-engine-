import os
import json
import traceback
import psycopg2
import psycopg2.extras
import requests
import random
import subprocess
import time

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]
redis_headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}

# --- NVIDIA circuit breaker (in-memory, resets on process restart) ---
def generate_image_with_fallback(prompt, out_path, width=1024, height=1024, cfg_scale=3.5, steps=50, seed=None):
    """Routes through provider_gateway.call_provider() for shared,
    Redis-backed rate limiting and circuit breaking across all joy-vm
    pipelines. Tries NVIDIA Flux first, falls back to Cloudflare Workers AI
    (Flux schnell) on any NVIDIA failure or when NVIDIA's circuit is open.
    Raises GatewayRequeueSignal (not a generic Exception) when the gateway
    says the caller should back off and requeue instead of retrying inline."""
    import base64
    import random as _random
    from provider_gateway import call_provider, ProviderError, RateLimitError

    flux_url = "https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-dev"
    nvidia_payload = {
        "prompt": prompt, "mode": "base", "cfg_scale": cfg_scale,
        "width": width, "height": height,
        "seed": seed if seed is not None else _random.randint(1, 2**31 - 1),
        "steps": steps,
    }

    def _call_nvidia():
        resp = call_nvidia_with_rotation(flux_url, nvidia_payload, timeout=30, dedup_key=None, max_attempts=1)
        data = resp.json()
        img_b64 = data["artifacts"][0]["base64"]
        with open(out_path, "wb") as f:
            f.write(base64.b64decode(img_b64))
        return "nvidia"

    nvidia_result = call_provider("nvidia", _call_nvidia)
    if nvidia_result.success:
        return nvidia_result.data

    print(f"    [image fallback] NVIDIA gateway declined ({nvidia_result.requeue_after_seconds}s backoff), trying Cloudflare")

    from cloudflare_pool import generate_cloudflare_image

    def _call_cloudflare():
        return generate_cloudflare_image(prompt, width, height, out_path)

    cf_result = call_provider("cloudflare", _call_cloudflare)
    if cf_result.success:
        return cf_result.data

    print(f"    [image fallback] Cloudflare gateway also declined ({cf_result.requeue_after_seconds}s backoff)")
    raise GatewayRequeueSignal(
        retry_after=max(nvidia_result.requeue_after_seconds, cf_result.requeue_after_seconds),
        message="both nvidia and cloudflare providers declined via gateway"
    )


def get_db():
    return psycopg2.connect(DB_URL)


def get_default(key, fallback=None):
    """Single source for any value that isn't channel-specific.
    Nothing in worker code is a hardcoded constant — it's all here in the DB."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM system_defaults WHERE key = %s", (key,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row is None:
        return fallback
    return row[0]


def get_all_defaults():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM system_defaults")
    rows = dict(cur.fetchall())
    cur.close()
    conn.close()
    return rows


def call_nvidia_with_rotation(url, payload, timeout=120, max_attempts=None, dedup_key=None):
    """dedup_key: if provided, guards against duplicate concurrent/rapid
    calls for the same logical work (e.g. f"nvidia:{run_id}:{stage}").
    A second call with the same key within the lock window raises
    immediately instead of burning a second NVIDIA request."""
    lock_acquired = False
    if dedup_key:
        import urllib.parse
        encoded_lock_key = urllib.parse.quote(f"nvcall:{dedup_key}", safe="")
        lock_resp = requests.post(
            f"{REDIS_URL}/set/{encoded_lock_key}/1/EX/180/NX",
            headers=redis_headers,
        )
        lock_json = lock_resp.json()
        if "error" in lock_json:
            raise Exception(f"Redis error acquiring dedup lock for {dedup_key}: {lock_json['error']}")
        if lock_json.get("result") is None:
            raise Exception(f"Duplicate NVIDIA call suppressed for {dedup_key} (already in flight or completed in last 180s)")
        lock_acquired = True
    """Single source of truth for every NVIDIA API call across all workers.
    Rotates through ALL configured nvidia_api_keys on ANY failure -
    timeout, connection error, 5xx, or 429 - not just rate limits.
    One API key can never be trusted alone; this is the shared retry path
    every worker (script/image/metadata/thumbnail) must call instead of
    hitting requests.post directly."""
    keys_raw = get_default("nvidia_api_keys", "[]")
    keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
    if not keys:
        raise ValueError("No nvidia_api_keys configured in system_defaults")
    # Persist across MULTIPLE full rotations through all keys, not just one
    # lap. max_attempts, if given, is a hard ceiling on total tries; otherwise
    # default to several full rotations through the key list.
    rotations = 5
    attempts = max_attempts or (len(keys) * rotations)
    max_backoff = 30
    last_error = None
    for attempt in range(attempts):
        key = keys[attempt % len(keys)]
        lap = (attempt // len(keys)) + 1
        print(f"    [nvidia rotation] attempt {attempt+1}/{attempts} (lap {lap}) using key ending ...{key[-4:]}")
        h = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
        try:
            resp = requests.post(url, headers=h, json=payload, timeout=timeout)
            if resp.status_code == 200:
                return resp
            last_error = Exception(f"NVIDIA {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.RequestException as e:
            last_error = e
            print(f"    [nvidia rotation] attempt {attempt+1} failed: {e}")
        if attempt < attempts - 1:
            import time
            time.sleep(min(2 ** (attempt % len(keys)), max_backoff))
    if dedup_key and lock_acquired:
        import urllib.parse
        encoded_lock_key = urllib.parse.quote(f"nvcall:{dedup_key}", safe="")
        requests.post(f"{REDIS_URL}/del/{encoded_lock_key}", headers=redis_headers)
    raise last_error


def get_channel_config(channel_id):
    """Reads the channels table (channel_id, upload_interval_hours, assigned_vm, config_json)
    and layers system_defaults underneath — any key a channel's config_json doesn't set
    falls back to the shared default, so no worker file ever hardcodes a value."""
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT channel_id, upload_interval_hours, assigned_vm, config_json FROM channels WHERE channel_id = %s", (channel_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row is None:
        raise ValueError(f"No channel config found for channel_id={channel_id!r}")
    result = get_all_defaults()
    result.update(dict(row))
    result.update(result.pop("config_json") or {})
    return result


def update_channel_config(channel_id, updates: dict):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE channels SET config_json = config_json || %s::jsonb WHERE channel_id = %s",
        (json.dumps(updates), channel_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def pause_channel(channel_id, reason):
    update_channel_config(channel_id, {"paused": True, "paused_reason": reason})


def resume_channel(channel_id):
    update_channel_config(channel_id, {"paused": False, "paused_reason": None})
    notify_discord(f"✅ {channel_id} resumed — upload succeeded, quota is back.")


def is_paused(channel_id):
    cfg = get_channel_config(channel_id)
    return bool(cfg.get("paused"))


def notify_discord(message):
    webhook = get_default("discord_webhook_url") or os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook:
        print(f"[notify_discord] no webhook configured, message was: {message}")
        return
    try:
        requests.post(webhook, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"[notify_discord] failed to send: {e}")


def push_job(queue_name, run_id, channel_id, attempt=0, extra=None):
    # Dedup guard: skip if this exact (queue, run_id) was pushed in the
    # last 60s, so any double-call (retry, race, duplicate trigger)
    # only ever lands one entry in the stream.
    dedup_key = f"pushed:{queue_name}:{run_id}"
    dedup_resp = requests.post(
        f"{REDIS_URL}/set/{dedup_key}/1/EX/60/NX",
        headers=redis_headers,
    )
    if dedup_resp.json().get("result") is None:
        print(f"    [push_job] skipped duplicate push of {run_id} to {queue_name} (already pushed in last 60s)")
        return {"skipped": True, "reason": "duplicate"}

    payload = {"run_id": str(run_id), "channel_id": channel_id, "attempt": attempt}
    if extra:
        payload.update(extra)
    r = requests.post(
        f"{REDIS_URL}/xadd/{queue_name}/*/job/{json.dumps(payload)}",
        headers=redis_headers,
    )
    return r.json()


def push_failed_job(run_id, channel_id, stage, error, payload=None):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO failed_jobs (run_id, channel_id, stage, error_message, stack_trace, payload_json)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (
            str(run_id) if run_id else None,
            channel_id,
            stage,
            str(error),
            traceback.format_exc(),
            json.dumps(payload) if payload else None,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    notify_discord(f"🔴 {channel_id} / {stage} failed: {str(error)[:200]}")


def with_fault_tolerance(stage_name):
    def decorator(fn):
        def wrapper(run_id, channel_id, *args, **kwargs):
            try:
                return fn(run_id, channel_id, *args, **kwargs)
            except Exception as e:
                push_failed_job(run_id, channel_id, stage_name, e,
                                 payload={"args": str(args), "kwargs": str(kwargs)})
                return None
        return wrapper
    return decorator


class QuotaExceededError(Exception):
    pass

class GatewayRequeueSignal(Exception):
    """Raised by generate_image_with_fallback() when provider_gateway.call_provider()
    reports should_requeue=True for both nvidia and cloudflare (circuit open,
    admission budget exhausted, or rate limited). Callers should catch this
    BEFORE any generic except Exception, push the job back onto its queue,
    and return cleanly (no FAILED status, no deadletter) rather than treating
    it as a real failure. Carries .retry_after so retry_with_backoff's
    should_retry hook can skip inline retries and let the caller requeue."""
    def __init__(self, retry_after=None, message="gateway requested requeue"):
        super().__init__(message)
        self.retry_after = retry_after


def retry_with_backoff(fn, max_attempts=5, base_delay=2, max_delay=60, should_retry=None):
    """Calls fn() with no args. Retries on any Exception. Raises the last
    exception if all attempts are exhausted."""
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


RCLONE_REMOTE = "yes:pipeline-backup-v2"


def rclone_upload(local_path, remote_key):
    dest = f"{RCLONE_REMOTE}/{remote_key}"
    subprocess.run(
        ["rclone", "copyto", local_path, dest],
        check=True, capture_output=True, text=True
    )
    return remote_key
