"""
Periodic second-chance reviver for runs marked [MANUAL REVIEW] by
stuck_run_check.py. Covers ALL stages/workers (script, image, assembly,
metadata, thumbnail, upload) - not just image.

Rationale: stuck_run_check.py correctly gives up after MAX_REPAIR_ATTEMPTS
and alerts a human, but has no way to distinguish "permanently broken" from
"failed during a transient outage that has since cleared". This script
probes dead runs periodically and gives them one more chance, capped, so
a temporary NVIDIA/Cloudflare outage doesn't permanently kill a run that
would succeed fine once the backend recovers.

Run this on a longer cadence than stuck_run_check.py (e.g. every 6 hours
via cron), NOT on every pipeline loop pass - the whole point is to wait
out real outages, not hammer a dead backend faster.
"""
import os, json, requests, psycopg2, datetime

DB_URL = os.environ["PIPELINE_DB_URL"]
REDIS_URL = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}

# How long a run must have sat dead in [MANUAL REVIEW] before we probe it again.
REVIVE_COOLDOWN_HOURS = 6

# Max number of "second chance" revival attempts, separate from the original
# MAX_REPAIR_ATTEMPTS in stuck_run_check.py. Once exhausted, it's left dead
# for good and requires an actual human to intervene - this never revives
# a run infinitely.
MAX_REVIVE_ATTEMPTS = 3

# Which queue to push back to, based on the status the run is sitting at
# when it was marked dead. Mirrors stuck_run_check.py's QUEUE_FOR_STATUS.
QUEUE_FOR_STATUS = {
    "PLANNED": "q.script",
    "SCRIPTED": "q.image",
    "ASSETS_GENERATED": "q.assembly",
    "ASSEMBLED": "q.metadata",
    "METADATA_READY": "q.thumbnail",
    "THUMBNAIL_READY": "q.upload",
}

def notify_discord(message):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
    except Exception as e:
        print(f"  [notify_discord] failed to send alert: {e}")

def push_job(queue, run_id, channel_id):
    dedup_key = f"pushed:{queue}:{run_id}"
    dedup_resp = requests.post(
        f"{REDIS_URL}/set/{dedup_key}/1/EX/60/NX", headers=headers
    )
    if dedup_resp.json().get("result") is None:
        print(f"  [push_job] skipped duplicate push of {run_id} to {queue} (already pushed in last 60s)")
        return
    payload = json.dumps({"run_id": run_id, "channel_id": channel_id, "attempt": 0})
    requests.post(f"{REDIS_URL}/xadd/{queue}/*/job/{payload}", headers=headers)

def get_conn():
    return psycopg2.connect(DB_URL)

def main():
    conn = get_conn()
    cur = conn.cursor()

    # Find every run currently marked dead, across every stage/status -
    # not just SCRIPTED/image. LIKE '[MANUAL REVIEW]%' matches the exact
    # marker stuck_run_check.py writes for every failure type (auth
    # failures are deliberately excluded below since those need a real
    # human re-auth, not a retry).
    cur.execute("""
        SELECT run_id, channel_id, status, error, updated_at, revive_count
        FROM runs
        WHERE error LIKE '[MANUAL REVIEW]%%'
    """)
    rows = cur.fetchall()

    if not rows:
        print("no runs currently marked [MANUAL REVIEW]")
        conn.close()
        return

    for run_id, channel_id, status, error, last_updated, revive_count in rows:
        revive_count = revive_count or 0

        # Never auto-retry auth failures - these need an actual human to
        # re-authenticate the channel, retrying won't fix an expired token.
        if "invalid_grant" in error or "expired/revoked OAuth" in error:
            print(f"  run {run_id}: auth failure, skipping (needs manual re-auth, not a retry)")
            continue

        if revive_count >= MAX_REVIVE_ATTEMPTS:
            print(f"  run {run_id}: exhausted {revive_count} revive attempts, leaving dead permanently")
            continue

        elapsed_hours = (datetime.datetime.now(datetime.timezone.utc) - last_updated).total_seconds() / 3600
        if elapsed_hours < REVIVE_COOLDOWN_HOURS:
            print(f"  run {run_id}: only {elapsed_hours:.1f}h since marked dead, waiting for {REVIVE_COOLDOWN_HOURS}h cooldown")
            continue

        queue = QUEUE_FOR_STATUS.get(status)
        if not queue:
            print(f"  run {run_id}: status={status} has no known queue, skipping")
            continue

        cur.execute(
            "UPDATE runs SET error=NULL, updated_at=now(), revive_count=%s WHERE run_id=%s",
            (revive_count + 1, run_id)
        )
        conn.commit()
        push_job(queue, run_id, channel_id)
        print(f"  run {run_id} ({channel_id}): revived after {elapsed_hours:.1f}h dead, pushed to {queue} (revive attempt {revive_count+1}/{MAX_REVIVE_ATTEMPTS})")
        notify_discord(f":large_green_circle: run {run_id} ({channel_id}) revived from [MANUAL REVIEW] after {elapsed_hours:.1f}h, retrying stage {status} (attempt {revive_count+1}/{MAX_REVIVE_ATTEMPTS})")

    conn.close()

if __name__ == "__main__":
    main()
