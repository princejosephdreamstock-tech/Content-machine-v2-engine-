import psycopg2
import os

DB_URL = os.environ["PIPELINE_DB_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("SELECT channel_id, config_json->>'shorts_enabled' FROM channels")
channels = cur.fetchall()

for channel_id, shorts_enabled in channels:
    if shorts_enabled != "true":
        continue

    cur.execute("""
        SELECT COUNT(*) FROM shorts_runs
        WHERE channel_id = %s AND created_at >= now() - interval '1 day'
    """, (channel_id,))
    recent_count = cur.fetchone()[0]

    if recent_count == 0:
        cur.execute("""
            INSERT INTO shorts_runs (channel_id, status) VALUES (%s, 'SCRIPT_PENDING')
            RETURNING run_id
        """, (channel_id,))
        run_id = cur.fetchone()[0]
        conn.commit()
        print(f"Created shorts run {run_id} for channel_id={channel_id}")
    else:
        print(f"channel_id={channel_id} shorts already posted in last 24h, skipping")

cur.close()
conn.close()
