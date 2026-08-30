#!/usr/bin/env python3
"""
1. Requeues the stuck axiom_forensics image job (run_id=eb0fd768...)
2. Deletes the 3 leftover houseplant failed_jobs rows (cross-VM junk)
"""
import os

def load_env_file(path):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val

load_env_file("/home/ec2-user/pipeline.env")

import sys
sys.path.insert(0, "/home/ec2-user/engine_v2")
from engine_common import get_db, push_job
import psycopg2.extras

STUCK_RUN_ID = "eb0fd768-3470-4d78-aceb-8e69225ee1be"

conn = get_db()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# --- Step 1: show what's about to be deleted (houseplant junk) ---
cur.execute("SELECT id, channel_id, stage, created_at FROM failed_jobs WHERE channel_id = 'houseplant'")
houseplant_rows = cur.fetchall()
print(f"Found {len(houseplant_rows)} houseplant failed_jobs rows to delete:")
for r in houseplant_rows:
    print(f"  id={r['id']} stage={r['stage']} created_at={r['created_at']}")

# --- Step 2: delete them ---
cur.execute("DELETE FROM failed_jobs WHERE channel_id = 'houseplant'")
conn.commit()
print(f"Deleted {cur.rowcount} houseplant rows.\n")

# --- Step 3: look up the stuck run's payload details ---
cur.execute("SELECT run_id, channel_id FROM runs WHERE run_id = %s", (STUCK_RUN_ID,))
run_row = cur.fetchone()
if not run_row:
    print(f"WARNING: run_id {STUCK_RUN_ID} not found in runs table — cannot requeue automatically.")
else:
    payload = {"run_id": run_row["run_id"], "channel_id": run_row["channel_id"]}
    push_job("q.image", payload)
    print(f"Requeued to q.image: {payload}")

cur.close()
conn.close()
