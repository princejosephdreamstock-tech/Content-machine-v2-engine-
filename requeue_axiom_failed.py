#!/usr/bin/env python3
"""
Requeues all FAILED runs for channel_id='axiom_forensics' back to SCRIPTED,
clearing the error field. No hardcoded run_ids - queries dynamically.
Safe to rerun any time; only touches axiom_forensics rows.
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
            if line.startswith("export "):
                line = line[len("export "):]
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val

load_env_file("/home/ec2-user/pipeline.env")

import sys
sys.path.insert(0, "/home/ec2-user/engine_v2")
from engine_common import get_db
import psycopg2.extras

conn = get_db()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""
    SELECT run_id, error, updated_at FROM runs
    WHERE channel_id = 'axiom_forensics' AND status = 'FAILED'
""")
rows = cur.fetchall()

print(f"Found {len(rows)} FAILED axiom_forensics runs to requeue:")
for r in rows:
    print(f"  run_id={r['run_id']} error={r['error']!r} updated_at={r['updated_at']}")

if rows:
    cur.execute("""
        UPDATE runs SET status = 'SCRIPTED', error = NULL
        WHERE channel_id = 'axiom_forensics' AND status = 'FAILED'
        RETURNING run_id
    """)
    updated = cur.fetchall()
    conn.commit()
    print(f"Requeued {len(updated)} run(s).")
else:
    print("Nothing to requeue.")

cur.close()
conn.close()
