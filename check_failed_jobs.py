#!/usr/bin/env python3
"""
List failed_jobs rows for axiom_forensics using PIPELINE_DB_URL.
Read-only — no writes/deletes.
"""
import os
import sys

def load_env_file(path):
    if not os.path.exists(path):
        return False
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    return True

# Same env file the workers themselves use
load_env_file("/home/ec2-user/pipeline.env")

DB_URL = os.environ.get("PIPELINE_DB_URL")
if not DB_URL:
    print("ERROR: PIPELINE_DB_URL not found even after loading pipeline.env")
    sys.exit(1)

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 not installed. Try: pip3 install psycopg2-binary")
    sys.exit(1)

conn = psycopg2.connect(DB_URL)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""
    SELECT run_id, stage, error_message, created_at
    FROM failed_jobs
    WHERE channel_id = %s
    ORDER BY created_at DESC
""", ("axiom_forensics",))

rows = cur.fetchall()
print(f"\n{len(rows)} failed_jobs rows for axiom_forensics:\n")
for r in rows:
    print(f"[{r['created_at']}] stage={r['stage']} run_id={r['run_id']}")
    print(f"  error: {r['error_message']}")
    print()

cur.close()
conn.close()
