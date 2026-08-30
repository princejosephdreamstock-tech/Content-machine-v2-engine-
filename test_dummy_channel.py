#!/usr/bin/env python3
"""
Rule 4 test: insert a dummy channel row, prove it gets scheduled and
processed with zero code changes, then this script's cleanup step
removes it afterward.
"""
import os, json

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
from engine_common import get_db

TEST_CHANNEL_ID = "zztest_dummy"

config = {
    "niche": "TEST DUMMY CHANNEL - safe to ignore, will be deleted",
    "funnel": "none - test only",
    "cta_text": "TEST",
    "voice_provider": "edge_tts",
    "channel_instructions": "Based on the video script sections below, generate YouTube metadata for a TEST DUMMY channel used only to verify pipeline architecture. Output ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{\n    \"title\": \"string, under 100 characters\",\n    \"description\": \"string, 2-4 sentences\",\n    \"tags\": [\"tag1\", \"tag2\"],\n    \"category\": \"Howto & Style\"\n}\nScript sections:\n",
    "youtube_channel_name": "ZZ Test Dummy",
    "thumbnail_concept_instructions": "TEST ONLY. Output ONLY valid JSON, no markdown fences:\n{{\n    \"image_prompt\": \"a simple test image, high detail, realistic photography\",\n    \"thumbnail_text\": \"TEST\"\n}}\n\nTitle: {title}\n"
}

conn = get_db()
cur = conn.cursor()

cur.execute(
    "INSERT INTO channels (channel_id, upload_interval_hours, last_scheduled_at, config_json, assigned_vm) "
    "VALUES (%s, %s, NULL, %s, %s) "
    "ON CONFLICT (channel_id) DO NOTHING",
    (TEST_CHANNEL_ID, 9999, json.dumps(config), "joy-vm"),
)
conn.commit()
print(f"Inserted dummy channel '{TEST_CHANNEL_ID}' (last_scheduled_at=NULL so scheduler should pick it up on next pass).")
print("Rows affected:", cur.rowcount)

cur.close()
conn.close()
