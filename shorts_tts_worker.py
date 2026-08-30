import psycopg2
import os
import subprocess
import re

DB_URL = os.environ["PIPELINE_DB_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT run_id, script, channel_id FROM shorts_runs
    WHERE status = 'TTS_PENDING'
    LIMIT 1
""")
result = cur.fetchone()
if not result:
    print("No pending TTS runs")
    conn.close()
    exit()

run_id, script, channel_id = result

cur.execute("SELECT config_json ->> 'voice_name_edge' FROM channels WHERE channel_id = %s", (channel_id,))
voice_row = cur.fetchone()
voice = voice_row[0] if voice_row and voice_row[0] else "en-US-EmmaMultilingualNeural"
print(f"Channel: {channel_id}, voice: {voice}")

script_clean = script
for line in script.split('\n'):
    if line.strip().startswith('#'):
        script_clean = script_clean.replace(line, '')

script_clean = re.sub(r'\n\n+', '\n', script_clean).strip()

print(f"Original length: {len(script)} chars")
print(f"Cleaned length: {len(script_clean)} chars")
print(f"Cleaned script:\n{script_clean}\n")

audio_path = f"/home/ec2-user/shorts_pipeline/audio_{run_id}.mp3"

script_escaped = script_clean.replace('"', '\\"')
cmd = [
    "/home/ec2-user/.local/bin/edge-tts",
    "--voice", voice,
    "--text", script_clean,
    "--write-media", audio_path,
]

subprocess.run(cmd, check=True)

cur.execute("""
    UPDATE shorts_runs
    SET audio_path = %s, status = 'ASSEMBLY_PENDING', updated_at = now()
    WHERE run_id = %s
""", (audio_path, run_id))
conn.commit()
print(f"TTS generated: {audio_path}")
cur.close()
conn.close()
