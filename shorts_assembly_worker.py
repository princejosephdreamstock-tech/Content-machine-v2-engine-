import psycopg2
import os
import subprocess

DB_URL = os.environ["PIPELINE_DB_URL"]
DEFAULT_DEMO_VIDEO = "/home/ec2-user/shorts_demo_videos/demo.mp4"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT run_id, audio_path, channel_id FROM shorts_runs
    WHERE status = 'ASSEMBLY_PENDING'
    LIMIT 1
""")
result = cur.fetchone()
if not result:
    print("No pending assembly runs")
    conn.close()
    exit()

run_id, audio_path, channel_id = result

cur.execute("SELECT config_json ->> 'demo_video_path' FROM channels WHERE channel_id = %s", (channel_id,))
video_row = cur.fetchone()
demo_video = video_row[0] if video_row and video_row[0] else DEFAULT_DEMO_VIDEO
print(f"Channel: {channel_id}, video: {demo_video}")

video_output = f"/home/ec2-user/shorts_pipeline/video_{run_id}.mp4"

# ffmpeg: merge audio to video (replace audio track)
cmd = f"ffmpeg -i {demo_video} -i {audio_path} -c:v copy -c:a aac -map 0:v:0 -map 1:a:0 -y {video_output}"
subprocess.run(cmd, shell=True, check=True)

cur.execute("""
    UPDATE shorts_runs
    SET video_path = %s, status = 'UPLOAD_PENDING', updated_at = now()
    WHERE run_id = %s
""", (video_output, run_id))
conn.commit()
print(f"Assembled video: {video_output}")
cur.close()
conn.close()
