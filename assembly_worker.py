import os
import json
import subprocess
import time
import requests
import psycopg2
from engine_common import retry_with_backoff, rclone_upload, get_channel_config
import socket
socket.setdefaulttimeout(120)  # prevent indefinite hangs on any blocking network call

DB_URL = os.environ.get("PIPELINE_DB_URL")
REDIS_URL = os.environ.get("UPSTASH_REDIS_REST_URL")
REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

headers = {"Authorization": f"Bearer {REDIS_TOKEN}"}
STATE_FILE = os.path.expanduser("~/.q_assembly_lastid")
VIDEO_DIR = os.path.expanduser("~/pipeline_videos_v2")
TMP_DIR = os.path.expanduser("~/pipeline_tmp_v2")

DEFAULT_FONT_PATH = "/usr/share/fonts/dejavu-sans-fonts/DejaVuSans-Bold.ttf"
DEFAULT_FPS = 25
DEFAULT_TARGET_W = 1920
DEFAULT_TARGET_H = 1080

os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)


def run(cmd, max_attempts=3):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            subprocess.run(cmd, check=True)
            return
        except subprocess.CalledProcessError as e:
            last_exc = e
            print(f"    ffmpeg attempt {attempt}/{max_attempts} failed (exit {e.returncode}), waiting before retry...")
            time.sleep(10)
    raise last_exc


def get_duration(path):
    out = subprocess.run(
        ["/usr/local/bin/ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True, check=True
    )
    return float(out.stdout.strip())


def build_section(run_id, idx, section, tmp_dir, font_path, fps, target_w, target_h):
    image_path = section["image_path"]
    audio_path = section["audio_path"]
    duration = section.get("duration") or get_duration(audio_path)

    caption_path = os.path.join(tmp_dir, f"sec_{idx}_caption.txt")
    with open(caption_path, "w") as f:
        f.write(section["caption"])

    num_frames = int(duration * fps)
    video_path = os.path.join(tmp_dir, f"sec_{idx}_video.mp4")
    vf = (
        f"scale={int(target_w*1.3)}:{int(target_h*1.3)}:force_original_aspect_ratio=increase,"
        f"crop={int(target_w*1.3)}:{int(target_h*1.3)},"
        f"zoompan=z='min(zoom+0.0012,1.4)':d={num_frames}:s={target_w}x{target_h}:fps={fps},"
        f"drawtext=fontfile={font_path}:textfile={caption_path}:"
        f"fontsize=64:fontcolor=white:box=1:boxcolor=black@0.55:boxborderw=24:"
        f"x=(w-text_w)/2:y=h-220"
    )
    run(["/usr/local/bin/ffmpeg", "-y", "-loglevel", "warning", "-threads", "1", "-loop", "1", "-i", image_path, "-t", str(duration),
         "-vf", vf, "-pix_fmt", "yuv420p", "-r", str(fps), video_path])

    final_path = os.path.join(tmp_dir, f"sec_{idx}_final.mp4")
    run(["/usr/local/bin/ffmpeg", "-y", "-loglevel", "warning", "-threads", "1", "-i", video_path, "-i", audio_path,
         "-c:v", "libx264", "-preset", "veryfast",
         "-c:a", "aac", "-shortest", final_path])
    return final_path


def push_job(queue_name, run_id, channel_id):
    payload = json.dumps({"run_id": run_id, "channel_id": channel_id, "attempt": 0})
    r = requests.post(f"{REDIS_URL}/xadd/{queue_name}/*/job/{payload}", headers=headers)
    return r.json()


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.assembly/({last_id}/+", headers=headers)
    return r.json().get("result", [])


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def get_conn():
    return psycopg2.connect(DB_URL)


def process_job(run_id, channel_id):
    conn = get_conn()
    config = get_channel_config(channel_id)

    if config.get("skip_assembly_worker", False):
        print(f"  run {run_id} channel {channel_id} is flagged skip_assembly_worker, skipping")
        conn.rollback()
        conn.close()
        return

    font_path = config.get("assembly_font_path", DEFAULT_FONT_PATH)
    fps = config.get("assembly_fps", DEFAULT_FPS)
    target_w = config.get("assembly_target_w", DEFAULT_TARGET_W)
    target_h = config.get("assembly_target_h", DEFAULT_TARGET_H)

    cur = conn.cursor()
    cur.execute("SELECT script_json, video_path FROM runs WHERE run_id = %s", (run_id,))
    row = cur.fetchone()
    if row is None or not row[0]:
        print(f"  run {run_id} missing script_json, skipping")
        conn.rollback()
        return

    sections, existing_video = row
    if existing_video:
        print(f"  run {run_id} already assembled, forwarding to q.metadata")
        conn.commit()
        push_job("q.metadata", run_id, channel_id)
        return

    if not all("image_path" in s and "audio_path" in s for s in sections):
        print(f"  run {run_id} missing image/audio paths, cannot assemble yet")
        conn.rollback()
        return

    run_tmp_dir = os.path.join(TMP_DIR, run_id)
    os.makedirs(run_tmp_dir, exist_ok=True)

    try:
        final_segments = []
        for idx, section in enumerate(sections):
            print(f"  building section {idx}...")
            seg_path = build_section(run_id, idx, section, run_tmp_dir, font_path, fps, target_w, target_h)
            final_segments.append(seg_path)

        concat_list = os.path.join(run_tmp_dir, "final_concat.txt")
        with open(concat_list, "w") as f:
            for p in final_segments:
                f.write(f"file '{os.path.abspath(p)}'\n")

        final_path = os.path.join(VIDEO_DIR, f"{run_id}.mp4")
        run(["/usr/local/bin/ffmpeg", "-y", "-f", "concat", "-safe", "0",
             "-i", concat_list, "-c", "copy", final_path])
    except Exception as e:
        cur.execute("UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e)[:500], run_id))
        cur.execute("INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'assembly',%s)", (run_id, str(e)[:500]))
        conn.commit()
        print(f"  run {run_id} FAILED during assembly: {e}")
        return

    if config.get("upload_to_s3", False):
        s3_key = f"{channel_id}/{run_id}/video.mp4"
        retry_with_backoff(lambda: rclone_upload(final_path, s3_key), max_attempts=3)

    cur.execute(
        "UPDATE runs SET video_path=%s, status='ASSEMBLED', updated_at=now() WHERE run_id=%s",
        (final_path, run_id)
    )
    cur.execute(
        "INSERT INTO run_logs (run_id, stage, message) VALUES (%s,'assembly',%s)",
        (run_id, f"assembled video -> {final_path}")
    )
    conn.commit()
    print(f"  run {run_id} ASSEMBLED -> {final_path}")
    push_job("q.metadata", run_id, channel_id)


def main():
    entries = read_new_jobs()
    if not entries:
        print("no new jobs on q.assembly")
        return
    for entry_id, fields in entries:
        field_dict = dict(zip(fields[::2], fields[1::2]))
        payload = json.loads(field_dict["job"])
        run_id = payload["run_id"]
        channel_id = payload["channel_id"]
        print(f"processing job {entry_id}: run_id={run_id} channel_id={channel_id}")
        try:
            process_job(run_id, channel_id)
        except Exception as e:
            print(f"  UNCAUGHT error processing run_id={run_id}: {e}")
        finally:
            save_last_id(entry_id)

if __name__ == "__main__":
    main()
