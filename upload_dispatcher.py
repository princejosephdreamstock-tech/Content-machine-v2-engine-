import os
from engine_common import get_db, push_job
import socket
socket.setdefaulttimeout(120)  # prevent indefinite hangs on any blocking network call

DB_URL = os.environ["PIPELINE_DB_URL"]


def main():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT run_id, channel_id FROM runs
        WHERE video_path IS NOT NULL
        AND metadata_json IS NOT NULL
        AND youtube_video_id IS NULL
        AND (scheduled_for_time IS NULL OR scheduled_for_time <= now())
        AND status NOT IN ('FAILED')
    """)
    rows = cur.fetchall()

    for run_id, channel_id in rows:
        print(f"dispatching run {run_id} ({channel_id}) to q.upload")
        push_job("q.upload", run_id, channel_id)

    if not rows:
        print("no runs due for upload dispatch")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
