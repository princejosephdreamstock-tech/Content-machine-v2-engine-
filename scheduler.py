import os
import random
from engine_common import get_db, push_job, is_paused, notify_discord

BATCH_SIZE = 20

# No channel exclusion list here by design -- the `channels` table on this
# VM's own DB is the single source of truth for what gets scheduled. Adding
# or removing a channel from this engine is purely a DB operation (insert or
# delete a row in `channels`), never a code change.


def main():
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT channel_id FROM channels
        WHERE (last_scheduled_at IS NULL
           OR now() >= last_scheduled_at + (upload_interval_hours || ' hours')::interval)
        """
    )
    due_channels = cur.fetchall()

    for (channel_id,) in due_channels:
        if is_paused(channel_id):
            print(f"  {channel_id} is paused (quota), skipping new run creation")
            continue

        run_ids = []
        for i in range(1):
            cur.execute(
                """
                INSERT INTO runs (channel_id, status, scheduled_for_time)
                VALUES (%s, 'PLANNED', now())
                RETURNING run_id
                """,
                (channel_id,),
            )
            run_id = str(cur.fetchone()[0])
            run_ids.append(run_id)
            push_job("q.script", run_id, channel_id)
            print(f"Created run {run_id} for channel {channel_id}, ready immediately")

        next_interval_hours = random.uniform(20, 28)
        cur.execute(
            "UPDATE channels SET last_scheduled_at = now(), upload_interval_hours = %s WHERE channel_id = %s",
            (next_interval_hours, channel_id),
        )

        notify_discord(f"{BATCH_SIZE} videos queued for {channel_id} (no stagger)")

    conn.commit()
    cur.close()
    conn.close()
    print("Scheduler pass complete.")


if __name__ == "__main__":
    main()
