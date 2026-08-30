#!/bin/bash
cd ~/engine_v2
set -a
source /home/ec2-user/pipeline.env
set +a
mkdir -p ~/logs

MAIN_WORKERS="script_worker.py tts_worker.py image_worker.py assembly_worker.py metadata_worker.py thumbnail_worker.py upload_dispatcher.py upload_worker.py"
SHORTS_WORKERS="shorts_scheduler.py shorts_script_worker.py shorts_tts_worker.py shorts_assembly_worker.py shorts_upload_worker.py"

notify_discord_bash() {
    local tagged="[${VM_NAME:-unknown-vm}] $1"
    curl -s -X POST -H "Content-Type: application/json" \
        -d "{\"content\": $(python3 -c "import json,sys; print(json.dumps(sys.argv[1]))" "$tagged")}" \
        "$DISCORD_WEBHOOK_URL" > /dev/null 2>&1
}

handle_timeout() {
    local stage="$1"
    local summary="$2"
    local tmo="$3"
    local run_id
    run_id=$(echo "$summary" | grep -oE "run_id=[a-f0-9-]+" | head -1 | cut -d= -f2)
    local channel_id
    channel_id=$(echo "$summary" | grep -oE "channel_id=[a-z_]+" | head -1 | cut -d= -f2)
    if [ -z "$run_id" ]; then
        echo "===> $(date '+%H:%M:%S') timeout in $stage but no run_id found, cannot retry or record" >> ~/logs/pipeline_v2.log
        return
    fi
    python3 -c "
import sys
sys.path.insert(0, '/home/ec2-user/engine_v2')
from engine_common import get_db, push_job
conn = get_db()
cur = conn.cursor()
cur.execute(\"SELECT retry_count FROM runs WHERE run_id=%s\", ('$run_id',))
row = cur.fetchone()
retry_count = (row[0] or 0) if row else 0
MAX_RETRIES = 3
if retry_count < MAX_RETRIES:
    cur.execute(
        \"UPDATE runs SET retry_count=%s, error=%s, updated_at=now() WHERE run_id=%s\",
        (retry_count + 1, 'Timed out after ${tmo}s in $stage, retry ' + str(retry_count + 1) + '/' + str(MAX_RETRIES), '$run_id')
    )
    conn.commit()
    stage_queue_map = {
        'script_worker.py': 'q.script',
        'tts_worker.py': 'q.tts',
        'image_worker.py': 'q.image',
        'assembly_worker.py': 'q.assembly',
    }
    q = stage_queue_map.get('$stage')
    if q:
        push_job(q, '$run_id', '$channel_id' or None, attempt=retry_count + 1)
        print(f'requeued {\"$run_id\"} to {q}, attempt {retry_count+1}/{MAX_RETRIES}')
else:
    cur.execute(
        \"INSERT INTO failed_jobs (run_id, channel_id, stage, error_message) VALUES (%s, %s, %s, %s)\",
        ('$run_id', '$channel_id' or None, '$stage', 'Timed out after ${tmo}s, exceeded max retries (' + str(MAX_RETRIES) + ')')
    )
    cur.execute(
        \"UPDATE runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s\",
        ('Timed out repeatedly in $stage, gave up after ' + str(MAX_RETRIES) + ' retries', '$run_id')
    )
    conn.commit()
    print(f'{\"$run_id\"} exceeded max retries, moved to failed_jobs')
cur.close()
conn.close()
" >> ~/logs/pipeline_v2.log 2>&1
}

shorts_handle_timeout() {
    local stage="$1"
    local summary="$2"
    local tmo="$3"
    local run_id
    run_id=$(echo "$summary" | grep -oE "run_id=[a-f0-9-]+" | head -1 | cut -d= -f2)
    if [ -z "$run_id" ]; then
        echo "===> $(date '+%H:%M:%S') timeout in $stage but no run_id found, cannot retry or record" >> ~/logs/pipeline_v2.log
        return
    fi
    python3 -c "
import sys
sys.path.insert(0, '/home/ec2-user/engine_v2')
from engine_common import get_db
conn = get_db()
cur = conn.cursor()
cur.execute(\"SELECT retry_count FROM shorts_runs WHERE run_id=%s\", ('$run_id',))
row = cur.fetchone()
retry_count = (row[0] or 0) if row else 0
MAX_RETRIES = 3
if retry_count < MAX_RETRIES:
    cur.execute(
        \"UPDATE shorts_runs SET retry_count=%s, error=%s, updated_at=now() WHERE run_id=%s\",
        (retry_count + 1, 'Timed out after ${tmo}s in $stage, retry ' + str(retry_count + 1) + '/' + str(MAX_RETRIES), '$run_id')
    )
    conn.commit()
    print(f'{\"$run_id\"} will retry naturally next pass, attempt {retry_count+1}/{MAX_RETRIES}')
else:
    cur.execute(
        \"UPDATE shorts_runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s\",
        ('Timed out repeatedly in $stage, gave up after ' + str(MAX_RETRIES) + ' retries', '$run_id')
    )
    conn.commit()
    print(f'{\"$run_id\"} exceeded max retries, marked FAILED in shorts_runs')
cur.close()
conn.close()
" >> ~/logs/pipeline_v2.log 2>&1
}

run_stage() {
    local tmo=300
    if [ "$1" = "image_worker.py" ] || [ "$1" = "tts_worker.py" ] || [ "$1" = "script_worker.py" ]; then
        tmo=900
    fi
    if [ "$1" = "assembly_worker.py" ]; then
        tmo=3600
    fi
    echo "===> $(date '+%H:%M:%S') STARTING $1" >> ~/logs/pipeline_v2.log
    local output
    output=$(flock /home/ec2-user/pipeline_global.lock timeout $tmo python3 -u "$1" 2>&1)
    code=$?
    echo "$output" >> ~/logs/pipeline_v2.log
    local summary
    summary=$(echo "$output" | grep -oE "run_id=[a-f0-9-]+|channel_id=[a-z_]+" | sort -u | tr '\n' ' ')
    if [ -z "$summary" ]; then
        summary=$(echo "$output" | tail -c 200)
    fi
    if [ $code -eq 124 ]; then
        echo "===> $(date '+%H:%M:%S') $1 TIMED OUT after ${tmo}s" >> ~/logs/pipeline_v2.log
        notify_discord_bash "⏱️ [v2] $1 TIMED OUT after ${tmo}s | $summary"
        handle_timeout "$1" "$summary" "$tmo"
    else
        echo "===> $(date '+%H:%M:%S') $1 finished (exit $code)" >> ~/logs/pipeline_v2.log
        if [ $code -ne 0 ]; then
            notify_discord_bash "❌ [v2] $1 FAILED (exit $code) | $summary"
        elif ! echo "$output" | grep -qiE "no new jobs|no pending|nothing to do"; then
            notify_discord_bash "▶️ [v2] $1 did work | $summary"
        fi
    fi
}

shorts_run_stage() {
    local tmo=900
    if [ "$1" = "shorts_assembly_worker.py" ]; then
        tmo=3600
    fi
    if [ "$1" = "shorts_upload_worker.py" ]; then
        tmo=1800
    fi
    if [ "$1" = "shorts_scheduler.py" ]; then
        tmo=300
    fi
    echo "===> $(date '+%H:%M:%S') STARTING $1" >> ~/logs/pipeline_v2.log
    local output
    output=$(flock /home/ec2-user/pipeline_global.lock timeout $tmo python3 -u "$1" 2>&1)
    code=$?
    echo "$output" >> ~/logs/pipeline_v2.log
    local summary
    summary=$(echo "$output" | grep -oE "run_id=[a-f0-9-]+|channel_id=[a-z_]+" | sort -u | tr '\n' ' ')
    if [ -z "$summary" ]; then
        summary=$(echo "$output" | tail -c 200)
    fi
    if [ $code -eq 124 ]; then
        echo "===> $(date '+%H:%M:%S') $1 TIMED OUT after ${tmo}s" >> ~/logs/pipeline_v2.log
        notify_discord_bash "⏱️ [v2-shorts] $1 TIMED OUT after ${tmo}s | $summary"
        shorts_handle_timeout "$1" "$summary" "$tmo"
    else
        echo "===> $(date '+%H:%M:%S') $1 finished (exit $code)" >> ~/logs/pipeline_v2.log
        if [ $code -ne 0 ]; then
            notify_discord_bash "❌ [v2-shorts] $1 FAILED (exit $code) | $summary"
        elif ! echo "$output" | grep -qiE "no new jobs|no pending|nothing to do"; then
            notify_discord_bash "▶️ [v2-shorts] $1 did work | $summary"
        fi
    fi
}

while true; do
    for w in $MAIN_WORKERS; do
        run_stage "$w"
    done
    for w in $SHORTS_WORKERS; do
        shorts_run_stage "$w"
    done
    echo "$(date): full v2 pass complete, sleeping" >> ~/logs/pipeline_v2.log
    sleep 60
done
