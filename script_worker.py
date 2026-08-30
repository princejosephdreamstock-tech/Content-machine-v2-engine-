import socket
socket.setdefaulttimeout(120)
import os
import re
import ast
import json
import time
import random
import requests
from engine_common import (
    get_db, get_channel_config, push_job, push_failed_job,
    is_paused, redis_headers, REDIS_URL, call_nvidia_with_rotation,
    GatewayRequeueSignal,
)

STATE_FILE = os.path.expanduser("~/engine_v2/.q_script_lastid")
NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY")
NVIDIA_API_KEY_2 = os.environ.get("NVIDIA_API_KEY_2")
NVIDIA_KEYS = [k for k in (NVIDIA_API_KEY, NVIDIA_API_KEY_2) if k]

# 12-section outline (ported from production script_worker.py)
SECTION_OUTLINE = [
    ("HOOK", "Open with a curiosity gap or unexpected truth tied to the search query. Mention the guide early as a resource/solution."),
    ("SECTION 1", "Answer the first, most obvious part of what they searched. Real numbers, specific details."),
    ("SECTION 2", "Answer the next logical part of their question. Real numbers, specific details."),
    ("SECTION 3", "Answer another distinct angle of the question. Real numbers, specific details."),
    ("SECTION 4", "Go deeper on a commonly misunderstood part of the topic. Real numbers, specific details."),
    ("SECTION 5", "Cover a related sub-question people also ask. Real numbers, specific details."),
    ("SECTION 6", "Address a common mistake people make on this topic. Real numbers, specific details."),
    ("SECTION 7", "Give a practical, actionable step tied to the topic. Real numbers, specific details."),
    ("SECTION 8", "Cover an edge case or 'it depends' nuance of the topic. Real numbers, specific details."),
    ("SECTION 9", "Wrap the core answer content with the most important takeaway. Real numbers, specific details."),
    ("BONUS", "An unexpected insight they didn't ask for but will want (retention spike)."),
    ("CTA", "Loop back to the hook's problem, reinforce the guide as the complete solution. Clear soft CTA."),
]
TOTAL_SECTIONS = len(SECTION_OUTLINE)

CHAIN_SYSTEM_TEMPLATE = """You are writing a YouTube video script across multiple turns, one section per turn, following a fixed 12-section outline. It DIRECTLY ANSWERS a real search query people are googling, positioning a free guide as the solution.

INPUT SEARCH QUERY: {topic}
CONTEXT: Channel - {channel_name} | Guide - {guide_name} | Funnel - {funnel_context}

FIXED OUTLINE (do not add, remove, or reorder):
{outline_summary}

RULES THAT APPLY TO EVERY SECTION, NO EXCEPTIONS:
1. Treat the search phrase as a real question - answer it honestly and completely across the sections.
2. Never invent statistics, testimonials, customer names, or specific claims not grounded in general real knowledge of the topic.
3. Vary your opening line every section - never reuse the same sentence pattern twice in a row.
4. Continue naturally from the section you just wrote - build on it, don't restart the pitch.
5. Output ONLY one Python dict per turn, nothing else - no markdown, no preamble, no section label prefix.

Confirm you understand by replying with exactly: READY"""

SECTION_INSTRUCTION_TEMPLATE = """Now write the "{label}" section. Purpose: {task}

Output EXACTLY one Python dict, nothing else, in this exact format:
{{"caption": "PUNCHY TITLE (3-5 words)", "text": "2-4 sentences, real numbers/specifics, natural guide mention only if this section calls for it", "image_prompt": "Specific scene: action, framing, lighting, mood, ending with: high detail, realistic photography"}}

REMINDER - answer the real search query honestly. REMINDER - no invented stats/names/claims. REMINDER - output ONLY the dict, no markdown, no label, no commentary."""

CORRECTION_TEMPLATE = """That attempt had problems: {problems}. Rewrite the "{label}" section from scratch fixing these issues. Output ONLY one Python dict in the same format, nothing else."""

# Generic fallback used only if a channel's config_json has no guide_name/funnel
# (should not happen post-migration, but keeps this worker from hard-crashing).
GENERIC_FUNNEL = "Guide solves the core problem in this niche -> Later upsell: premium offer"


class RateLimitError(Exception):
    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


_nvidia_key_idx = 0

CHAIN_TIME_BUDGET_SECONDS = 750


class ChainTimeBudgetExceeded(Exception):
    pass


def _call_llm_nvidia(messages, max_tokens=400, timeout=60):
    """Routes NVIDIA calls through provider_gateway.call_provider() for
    shared, Redis-backed rate limiting and circuit breaking. Tries the
    primary model first, falls back to minimax-m3 on failure, both gated
    through the gateway so a bad NVIDIA stretch trips the shared breaker
    instead of hammering ungoverned."""
    from provider_gateway import call_provider

    if not NVIDIA_KEYS:
        raise RuntimeError("No NVIDIA_API_KEY set in environment")

    primary_payload = {
        "model": "nvidia/nemotron-3.5-lightning-30b-a3b",
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.7,
        "chat_template_kwargs": {"enable_thinking": False}
    }

    def _call_primary():
        resp = call_nvidia_with_rotation("https://integrate.api.nvidia.com/v1/chat/completions", primary_payload, timeout=timeout, dedup_key=None)
        return resp.json()["choices"][0]["message"]["content"].strip()

    primary_result = call_provider("nvidia", _call_primary)
    if primary_result.success:
        return primary_result.data

    print(f"[fallback] primary model gateway declined ({primary_result.requeue_after_seconds}s backoff), trying minimax-m3")

    fallback_payload = {
        "model": "minimaxai/minimax-m3",
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.7,
        "chat_template_kwargs": {"thinking_mode": "disabled"}
    }

    def _call_fallback():
        resp = call_nvidia_with_rotation("https://integrate.api.nvidia.com/v1/chat/completions", fallback_payload, timeout=timeout, dedup_key=None)
        return resp.json()["choices"][0]["message"]["content"].strip()

    fallback_result = call_provider("nvidia", _call_fallback)
    if fallback_result.success:
        return fallback_result.data

    print(f"[fallback] minimax-m3 gateway also declined ({fallback_result.requeue_after_seconds}s backoff)")
    raise GatewayRequeueSignal(
        retry_after=max(primary_result.requeue_after_seconds, fallback_result.requeue_after_seconds),
        message="nvidia gateway declined both primary and fallback models (script)"
    )


def _extract_dict(raw_text):
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("no dict found in model output")
    return ast.literal_eval(raw_text[start:end + 1])


def _validate_section(section, index, is_first, is_last):
    problems = []
    if not isinstance(section, dict):
        return ["output was not a dict"]
    for k in ("caption", "text", "image_prompt"):
        if k not in section or not isinstance(section[k], str) or not section[k].strip():
            problems.append(f"missing/empty key: {k}")
    if problems:
        return problems
    if re.search(r"(\*\*|^\s*[-*]\s|^#{1,6}\s)", section["text"], re.IGNORECASE | re.MULTILINE):
        problems.append("contains markdown formatting")
    if is_first and "guide" not in section["text"].lower():
        problems.append("guide mention missing in hook section")
    if is_last and "guide" not in section["text"].lower():
        problems.append("guide mention missing in CTA section")
    return problems


def retry_with_backoff(fn, max_attempts=5, base_delay=2, max_delay=60):
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            if attempt == max_attempts - 1:
                break
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None:
                sleep_time = min(retry_after + random.uniform(0.5, 2.0), max_delay * 4)
            else:
                delay = min(base_delay * (2 ** attempt), max_delay)
                jitter = delay * random.uniform(-0.2, 0.2)
                sleep_time = max(0.5, delay + jitter)
            time.sleep(sleep_time)
    raise last_exc


def _generate_chain(topic, channel_id, config):
    """Config-driven version of production's _generate_chain — pulls
    guide_name/funnel from the channel's DB config instead of a hardcoded dict."""
    guide_name = config.get("guide_name", channel_id)
    funnel_context = config.get("funnel", GENERIC_FUNNEL)
    outline_summary = "\n".join(f"- {label}: {task}" for label, task in SECTION_OUTLINE)
    system_prompt = CHAIN_SYSTEM_TEMPLATE.format(
        topic=topic,
        channel_name=channel_id,
        guide_name=guide_name,
        funnel_context=funnel_context,
        outline_summary=outline_summary,
    )

    chain_start = time.time()

    def check_budget(where):
        elapsed = time.time() - chain_start
        if elapsed > CHAIN_TIME_BUDGET_SECONDS:
            raise ChainTimeBudgetExceeded(f"{channel_id}: exceeded {CHAIN_TIME_BUDGET_SECONDS}s budget at {where} (elapsed {elapsed:.0f}s)")

    print(f"[script_worker] {channel_id}: starting chain, {TOTAL_SECTIONS} sections")
    messages = [{"role": "user", "content": system_prompt}]
    print(f"[script_worker] {channel_id}: sending ack/system prompt call")
    ack = _call_llm_nvidia(messages, max_tokens=20, timeout=60)
    print(f"[script_worker] {channel_id}: ack received ({len(ack)} chars)")
    messages.append({"role": "assistant", "content": ack})

    sections = []
    for i, (label, task) in enumerate(SECTION_OUTLINE):
        check_budget(f"before section {i+1}/{TOTAL_SECTIONS}")
        is_first = (i == 0)
        is_last = (i == TOTAL_SECTIONS - 1)
        instruction = SECTION_INSTRUCTION_TEMPLATE.format(label=label, task=task)
        attempt_messages = messages + [{"role": "user", "content": instruction}]

        section = None
        for attempt in range(2):
            check_budget(f"section {i+1}/{TOTAL_SECTIONS} attempt {attempt+1}")
            print(f"[script_worker] {channel_id}: section {i+1}/{TOTAL_SECTIONS} '{label}' attempt {attempt+1}/2 - calling LLM")
            raw = _call_llm_nvidia(attempt_messages, max_tokens=400, timeout=60)
            print(f"[script_worker] {channel_id}: section {i+1}/{TOTAL_SECTIONS} '{label}' attempt {attempt+1}/2 - got {len(raw)} chars")
            try:
                section = _extract_dict(raw)
                problems = _validate_section(section, i, is_first, is_last)
            except Exception as e:
                section = None
                problems = [f"parse error: {e}"]

            if not problems:
                break

            if attempt == 0:
                correction = CORRECTION_TEMPLATE.format(problems="; ".join(problems), label=label)
                attempt_messages = attempt_messages + [
                    {"role": "assistant", "content": raw},
                    {"role": "user", "content": correction},
                ]
            else:
                raise ValueError(f"{label} failed validation after retry: {'; '.join(problems)}")

        messages.append({"role": "user", "content": instruction})
        messages.append({"role": "assistant", "content": json.dumps(section)})
        sections.append(section)
        print(f"[script_worker] {channel_id}: section {i+1}/{TOTAL_SECTIONS} '{label}' complete")

    return sections


def generate_script(channel_id, topic):
    """Pulls guide_name/funnel from config_json (falls back to niche-only
    generic funnel text if a channel hasn't set one). No per-channel .py dicts.
    Runs the chain once — inner per-section retries plus the time budget
    already absorb transient failures, so we no longer re-run the WHOLE
    chain (and re-burn every already-good section) on one late failure."""
    config = get_channel_config(channel_id)
    try:
        sections = _generate_chain(topic, channel_id, config)
    except ChainTimeBudgetExceeded as e:
        print(f"[script_worker] {channel_id}: {e}")
        raise
    return [
        {"caption": s["caption"], "voiceover_text": s["text"], "image_prompt": s["image_prompt"]}
        for s in sections
    ]


def read_new_jobs():
    last_id = "0"
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            last_id = f.read().strip() or "0"
    r = requests.get(f"{REDIS_URL}/xrange/q.script/({last_id}/+", headers=redis_headers, timeout=30)
    return r.json().get("result", []), last_id


def save_last_id(entry_id):
    with open(STATE_FILE, "w") as f:
        f.write(entry_id)


def process_job(run_id, channel_id, topic):
    if is_paused(channel_id):
        print(f"  channel {channel_id} is paused (quota), re-queueing run {run_id}")
        push_job("q.script", run_id, channel_id)
        return
    try:
        sections = generate_script(channel_id, topic)
    except GatewayRequeueSignal as e:
        print(f"  gateway requested requeue for run {run_id} ({e.retry_after}s hint), re-queueing")
        push_job("q.script", run_id, channel_id)
        return
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT status FROM runs WHERE run_id=%s", (run_id,))
    row = cur.fetchone()
    if row and row[0] not in ("PLANNED", "SCRIPTED"):
        print(f"  run {run_id} already past SCRIPTED (status={row[0]}), skipping duplicate q.tts push")
        cur.close()
        conn.close()
        return
    cur.execute(
        "UPDATE runs SET status='SCRIPTED', script_json=%s, updated_at=now() WHERE run_id=%s",
        (json.dumps(sections), run_id),
    )
    conn.commit()
    cur.close()
    conn.close()
    push_job("q.tts", run_id, channel_id)
    print(f"[script_worker] {channel_id} run {run_id} -> SCRIPTED ({len(sections)} sections)")


def main_loop():
    while True:
        entries, last_id = read_new_jobs()
        for entry_id, fields in entries:
            job = {}
            try:
                field_dict = dict(zip(fields[::2], fields[1::2]))
                job = json.loads(field_dict.get("job", "{}"))
                run_id = job["run_id"]
                channel_id = job["channel_id"]
                topic = job.get("topic", channel_id)
                process_job(run_id, channel_id, topic)
            except Exception as e:
                push_failed_job(job.get("run_id"), job.get("channel_id"), "script", e, payload=fields)
            save_last_id(entry_id)
        time.sleep(5)


if __name__ == "__main__":
    main_loop()
