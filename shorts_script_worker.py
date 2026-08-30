import psycopg2
import os
import requests
from pipeline_common import retry_with_backoff, get_nvidia_key, RateLimitError, call_nvidia_with_rotation

DB_URL = os.environ["PIPELINE_DB_URL"]
_nvidia_key_idx = 0

GUIDE_INFO = {
    "houseplant": {
        "name": "Plant Care Guide",
        "url": "https://dreamprince.gumroad.com/l/Houseplantcare",
        "product_context": "Plant care education and resources",
        "funnel": "Guide teaches plant care fundamentals → Later upsell: premium courses, plant subscription service, or care consulting"
    },
    "homeschool": {
        "name": "Homeschool Starting Guide",
        "url": "https://dreamprince.gumroad.com/l/homeschool-starting-point",
        "product_context": "Homeschool curriculum and support system",
        "funnel": "Guide shows the framework → Later upsell: premium curriculum, teacher community access, or planning software"
    },
    "ai_for_business": {
        "name": "YouTube Automation Guide",
        "url": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "product_context": "Content Machine - automated YouTube video generation (6+ videos/day on autopilot)",
        "funnel": "Guide teaches DIY setup → Later upsell: done-for-you service or VIP access"
    },
    "content_machine_lucas": {
        "name": "YouTube Automation Guide",
        "url": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "product_context": "Content Machine - automated YouTube video generation (6+ videos/day on autopilot)",
        "funnel": "Guide teaches DIY setup → Later upsell: done-for-you service or VIP access"
    },
    "demo_shorts": {
        "name": "YouTube Automation Guide",
        "url": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "product_context": "Content Machine - automated YouTube video generation, speed/no-code/done-in-a-day angle",
        "funnel": "Guide teaches DIY setup → Later upsell: done-for-you service or VIP access"
    }
}

PROMPT_TEMPLATE_HOUSEPLANT = """WRITE A 30-45 SECOND YOUTUBE SHORTS SCRIPT for plant lovers searching: "{topic}"

THE FUNNEL:
- ENTRY: Plant Care Guide (shows them plants don't have to die, it's a learnable skill)
- PRODUCT BEHIND IT: Premium plant care education, resources, and community
- LATER UPSELL: Plant subscriptions, advanced care courses, 1-on-1 consulting

PSYCHOLOGICAL CONTEXT:
- Most people think they can't keep plants alive ("I kill everything")
- They waste money on dead plants and feel guilty
- They see others' thriving plants and think "I could never..."
- The guide is the AHA moment: plant care is simple rules, not a gift
- Position the guide as the permission slip to try again
- Plant the seed: mastery (and beautiful plants) is possible

STRUCTURE:
1. HOOK (3 sec): Start with the common belief. "Most people kill plants because they're doing ONE thing wrong."
2. REVEAL (8 sec): Show the possibility. "But plant care isn't magic - it's just understanding ONE specific thing about your plant. Our guide breaks down the exact framework successful plant parents use."
3. PROMISE (5 sec): Position guide as the shortcut. "You don't need a green thumb - you just need the right knowledge."
4. CURIOSITY + CTA (3 sec): Make them want to know more. "Grab the guide, learn what you've been missing, and watch your plants thrive."

TONE: Encouraging, like you're giving someone permission to try again. Honest. Practical.

CONSTRAINTS:
- 80-120 words total (30-45 seconds aloud)
- NO hashtags, emojis, fluff
- Raw script only
- End on hope/possibility, not desperation

OUTPUT ONLY THE SCRIPT."""

PROMPT_TEMPLATE_HOMESCHOOL = """WRITE A 30-45 SECOND YOUTUBE SHORTS SCRIPT for parents searching: "{topic}"

THE FUNNEL:
- ENTRY: Homeschool Starting Guide (shows overwhelmed parents that homeschooling IS manageable)
- PRODUCT BEHIND IT: Complete homeschool curriculum and support community
- LATER UPSELL: Premium curriculum packages, teacher community access, planning software

PSYCHOLOGICAL CONTEXT:
- Parents are overwhelmed: "I'm not qualified to teach," "Where do I even start?", "Will my kids fall behind?"
- They see homeschool families and think "I could never organize that"
- The guide is the permission slip: homeschooling has a framework, and it's learnable
- Position guide as the starting blueprint that makes it all click
- Plant the seed: they don't have to figure this out alone

STRUCTURE:
1. HOOK (3 sec): Start with the fear. "Most parents think homeschooling is impossible. That it takes a genius to manage."
2. REVEAL (8 sec): Show the reality. "But homeschooling is actually a system - the same framework successful families use. Our guide shows you exactly what to do, day one."
3. PROMISE (5 sec): Position guide as the confidence builder. "You don't need to be an educator - you just need the right structure."
4. CURIOSITY + CTA (3 sec): Make them feel empowered. "Grab the guide, see how simple it actually is, and start with confidence."

TONE: Reassuring, like you're speaking to a worried parent. Practical. Empowering.

CONSTRAINTS:
- 80-120 words (30-45 seconds aloud)
- NO hashtags, emojis, corporate tone
- Raw script only
- End on confidence, not fear

OUTPUT ONLY THE SCRIPT."""

PROMPT_TEMPLATE_LUCAS = """Write an original, high-converting 30-45 second YouTube Shorts script for creators searching: "{topic}"

WHAT YOU ARE SELLING - Content Machine, a real automated YouTube video production system:
- Runs entirely on autopilot: script writing, text-to-speech voiceover, AI image generation, video assembly, thumbnail creation, metadata/SEO, and upload - all automated end to end
- Produces 6+ full YouTube videos per day per channel, with zero manual editing
- Runs multiple faceless channels simultaneously from one system, each with its own niche and identity
- Built on real infrastructure (cloud servers, Python pipelines) - not a gimmick or a course full of theory, an actual working system
- Removes the two biggest bottlenecks for YouTube creators: time spent editing and inability to scale output
- The guide teaches the exact framework and buyer gets direct access to see how the system is built and how to run it themselves

YOUR JOB - write copy that sells hard and converts:
- Invent a sharp, specific, scroll-stopping hook tied directly to the topic "{topic}" - never generic, never recycled
- Use the real mechanics above as ammunition: cite specifics (automation of editing, multi-channel scale, daily output) to make the pitch credible and concrete, not vague hype
- Create real urgency and desire - make the viewer feel like they are behind if they keep doing this manually
- Push hard toward action: this is a direct-response sales script, not a soft educational video
- End with a strong, direct call to action to get the guide right now

REQUIREMENTS:
- 80-120 words total (about 30-45 seconds spoken aloud)
- No hashtags, emojis, or corporate-sounding phrases
- Do NOT describe the guide as free or low-cost - treat it as a valuable paid product worth the money
- Every script must sound different from the last - vary sentence structure, vocabulary, and angle each time
- Do not reuse the same hook, numbers, or phrasing across different scripts
- Write like a sharp, confident creator talking straight to camera, selling hard but sounding real - not a corporate ad

OUTPUT ONLY THE SCRIPT TEXT, NOTHING ELSE.
Do not include any preamble, explanation, or lead-in like "Here's a script" or "Here you go" - your entire response must be the spoken script itself, starting with the first word of the hook."""

def call_openrouter(topic, channel_id):
    global _nvidia_key_idx
    guide = GUIDE_INFO.get(channel_id, GUIDE_INFO["houseplant"])

    if channel_id == "houseplant":
        prompt = PROMPT_TEMPLATE_HOUSEPLANT.format(topic=topic)
    elif channel_id == "homeschool":
        prompt = PROMPT_TEMPLATE_HOMESCHOOL.format(topic=topic)
    elif channel_id in ("content_machine_lucas", "ai_for_business", "demo_shorts"):
        prompt = PROMPT_TEMPLATE_LUCAS.format(topic=topic)
    else:
        prompt = PROMPT_TEMPLATE_HOUSEPLANT.format(topic=topic)

    primary_payload = {
        "model": "nvidia/nemotron-3.5-lightning-30b-a3b",
        "chat_template_kwargs": {"enable_thinking": False},
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 250,
        "temperature": 0.8
    }
    try:
        resp = call_nvidia_with_rotation("https://integrate.api.nvidia.com/v1/chat/completions", primary_payload, timeout=60, dedup_key=None)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[fallback] primary model failed ({e}), trying minimax-m3")
        fallback_payload = {
            "model": "minimaxai/minimax-m3",
            "chat_template_kwargs": {"thinking_mode": "disabled"},
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 250,
            "temperature": 0.8
        }
        resp = call_nvidia_with_rotation("https://integrate.api.nvidia.com/v1/chat/completions", fallback_payload, timeout=60, dedup_key=None)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()

import re as _re

def _strip_preamble(script_text):
    """Removes any lead-in line the model adds before the actual script,
    e.g. 'Here's a script that fits your requirements:'"""
    lines = script_text.strip().split("\n")
    cleaned = []
    skipping_preamble = True
    for line in lines:
        stripped = line.strip().strip('"').strip()
        if skipping_preamble:
            if not stripped:
                continue
            if _re.match(r'^(here.?s|here is|sure|okay|ok|below is|this is)\b', stripped, _re.IGNORECASE) and stripped.endswith(":"):
                continue
            if _re.match(r'^(here.?s|here is|sure|okay|ok|below is|this is)\b.*(script|version|draft)', stripped, _re.IGNORECASE):
                continue
            skipping_preamble = False
        cleaned.append(stripped)
    return "\n".join(cleaned).strip()


def _validate_guide_mention(script_text):
    script_text = _strip_preamble(script_text)
    if "guide" not in script_text.lower():
        raise ValueError("Guide mention missing in shorts script")
    return script_text

def generate_short_script(topic, channel_id):
    return retry_with_backoff(lambda: _validate_guide_mention(call_openrouter(topic, channel_id)), max_attempts=5)

def process_run(run_id, channel_id):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    topic = channel_id
    
    try:
        script_text = generate_short_script(topic, channel_id)
    except Exception as e:
        print(f"  run {run_id} script generation failed: {e}")
        cur.execute("UPDATE shorts_runs SET status='FAILED', error=%s, updated_at=now() WHERE run_id=%s", (str(e)[:1000], run_id))
        conn.commit()
        conn.close()
        return
    
    word_count = len(script_text.split())
    guide = GUIDE_INFO.get(channel_id, GUIDE_INFO["houseplant"])
    cur.execute(
        "UPDATE shorts_runs SET script=%s, topic=%s, status='TTS_PENDING', updated_at=now() WHERE run_id=%s",
        (script_text, topic, run_id)
    )
    conn.commit()
    print(f"  run {run_id} SCRIPT_READY ({word_count} words) → {guide['name']}")
    conn.close()

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    
    cur.execute("SELECT run_id, channel_id FROM shorts_runs WHERE status='SCRIPT_PENDING' ORDER BY created_at ASC LIMIT 10")
    runs = cur.fetchall()
    conn.close()
    
    if not runs:
        print("no pending shorts runs")
        return
    
    for (run_id, channel_id) in runs:
        print(f"processing shorts run: {run_id}")
        process_run(run_id, channel_id)

if __name__ == "__main__":
    main()
