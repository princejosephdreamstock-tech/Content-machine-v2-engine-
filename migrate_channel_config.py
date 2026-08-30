import os, json, psycopg2

DB_URL = os.environ["PIPELINE_DB_URL"]
conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur = conn.cursor()

# 1. NVIDIA keys -> system_defaults
nvidia_keys = [os.environ["NVIDIA_API_KEY"]]
if os.environ.get("NVIDIA_API_KEY_2"):
    nvidia_keys.append(os.environ["NVIDIA_API_KEY_2"])

cur.execute("""
    INSERT INTO system_defaults (key, value) VALUES ('nvidia_api_keys', %s)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
""", (json.dumps(nvidia_keys),))
print(f"nvidia_api_keys set: {len(nvidia_keys)} key(s)")

# 2. Per-channel config merges
updates = {
    "houseplant": {
        "guide_name": "Plant Care Guide",
        "guide_url": "https://dreamprince.gumroad.com/l/Houseplantcare",
        "funnel": "Guide shows them plant care is learnable → Later upsell: premium courses, plant products, care consulting",
        "cta_text": "Get the full Plant Care Guide",
        "guide_link": "https://dreamprince.gumroad.com/l/Houseplantcare",
        "channel_instructions": "Based on the video script sections below, generate YouTube metadata for a houseplant care channel.\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{\n    \"title\": \"string, under 100 characters, punchy, includes the plant name\",\n    \"description\": \"string, 2-4 sentences, educational houseplant care content\",\n    \"tags\": [\"tag1\", \"tag2\", \"...\"],\n    \"category\": \"Howto & Style\"\n}\nScript sections:\n",
        "thumbnail_concept_instructions": "Based on this video title, generate YouTube thumbnail concept data that maximizes click-through rate through curiosity and pattern-interrupt.\n\nPSYCHOLOGY: The best plant-care thumbnails create an open loop (something looks wrong, urgent, or surprising that the viewer needs to resolve) or show a stark before/after contrast. Avoid generic \"pretty plant\" imagery - it doesn't stop the scroll.\n\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{{\n    \"image_prompt\": \"one specific, visually dramatic moment under 40 words - prefer: a close-up of visible plant damage/distress (yellowing, wilting, spots) next to a healthy version for contrast, OR a hand pointing at/touching the exact problem area, OR a dramatic before/after split. Specify camera angle and framing. End with 'high detail, realistic plant photography, dramatic lighting, shallow depth of field'\",\n    \"thumbnail_text\": \"2-4 WORDS MAX, all caps, creates an open loop or urgency - a number, a warning, or a surprising claim the video will resolve (e.g. 'THIS KILLS PLANTS', '2 WEEKS LEFT', 'THE #1 MISTAKE') - never generic, never the full title, never a full sentence\"\n}}\n\nTitle: {title}\n",
    },
    "homeschool": {
        "guide_name": "Homeschool Starting Guide",
        "guide_url": "https://dreamprince.gumroad.com/l/homeschool-starting-point",
        "funnel": "Guide provides the framework → Later upsell: premium curriculum, teacher community, planning software",
        "cta_text": "Get the Homeschool Starting Guide",
        "guide_link": "https://dreamprince.gumroad.com/l/homeschool-starting-point",
        "channel_instructions": "Based on the video script sections below, generate YouTube metadata for a homeschool education channel.\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{\n    \"title\": \"string, under 100 characters, punchy, focuses on learning/education\",\n    \"description\": \"string, 2-4 sentences, homeschool curriculum or learning tips\",\n    \"tags\": [\"tag1\", \"tag2\", \"...\"],\n    \"category\": \"Howto & Style\"\n}\nScript sections:\n",
        "thumbnail_concept_instructions": "Based on this video title, generate YouTube thumbnail concept data that maximizes click-through rate through curiosity and pattern-interrupt.\n\nPSYCHOLOGY: The best homeschool thumbnails create an open loop around a decision, cost, or mistake the viewer is worried about. A parent's genuine expression (worried, surprised, relieved) reads instantly at thumbnail size and builds emotional connection faster than a neutral scene.\n\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{{\n    \"image_prompt\": \"one specific, visually dramatic moment under 40 words - prefer: a parent's genuine emotional expression (concerned, surprised, or relieved) while looking at curriculum materials, OR a stark contrast between overwhelming paperwork and a simple clear system, OR a specific dollar amount or checklist visible in frame. Specify camera angle and framing. End with 'high detail, realistic photography, warm lighting, shallow depth of field'\",\n    \"thumbnail_text\": \"2-4 WORDS MAX, all caps, creates an open loop or urgency - a number, a warning, or a surprising claim the video will resolve (e.g. 'I WASTED $2000', 'NOBODY TELLS YOU THIS', 'BIGGEST MISTAKE') - never generic, never the full title, never a full sentence\"\n}}\n\nTitle: {title}\n",
    },
    "franchise_insider": {
        "guide_name": "Franchise Buyer's Guide",
        "guide_url": "https://dreamprince.gumroad.com/l/franchise-buyers-guide",
        "funnel": "Guide breaks down real franchise costs/ROI → Later upsell: franchise consulting, vetted franchise directory, financing partners",
        "cta_text": "Get the Franchise Buyer's Guide",
        "guide_link": "https://dreamprince.gumroad.com/l/franchise-buyers-guide",
        "voice_name_edge": "en-US-EmmaMultilingualNeural",
        "channel_instructions": "Based on the video script sections below, generate YouTube metadata for a franchise cost/business breakdown channel.\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{\n    \"title\": \"string, under 100 characters, punchy, includes the franchise name and cost angle\",\n    \"description\": \"string, 2-4 sentences, franchise cost breakdown or franchise ownership insight\",\n    \"tags\": [\"tag1\", \"tag2\", \"...\"],\n    \"category\": \"Howto & Style\"\n}\nScript sections:\n",
        "thumbnail_concept_instructions": "Based on this video title, generate YouTube thumbnail concept data that maximizes click-through rate through curiosity and pattern-interrupt.\n\nPSYCHOLOGY: The best franchise thumbnails create an open loop around a real dollar figure or a decision the viewer is anxious about getting wrong. A person's genuine expression (shocked, skeptical, calculating) while looking at a storefront or numbers reads instantly at thumbnail size. A stark visible dollar amount (investment cost, profit, loss) is one of the strongest click drivers for this niche.\n\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{{\n    \"image_prompt\": \"one specific, visually dramatic moment under 40 words - prefer: a person's genuine shocked or skeptical expression while looking at a franchise storefront or a laptop showing numbers, OR a specific large dollar amount rendered clearly in frame, OR a stark contrast between a polished storefront and a stack of bills/paperwork. Specify camera angle and framing. End with 'high detail, realistic photography, natural lighting, shallow depth of field'\",\n    \"thumbnail_text\": \"2-4 WORDS MAX, all caps, creates an open loop or urgency - a dollar figure, a warning, or a surprising claim the video will resolve (e.g. '$1.2M TO START', 'THEY WON'T TELL YOU', 'I LOST EVERYTHING') - never generic, never the full title, never a full sentence\"\n}}\n\nTitle: {title}\n",
    },
    "content_machine_lucas": {
        "guide_name": "YouTube Automation Guide",
        "guide_url": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "cta_text": "Get the free YouTube Automation Guide",
        "guide_link": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "channel_instructions": "Based on the video script sections below, generate YouTube metadata for a YouTube automation channel.\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{\n    \"title\": \"string, under 100 characters, punchy, about YouTube automation or content creation\",\n    \"description\": \"string, 2-4 sentences, YouTube content strategy or automation tips\",\n    \"tags\": [\"tag1\", \"tag2\", \"...\"],\n    \"category\": \"Howto & Style\"\n}\nScript sections:\n",
        "thumbnail_concept_instructions": "Based on this video title, generate YouTube thumbnail concept data that maximizes click-through rate through curiosity and pattern-interrupt.\n\nPSYCHOLOGY: The best \"make money/automation\" thumbnails show a concrete, specific result (a number, a screen, a dashboard) rather than an abstract \"hustle\" vibe - specificity signals proof, not hype. A genuine shocked/excited facial expression paired with a visible number performs best.\n\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{{\n    \"image_prompt\": \"one specific, visually dramatic moment under 40 words - prefer: a creator's genuine shocked or excited expression next to a visible phone/laptop screen showing a specific number (views, dollar amount, video count), OR a stark before/after split (empty vs full content calendar), OR a hand pointing at a dashboard result. Specify camera angle and framing. End with 'high detail, realistic photography, dramatic lighting, shallow depth of field'\",\n    \"thumbnail_text\": \"2-4 WORDS MAX, all caps, creates an open loop or urgency - a specific number, a warning, or a surprising claim the video will resolve (e.g. 'I MADE $4,200', '6 VIDEOS A DAY', 'STOP DOING THIS') - never generic, never the full title, never a full sentence\"\n}}\n\nTitle: {title}\n",
        "fixed_tags": [
            "youtube automation", "passive income", "faceless channel", "content automation",
            "youtube automation ai", "ai youtube automation", "n8n youtube automation",
            "youtube automation 2026", "free youtube automation", "start youtube automation",
            "youtube automation guide", "youtube automation niche", "what is youtube automation",
            "youtube automation course", "youtube automation with ai", "youtube automation channel",
            "faceless youtube channel"
        ],
    },
    "demo_shorts": {
        "demo_video_path": "/home/ec2-user/shorts_demo_videos/demo.mp4",
        "guide_name": "YouTube Automation Guide",
        "guide_url": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "cta_text": "Get the free YouTube Automation Guide",
        "guide_link": "https://dreamprince.gumroad.com/l/YouTubeautomation",
        "voice_name_edge": "en-US-AndrewMultilingualNeural",
        "channel_instructions": "Based on the video script sections below, generate YouTube metadata for a fast-paced YouTube automation shorts channel focused on speed and simplicity (no-code, done-in-a-day angle, distinct from creator/passive-income framing).\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{\n    \"title\": \"string, under 100 characters, punchy, emphasizes speed/simplicity of setup\",\n    \"description\": \"string, 2-4 sentences, quick-start YouTube automation tips\",\n    \"tags\": [\"tag1\", \"tag2\", \"...\"],\n    \"category\": \"Howto & Style\"\n}\nScript sections:\n",
        "thumbnail_concept_instructions": "Based on this video title, generate YouTube thumbnail concept data that maximizes click-through rate through curiosity and pattern-interrupt.\n\nPSYCHOLOGY: This channel's angle is speed and simplicity, not passive-income hype. The best thumbnails show a clock/timer, a 'before lunch' or countdown visual, or a simple 3-step overlay - concrete proof it's fast and easy, not abstract wealth imagery.\n\nOutput ONLY valid JSON, no markdown fences, no extra text, in exactly this shape:\n{{\n    \"image_prompt\": \"one specific, visually dramatic moment under 40 words - prefer: a visible countdown timer or clock next to a finished video thumbnail, OR a simple numbered step overlay (1-2-3) on a laptop screen, OR a creator's excited expression next to a stopwatch. Specify camera angle and framing. End with 'high detail, realistic photography, bright lighting, shallow depth of field'\",\n    \"thumbnail_text\": \"2-4 WORDS MAX, all caps, emphasizes speed - e.g. '10 MIN SETUP', 'DONE BY LUNCH', 'THIS FAST?' - never generic, never the full title, never a full sentence\"\n}}\n\nTitle: {title}\n",
        "fixed_tags": [
            "youtube automation", "youtube automation for beginners", "fast youtube automation",
            "easy youtube automation", "no code youtube automation", "youtube automation tutorial",
            "youtube automation 2026", "start youtube channel fast", "youtube shorts automation",
            "automate youtube channel", "youtube automation quick start"
        ],
    },
}

for channel_id, cfg in updates.items():
    cur.execute("""
        UPDATE channels SET config_json = config_json || %s::jsonb WHERE channel_id = %s
    """, (json.dumps(cfg), channel_id))
    print(f"{channel_id}: updated ({cur.rowcount} row)")

cur.close()
conn.close()
print("DONE")
