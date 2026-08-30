from engine_common import get_default
import json, time, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}
url = 'https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell'

payload = {
    'prompt': 'a divorce lawyer reviewing paperwork in a modern office, photorealistic',
    'mode': 'base',
    'cfg_scale': 0,
    'width': 1024,
    'height': 1024,
    'samples': 1,
    'steps': 4,
    'seed': 0
}

start = time.time()
try:
    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    elapsed = time.time() - start
    print(f'STATUS: {resp.status_code} in {elapsed:.1f}s')
    if resp.status_code == 200:
        data = resp.json()
        print(f'RESPONSE KEYS: {list(data.keys())}')
        print(f'FULL BODY (first 800 chars): {resp.text[:800]}')
    else:
        print(f'BODY: {resp.text[:500]}')
except requests.exceptions.ReadTimeout:
    elapsed = time.time() - start
    print(f'TIMED OUT after {elapsed:.1f}s')
