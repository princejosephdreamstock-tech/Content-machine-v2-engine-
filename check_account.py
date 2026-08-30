from engine_common import get_default
import json, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw

for i, key in enumerate(keys):
    print(f'--- key {i} (...{key[-4:]}) ---')
    headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}
    # a cheap, fast call - just check headers, not full generation
    resp = requests.post(
        'https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell',
        headers=headers,
        json={'prompt': 'test', 'mode': 'base', 'cfg_scale': 0, 'width': 512, 'height': 512, 'samples': 1, 'steps': 1, 'seed': 0},
        timeout=15
    )
    print(f'STATUS: {resp.status_code}')
    for h in resp.headers:
        if any(x in h.lower() for x in ['rate', 'limit', 'retry', 'quota', 'remaining']):
            print(f'  {h}: {resp.headers[h]}')
    if resp.status_code != 200:
        print(f'  BODY: {resp.text[:200]}')
