from engine_common import get_default
import json, time, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}

payload = {'prompt': 'test', 'mode': 'base', 'cfg_scale': 0, 'width': 1024, 'height': 1024, 'samples': 1, 'steps': 1, 'seed': 0}

start = time.time()
try:
    resp = requests.post('https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell', headers=headers, json=payload, timeout=90)
    elapsed = time.time() - start
    print(f'STATUS: {resp.status_code} in {elapsed:.1f}s')
    print(resp.text[:300])
except requests.exceptions.ReadTimeout:
    print(f'TIMED OUT after {time.time()-start:.1f}s')
