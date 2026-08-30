from engine_common import get_default
import json, time, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]

payload = {
    'prompt': 'a simple test image of a red apple',
    'mode': 'base',
    'cfg_scale': 3.5,
    'width': 1024,
    'height': 1024,
    'samples': 1,
    'steps': 4,
    'seed': 0
}
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}

url = 'https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell'

start = time.time()
resp = requests.post(url, headers=headers, json=payload, timeout=60)
elapsed = time.time() - start
print(f'STATUS: {resp.status_code} in {elapsed:.2f}s')
print(f'BODY (first 400 chars): {resp.text[:400]}')
