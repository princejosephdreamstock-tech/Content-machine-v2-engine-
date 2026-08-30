from engine_common import get_default
import json, time, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]

payload = {
    'model': 'black-forest-labs/flux.2-klein-4b',
    'prompt': 'a simple test image of a red apple',
    'n': 1,
    'response_format': 'b64_json'
}
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}

start = time.time()
resp = requests.post('https://integrate.api.nvidia.com/v1/images/generations', headers=headers, json=payload, timeout=60)
elapsed = time.time() - start
print(f'STATUS: {resp.status_code} in {elapsed:.2f}s')
print(f'BODY (first 400 chars): {resp.text[:400]}')
