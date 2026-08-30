from engine_common import get_default
import json, time, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]

payload = {
    'model': 'qwen/qwen-image-2512',
    'prompt': 'a simple test image of a red apple',
    'n': 1,
    'response_format': 'b64_json'
}
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}

url = 'https://ai.api.nvidia.com/v1/images/generations'

start = time.time()
resp = requests.post(url, headers=headers, json=payload, timeout=90)
elapsed = time.time() - start
print(f'STATUS: {resp.status_code} in {elapsed:.2f}s')
print(f'BODY (first 500 chars): {resp.text[:500]}')
