from engine_common import get_default
import json, requests

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}

for base in ['https://integrate.api.nvidia.com/v1/models', 'https://ai.api.nvidia.com/v1/models']:
    print(f'--- {base} ---')
    try:
        resp = requests.get(base, headers=headers, timeout=15)
        print(f'STATUS: {resp.status_code}')
        print(resp.text[:2000])
    except Exception as e:
        print(f'ERROR: {e}')
    print()
