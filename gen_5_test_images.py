from engine_common import get_default
import json, time, requests, base64

keys_raw = get_default('nvidia_api_keys', '[]')
keys = json.loads(keys_raw) if isinstance(keys_raw, str) else keys_raw
key = keys[0]
headers = {'Authorization': f'Bearer {key}', 'Accept': 'application/json'}
url = 'https://ai.api.nvidia.com/v1/genai/black-forest-labs/flux.1-schnell'

prompts = [
    'a divorce lawyer reviewing paperwork in a modern office, photorealistic',
    'a stressed couple sitting apart on a couch, dim lighting, cinematic',
    'a courtroom gavel next to a wedding ring, dramatic lighting',
    'a person calculating finances at a kitchen table, warm light',
    'a lawyer shaking hands with a client, professional office setting'
]

for i, prompt in enumerate(prompts):
    print(f'generating {i}: {prompt[:40]}...')
    payload = {
        'prompt': prompt,
        'mode': 'base',
        'cfg_scale': 0,
        'width': 1024,
        'height': 1024,
        'samples': 1,
        'steps': 4,
        'seed': i
    }
    try:
        start = time.time()
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        elapsed = time.time() - start
        if resp.status_code == 200:
            data = resp.json()
            # try common response shapes
            b64 = None
            if 'artifacts' in data:
                b64 = data['artifacts'][0].get('base64')
            elif 'data' in data:
                b64 = data['data'][0].get('b64_json')
            elif 'image' in data:
                b64 = data['image']
            if b64:
                img_bytes = base64.b64decode(b64)
                path = f'/home/ec2-user/test_images/test_{i}.jpg'
                with open(path, 'wb') as f:
                    f.write(img_bytes)
                print(f'  saved {path} in {elapsed:.1f}s')
            else:
                print(f'  200 OK but unknown response shape: {list(data.keys())}')
                print(f'  raw (first 300 chars): {resp.text[:300]}')
        else:
            print(f'  FAILED {resp.status_code}: {resp.text[:200]}')
    except Exception as e:
        print(f'  ERROR: {e}')

print('done')
