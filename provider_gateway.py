"""
provider_gateway.py

Shared adaptive rate-limiting + circuit-breaker gateway for all external
provider calls (Cloudflare, NVIDIA) in the pipeline.

Design goals (per spec):
- Deterministic only. No LLM/judgment-based decisions anywhere in this file.
- Proactive admission control (check budget before calling) instead of
  reactive retry-after-failure.
- Shared state across separate worker processes via Upstash Redis (REST API),
  since workers don't share memory.
- Atomic state transitions via Lua EVAL so concurrent workers can't race
  each other into an inconsistent state.

Workers should NEVER call requests.post(...) on Cloudflare/NVIDIA directly.
They call call_provider(provider, fn, *args, **kwargs) and act on the returned ProviderResult.
"""

import os
import time
import threading
import requests
from collections import namedtuple

UPSTASH_REDIS_REST_URL = os.environ["UPSTASH_REDIS_REST_URL"]
UPSTASH_REDIS_REST_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

_HEADERS = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
_REQUEST_TIMEOUT = 10

MAX_LIMIT = 20
BASELINE_LIMIT = 3
MIN_LIMIT = 1

FAIL_THRESHOLD = 5
FAIL_WINDOW_SECONDS = 30

DEFAULT_COOLDOWN_SECONDS = 60
MAX_COOLDOWN_SECONDS = 600

REQUEUE_AFTER_ADMISSION_REJECTED = 10
REQUEUE_AFTER_CIRCUIT_OPEN = 30
REQUEUE_AFTER_GENERIC_ERROR = 30

PROBE_STALE_SECONDS = 40  # a half_open probe with no heartbeat update in this long is presumed dead

class RateLimitError(Exception):
    """
    Raise this from `fn` when the provider responds with a rate-limit
    signal (HTTP 429, or equivalent). This is the ONLY error type that
    triggers the AIMD limit decrease.
    """
    def __init__(self, retry_after=None, message="rate limited"):
        super().__init__(message)
        self.retry_after = retry_after


class ProviderError(Exception):
    """
    Raise this from `fn` for any other failure (timeout, 5xx, connection
    error, etc). Does NOT affect the AIMD limit, but DOES count toward the
    circuit breaker's consecutive-failure count.
    """
    def __init__(self, retry_after=None, message="provider error"):
        super().__init__(message)
        self.retry_after = retry_after


ProviderResult = namedtuple(
    "ProviderResult",
    ["success", "data", "should_requeue", "requeue_after_seconds"]
)

def _redis_cmd(*parts):
    """
    Execute a single Redis command via Upstash REST API.
    """
    body = [str(p) for p in parts]
    resp = requests.post(UPSTASH_REDIS_REST_URL, headers=_HEADERS, json=body,
                          timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"Redis error on {parts[0]}: {data['error']}")
    return data.get("result") if isinstance(data, dict) else data


def _redis_eval(script, keys, args):
    """
    Execute a Lua script atomically via Upstash REST EVAL.
    """
    body = ["EVAL", script, str(len(keys))] + list(keys) + [str(a) for a in args]
    resp = requests.post(UPSTASH_REDIS_REST_URL, headers=_HEADERS, json=body,
                          timeout=_REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"Redis eval error: {data['error']}")
    return data.get("result") if isinstance(data, dict) else data


def _keys(provider):
    p = f"gateway:{provider}"
    return {
        "limit": f"{p}:limit",
        "in_flight": f"{p}:in_flight",
        "circuit_state": f"{p}:circuit_state",
        "opened_at": f"{p}:opened_at",
        "fail_count": f"{p}:fail_count",
        "cooldown": f"{p}:cooldown",
        "probe_heartbeat": f"{p}:probe_heartbeat",
    }

LUA_CIRCUIT_CHECK = """
local state = redis.call('GET', KEYS[1])
if not state or state == 'closed' then
  return 'closed'
end

local t = redis.call('TIME')
local now = tonumber(t[1])
local opened_at = tonumber(redis.call('GET', KEYS[2]) or '0')
local cooldown = tonumber(redis.call('GET', KEYS[3]) or ARGV[1])

if state == 'open' then
  if (now - opened_at) < cooldown then
    return 'open'
  end
  redis.call('SET', KEYS[1], 'half_open')
  redis.call('SET', KEYS[4], now)
  return 'probe'
end

if state == 'half_open' then
  local probe_seen = tonumber(redis.call('GET', KEYS[4]) or '0')
  local stale_after = tonumber(ARGV[2])
  if (now - probe_seen) >= stale_after then
    redis.call('SET', KEYS[1], 'open')
    redis.call('SET', KEYS[2], now)
    return 'open'
  end
  return 'open'
end

return 'open'
"""

LUA_ADMIT_CHECK = """
local in_flight = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(redis.call('GET', KEYS[2]) or ARGV[1])
if in_flight >= limit then
  return 0
end
redis.call('INCR', KEYS[1])
return 1
"""

LUA_RELEASE = """
local v = tonumber(redis.call('GET', KEYS[1]) or '0')
if v > 0 then
  redis.call('DECR', KEYS[1])
end
return 1
"""

LUA_RECORD_SUCCESS = """
redis.call('SET', KEYS[2], '0')

local state = redis.call('GET', KEYS[3])
if state == 'half_open' then
  redis.call('SET', KEYS[3], 'closed')
  redis.call('SET', KEYS[1], ARGV[2])
  redis.call('DEL', KEYS[4])
  return 'closed'
end

local limit = tonumber(redis.call('GET', KEYS[1]) or ARGV[2])
local max_limit = tonumber(ARGV[1])
if limit < max_limit then
  redis.call('INCR', KEYS[1])
end
return state or 'closed'
"""

LUA_RECORD_FAILURE = """
local t = redis.call('TIME')
local now = tonumber(t[1])

if ARGV[1] == '1' then
  local limit = tonumber(redis.call('GET', KEYS[1]) or ARGV[4])
  local new_limit = math.floor(limit / 2)
  if new_limit < 1 then new_limit = 1 end
  redis.call('SET', KEYS[1], new_limit)
end

local state = redis.call('GET', KEYS[3])

if state == 'half_open' then
  local cooldown = tonumber(redis.call('GET', KEYS[5]) or ARGV[4])
  local new_cooldown = cooldown * 2
  local max_cd = tonumber(ARGV[5])
  if new_cooldown > max_cd then new_cooldown = max_cd end
  redis.call('SET', KEYS[3], 'open')
  redis.call('SET', KEYS[4], now)
  redis.call('SET', KEYS[5], new_cooldown)
  redis.call('SET', KEYS[2], '0')
  return 'open'
end

local exists = redis.call('EXISTS', KEYS[2])
local fail_count
if exists == 0 then
  redis.call('SET', KEYS[2], '1', 'EX', ARGV[3])
  fail_count = 1
else
  fail_count = redis.call('INCR', KEYS[2])
end

local threshold = tonumber(ARGV[2])
if fail_count >= threshold and state ~= 'open' then
  redis.call('SET', KEYS[3], 'open')
  redis.call('SET', KEYS[4], now)
  redis.call('SET', KEYS[5], ARGV[4])
  redis.call('SET', KEYS[2], '0')
  return 'open'
end

return state or 'closed'
"""

def call_provider(provider, fn, *args, **kwargs):
    """
    provider: "cloudflare" or "nvidia"
    fn: callable that performs the actual API call. Contract:
      - returns data on success
      - raises RateLimitError(retry_after=...) on 429 / rate-limit response
      - raises ProviderError(retry_after=...) on any other failure
        (timeout, 5xx, connection error)

    Returns ProviderResult(success, data, should_requeue, requeue_after_seconds)
    """
    k = _keys(provider)

    circuit_result = _redis_eval(
        LUA_CIRCUIT_CHECK,
        [k["circuit_state"], k["opened_at"], k["cooldown"], k["probe_heartbeat"]],
        [DEFAULT_COOLDOWN_SECONDS, PROBE_STALE_SECONDS],
    )

    if circuit_result == "open":
        return ProviderResult(
            success=False, data=None,
            should_requeue=True,
            requeue_after_seconds=REQUEUE_AFTER_CIRCUIT_OPEN,
        )

    is_probe = (circuit_result == "probe")
    admitted_via_budget = False

    if not is_probe:
        admitted = _redis_eval(
            LUA_ADMIT_CHECK,
            [k["in_flight"], k["limit"]],
            [BASELINE_LIMIT],
        )
        if admitted != 1:
            return ProviderResult(
                success=False, data=None,
                should_requeue=True,
                requeue_after_seconds=REQUEUE_AFTER_ADMISSION_REJECTED,
            )
        admitted_via_budget = True

    try:
        heartbeat_stop = threading.Event()
        def _heartbeat():
            while not heartbeat_stop.wait(10):
                try:
                    _redis_cmd("EXPIRE", k["in_flight"], 30)
                    if is_probe:
                        _redis_cmd("SET", k["probe_heartbeat"], int(time.time()))
                except Exception:
                    pass
        heartbeat_thread = None
        if admitted_via_budget or is_probe:
            heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
            heartbeat_thread.start()
        try:
            data = fn(*args, **kwargs)
        finally:
            heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=1)
        _redis_eval(
            LUA_RECORD_SUCCESS,
            [k["limit"], k["fail_count"], k["circuit_state"], k["cooldown"]],
            [MAX_LIMIT, BASELINE_LIMIT],
        )
        return ProviderResult(
            success=True, data=data,
            should_requeue=False,
            requeue_after_seconds=0,
        )

    except RateLimitError as e:
        _redis_eval(
            LUA_RECORD_FAILURE,
            [k["limit"], k["fail_count"], k["circuit_state"], k["opened_at"], k["cooldown"]],
            ["1", FAIL_THRESHOLD, FAIL_WINDOW_SECONDS, DEFAULT_COOLDOWN_SECONDS, MAX_COOLDOWN_SECONDS],
        )
        delay = e.retry_after if e.retry_after else REQUEUE_AFTER_GENERIC_ERROR
        return ProviderResult(
            success=False, data=None,
            should_requeue=True,
            requeue_after_seconds=delay,
        )

    except ProviderError as e:
        _redis_eval(
            LUA_RECORD_FAILURE,
            [k["limit"], k["fail_count"], k["circuit_state"], k["opened_at"], k["cooldown"]],
            ["0", FAIL_THRESHOLD, FAIL_WINDOW_SECONDS, DEFAULT_COOLDOWN_SECONDS, MAX_COOLDOWN_SECONDS],
        )
        delay = e.retry_after if e.retry_after else REQUEUE_AFTER_GENERIC_ERROR
        return ProviderResult(
            success=False, data=None,
            should_requeue=True,
            requeue_after_seconds=delay,
        )

    except Exception:
        _redis_eval(
            LUA_RECORD_FAILURE,
            [k["limit"], k["fail_count"], k["circuit_state"], k["opened_at"], k["cooldown"]],
            ["0", FAIL_THRESHOLD, FAIL_WINDOW_SECONDS, DEFAULT_COOLDOWN_SECONDS, MAX_COOLDOWN_SECONDS],
        )
        return ProviderResult(
            success=False, data=None,
            should_requeue=True,
            requeue_after_seconds=REQUEUE_AFTER_GENERIC_ERROR,
        )

    finally:
        if admitted_via_budget:
            _redis_eval(LUA_RELEASE, [k["in_flight"]], [])

def compute_not_before(delay_seconds):
    """Return a unix timestamp `delay_seconds` in the future."""
    return int(time.time()) + int(delay_seconds)


def is_job_due(job_payload: dict) -> bool:
    """
    Returns True if the job has no not_before field, or if not_before
    has already passed.
    """
    not_before = job_payload.get("not_before")
    if not_before is None:
        return True
    return int(time.time()) >= int(not_before)


def _reset_test_state(provider):
    k = _keys(provider)
    for key in k.values():
        _redis_cmd("DEL", key)


def self_test():
    provider = "selftest_provider"
    print(f"Resetting state for '{provider}'...")
    _reset_test_state(provider)

    print("\n[1] First call should be admitted (closed circuit, empty budget)...")
    def ok_fn():
        return "ok"

    r = call_provider(provider, ok_fn)
    assert r.success is True, f"expected success, got {r}"
    print("    PASS:", r)

    print("\n[2] Simulate rate-limit failures to trip the breaker...")
    def rl_fn():
        raise RateLimitError(retry_after=7)

    last = None
    for i in range(FAIL_THRESHOLD):
        last = call_provider(provider, rl_fn)
        print(f"    attempt {i+1}: success={last.success} requeue_after={last.requeue_after_seconds}")

    state = _redis_cmd("GET", _keys(provider)["circuit_state"])
    print("    circuit_state after threshold failures:", state)
    assert state == "open", f"expected circuit to be open, got {state}"
    print("    PASS: circuit opened after consecutive failures")

    print("\n[3] Call while circuit is open should be rejected without calling fn...")
    called = {"n": 0}

    def should_not_be_called():
        called["n"] += 1
        return "should not happen"

    r = call_provider(provider, should_not_be_called)
    assert r.should_requeue is True
    assert called["n"] == 0, "fn was called while circuit should be open"
    print("    PASS:", r)

    print("\n[4] Force cooldown to expire, confirm probe path and recovery...")
    _redis_cmd("SET", _keys(provider)["opened_at"], 0)

    def ok_fn2():
        return "probe-ok"

    r = call_provider(provider, ok_fn2)
    assert r.success is True, f"expected probe to succeed, got {r}"
    state = _redis_cmd("GET", _keys(provider)["circuit_state"])
    limit = _redis_cmd("GET", _keys(provider)["limit"])
    assert state == "closed", f"expected closed after successful probe, got {state}"
    print(f"    PASS: probe succeeded, circuit_state={state}, limit reset to={limit}")

    print("\nAll self-tests passed.")
    _reset_test_state(provider)


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        self_test()
    else:
        print("Usage: python3 provider_gateway.py --selftest")
