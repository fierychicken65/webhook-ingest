import json
import urllib.request
import time
import random
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:8080"

def send_webhook(event):
    req = urllib.request.Request(
        f"{BASE_URL}/webhooks/calls",
        data=json.dumps(event).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = e.reason
        return e.code, body
    except Exception as e:
        return 500, str(e)

def get_stats(account_id):
    req = urllib.request.Request(f"{BASE_URL}/accounts/{account_id}/stats", method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))

def main():
    print("Starting Concurrent Webhook Ingestion Test...")
    
    account_id = f"acc_concurrent_py_{random.randint(1000, 9999)}"
    event_id = f"evt_concurrent_py_{random.randint(100000, 999999)}"
    call_id = f"call_concurrent_py_{random.randint(100000, 999999)}"
    
    event = {
        "event_id": event_id,
        "call_id": call_id,
        "account_id": account_id,
        "status": "completed",
        "duration_sec": 150,
        "recording_url": "https://example.com/concurrent.wav",
        "occurred_at": "2026-08-19T12:00:00Z"
    }

    # Verify initial stats are zero
    _, stats = get_stats(account_id)
    assert stats["call_count"] == 0

    concurrency = 50
    print(f"Sending {concurrency} duplicate webhooks concurrently to {BASE_URL}...")
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_webhook, event) for _ in range(concurrency)]
        results = [f.result() for f in futures]

    # Verify all requests returned 200 OK
    failed_requests = 0
    for idx, (status, body) in enumerate(results):
        if status != 200:
            print(f"Request {idx} failed with status {status}: {body}")
            failed_requests += 1

    assert failed_requests == 0, f"{failed_requests} requests failed!"
    print("All concurrent requests completed successfully.")

    # Wait a bit for async tasks to settle
    time.sleep(0.5)

    # Verify stats count is exactly 1 (meaning deduplicated and no double counting)
    _, stats = get_stats(account_id)
    print(f"Final stats: {stats}")
    assert stats["call_count"] == 1, f"Expected call_count to be 1, got {stats['call_count']}"
    assert stats["total_duration_sec"] == 150, f"Expected total_duration_sec to be 150, got {stats['total_duration_sec']}"

    print("Concurrent integration test passed successfully!")

if __name__ == "__main__":
    main()
