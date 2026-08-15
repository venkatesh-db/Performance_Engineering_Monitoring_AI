"""
Real, runnable demonstration of Loki label cardinality for Day 3 /
Module 3. Pushes the same 20 synthetic events two ways -- once with a
per-event ID as a LOKI LABEL (the mistake), once with it kept in the
JSON body (the fix used throughout this lab's actual app) -- then
queries Loki's own series API to show the real stream-count difference.

Run:  python cardinality_demo.py
"""
import json
import time
import urllib.request


def push(streams: list[dict]) -> None:
    body = json.dumps({"streams": streams}).encode()
    req = urllib.request.Request(
        "http://localhost:3100/loki/api/v1/push", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    urllib.request.urlopen(req, timeout=5)


def series_count(match: str) -> int:
    import urllib.parse
    url = "http://localhost:3100/loki/api/v1/series?" + urllib.parse.urlencode({"match[]": match})
    with urllib.request.urlopen(url, timeout=5) as resp:
        return len(json.load(resp)["data"])


def main() -> None:
    for i in range(20):
        push([{
            "stream": {"job": "cardinality-demo-bad", "transaction_id": f"txn-{i}"},
            "values": [[str(int(time.time() * 1e9)), json.dumps({"msg": "order placed", "transaction_id": f"txn-{i}"})]],
        }])
    for i in range(20):
        push([{
            "stream": {"job": "cardinality-demo-good"},
            "values": [[str(int(time.time() * 1e9)), json.dumps({"msg": "order placed", "transaction_id": f"txn-{i}"})]],
        }])

    time.sleep(1)
    bad = series_count('{job="cardinality-demo-bad"}')
    good = series_count('{job="cardinality-demo-good"}')
    print(f"BAD (transaction_id as a label):  {bad} distinct Loki streams for 20 events")
    print(f"GOOD (transaction_id in the body): {good} distinct Loki stream(s) for the same 20 events")


if __name__ == "__main__":
    main()
