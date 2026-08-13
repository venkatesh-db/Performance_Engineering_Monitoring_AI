"""
Consumer-lag comparison: run against availability.events.p1 (1 partition)
and availability.events.p3 (3 partitions) with the SAME consumer count
and SAME per-message processing delay, to isolate partition count as
the variable.

A topic with 1 partition can only ever be consumed by 1 consumer within
a group at a time -- extra consumer processes sit idle. A topic with 3
partitions lets 3 consumers process in parallel, dividing the same
workload roughly 3x faster (real physics, not a hand-wave).

Usage:
  # after producing to both topics via /api/admin/kafka/produce
  python kafka/consumer_lag_test.py --topic availability.events.p1 --num-consumers 1 --process-delay-ms 20
  python kafka/consumer_lag_test.py --topic availability.events.p3 --num-consumers 3 --process-delay-ms 20
"""
import argparse
import json
import threading
import time

from kafka import KafkaConsumer


def run_consumer(topic: str, group: str, process_delay_ms: float, results: list, consumer_id: int):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id=group,
        client_id=f"lag-test-consumer-{consumer_id}-{threading.get_ident()}",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
        api_version=(3, 7, 0),
        consumer_timeout_ms=20000,
    )
    count = 0
    lags = []
    for msg in consumer:
        lag_ms = (time.time() - msg.value["produced_at"]) * 1000
        lags.append(lag_ms)
        count += 1
        if process_delay_ms:
            time.sleep(process_delay_ms / 1000)
    consumer.close()
    results.append({"consumer_id": consumer_id, "consumed": count, "avg_lag_ms": sum(lags) / len(lags) if lags else 0,
                     "max_lag_ms": max(lags) if lags else 0})


def main(topic: str, num_consumers: int, process_delay_ms: float):
    group = f"lag-test-{topic}-{int(time.time())}"
    results: list = []
    threads = [
        threading.Thread(target=run_consumer, args=(topic, group, process_delay_ms, results, i))
        for i in range(num_consumers)
    ]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0

    total_consumed = sum(r["consumed"] for r in results)
    overall_max_lag = max((r["max_lag_ms"] for r in results), default=0)
    print(f"\nTopic={topic} consumers={num_consumers} process_delay_ms={process_delay_ms}")
    print(f"Total consumed: {total_consumed} in {elapsed:.1f}s -> {total_consumed/elapsed:.1f} msgs/sec")
    print(f"Max lag across all consumers: {overall_max_lag:.0f}ms")
    for r in results:
        print(f"  consumer {r['consumer_id']}: consumed={r['consumed']} avg_lag={r['avg_lag_ms']:.0f}ms max_lag={r['max_lag_ms']:.0f}ms")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", required=True)
    ap.add_argument("--num-consumers", type=int, default=1)
    ap.add_argument("--process-delay-ms", type=float, default=20)
    args = ap.parse_args()
    main(args.topic, args.num_consumers, args.process_delay_ms)
