"""
Direct Kafka consumer benchmark for Module 4. Measures per-message lag
(now - produced_at, embedded by producer_perf_test.py) so you can watch
consumer lag grow when --process-delay-ms makes this consumer slower
than the producer's arrival rate -- the module's "consumer lag" topic,
reproduced directly against the broker.

Usage:
  python kafka/consumer_perf_test.py --process-delay-ms 20 --topic booking.created
"""
import argparse
import json
import time

from kafka import KafkaConsumer


def main(bootstrap: str, topic: str, group: str, process_delay_ms: float, max_messages: int):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode()),
        consumer_timeout_ms=10000,
        api_version=(3, 7, 0),
    )

    lags = []
    count = 0
    for msg in consumer:
        lag_ms = (time.time() - msg.value["produced_at"]) * 1000
        lags.append(lag_ms)
        count += 1
        if process_delay_ms:
            time.sleep(process_delay_ms / 1000)
        if count % 100 == 0:
            recent = lags[-100:]
            print(f"[{count}] recent avg lag = {sum(recent)/len(recent):.0f}ms, "
                  f"max = {max(recent):.0f}ms")
        if max_messages and count >= max_messages:
            break

    if lags:
        print(f"\nConsumer perf test: {count} messages, "
              f"avg lag={sum(lags)/len(lags):.0f}ms, max lag={max(lags):.0f}ms, "
              f"final lag={lags[-1]:.0f}ms")
    else:
        print("no messages consumed -- check topic/bootstrap and that a producer has run")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="booking.perf.test")
    ap.add_argument("--group", default="perf-test-consumer")
    ap.add_argument("--process-delay-ms", type=float, default=0)
    ap.add_argument("--max-messages", type=int, default=0, help="0 = run until 10s idle")
    args = ap.parse_args()
    main(args.bootstrap, args.topic, args.group, args.process_delay_ms, args.max_messages)
