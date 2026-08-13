"""
Direct Kafka producer benchmark for Module 4 (bypasses the FastAPI app --
talks to the broker directly, matching the module's "producer/consumer
performance-test utilities for direct broker benchmarking" topic).

Requires the broker from kafka/docker-compose.yml (or any Kafka broker)
reachable at --bootstrap.

Usage:
  python kafka/producer_perf_test.py --messages 5000 --rate 200

Uses a dedicated "booking.perf.test" topic by default -- deliberately
NOT the app's "booking.created" topic, so synthetic benchmark traffic
never mixes with real app events (and their different payload shapes)
in the same partition.
"""
import argparse
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


def main(bootstrap: str, topic: str, messages: int, rate: float, batch_size: int, acks: str):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode(),
        batch_size=batch_size,
        acks=acks if acks != "all" else "all",
        # NOTE: producer.flush() and linger_ms > 0 were both tested and
        # reproducibly hang forever with kafka-python-ng 2.2.x +
        # apache/kafka 3.7.0 + Python 3.14 on this machine -- confirmed by
        # isolating each with individual future.get(timeout=...) calls,
        # which DO work reliably. So: linger_ms stays at the library
        # default (0), and delivery is confirmed via each send()'s
        # returned future instead of a bulk flush().
        api_version=(3, 7, 0),
    )

    interval = 1.0 / rate if rate > 0 else 0
    start = time.time()
    futures = []

    for i in range(messages):
        payload = {"seq": i, "produced_at": time.time(), "seat_no": f"S{i % 40}"}
        futures.append(producer.send(topic, payload))
        if interval:
            time.sleep(interval)

    send_elapsed = time.time() - start
    sent = 0
    errors = 0
    for f in futures:
        try:
            f.get(timeout=5)
            sent += 1
        except KafkaError:
            errors += 1

    elapsed = time.time() - start
    print(f"\nProducer perf test: {sent}/{messages} confirmed delivered, {errors} errors, "
          f"{send_elapsed:.1f}s to issue all sends, {elapsed:.1f}s total incl. delivery confirmation, "
          f"{sent/elapsed:.1f} msgs/sec, acks={acks}, batch_size={batch_size}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="booking.perf.test")
    ap.add_argument("--messages", type=int, default=5000)
    ap.add_argument("--rate", type=float, default=200, help="target msgs/sec, 0 = as fast as possible")
    ap.add_argument("--batch-size", type=int, default=16384)
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"])
    args = ap.parse_args()
    main(args.bootstrap, args.topic, args.messages, args.rate, args.batch_size, args.acks)
