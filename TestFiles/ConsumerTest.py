"""Quick look at what's actually landing in Kafka — the consumer-side twin of APISample.py.

Plain confluent-kafka consumer, no Spark. Starts in about a second instead of ~40, which makes
it the right tool for "is the producer working / what do these records look like" questions.
Safe to run alongside the real consumers: it uses a throwaway consumer group and never commits
offsets, so it can't disturb bronze_consumer or gold_live_consumer.

    python TestFiles/ConsumerTest.py
"""

import json
import time
import uuid
from collections import Counter
from pprint import pprint

from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "opensky-states"

# Read the existing topic backlog rather than waiting for the next poll. The producer only
# publishes every 300s, so "latest" means staring at an empty terminal for up to 5 minutes.
FROM_BEGINNING = True

SAMPLE_SIZE = 3       # full records to pprint
SCAN_SIZE = 2000      # records to read for the summary stats
IDLE_TIMEOUT = 30     # give up after this many seconds with no new messages


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Unique group each run: we always want our own offsets, never a resumed position.
        "group.id": f"consumer-test-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest" if FROM_BEGINNING else "latest",
        "enable.auto.commit": False,
    })


def consume(limit: int):
    """Yield up to `limit` messages, stopping early if the topic goes quiet."""
    consumer = build_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    last_message_at = time.time()
    count = 0

    try:
        while count < limit:
            msg = consumer.poll(1.0)

            if msg is None:
                if time.time() - last_message_at > IDLE_TIMEOUT:
                    print(f"\n(no new messages for {IDLE_TIMEOUT}s — stopping)")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"consumer error: {msg.error()}")
                break

            last_message_at = time.time()
            count += 1
            yield msg
    finally:
        consumer.close()


def show_messages():
    print(f"\n=== {KAFKA_TOPIC}: sample messages ===")
    for msg in consume(SAMPLE_SIZE):
        record = json.loads(msg.value())
        print(f"\npartition {msg.partition()} offset {msg.offset()}  key={msg.key().decode()}")
        pprint(record)


def show_summary():
    """Message counts, polls seen, and per-field null coverage.

    The null counts are the useful part — they tell you which columns are actually sparse
    before you write a gold_live sink that assumes they're populated.
    """
    print(f"\n=== {KAFKA_TOPIC}: summary over up to {SCAN_SIZE} messages ===")
    polls = Counter()
    aircraft = set()
    nulls = Counter()
    fields = []
    total = 0

    for msg in consume(SCAN_SIZE):
        record = json.loads(msg.value())
        total += 1
        polls[record["poll_time"]] += 1
        aircraft.add(record["icao24"])
        if not fields:
            fields = list(record.keys())
        for field, value in record.items():
            if value is None:
                nulls[field] += 1

    if not total:
        print("no messages read — is the producer running?")
        return

    print(f"messages:         {total}")
    print(f"unique aircraft:  {len(aircraft)}")
    print(f"distinct polls:   {len(polls)}")
    for poll_time, n in sorted(polls.items()):
        stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(poll_time))
        print(f"  poll_time {poll_time} ({stamp}) -> {n} aircraft")

    print("\nnull counts by field:")
    for field in fields:
        n = nulls[field]
        print(f"  {field:<16} {n:>5} / {total}  ({n / total:.0%})")


if __name__ == "__main__":
    show_messages()
    show_summary()
