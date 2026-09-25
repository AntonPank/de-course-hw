import json
import os
import time

from confluent_kafka import Consumer
from icecream import ic

# Дано, не редагувати.
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "github-events"
OUTPUT_PATH = "data/output/stats.json"

GROUP_ID = "github-stats-consumer"
IDLE_LIMIT_SECONDS = 5.0


def update_counts(by_type: dict, by_repo: dict, event: dict) -> None:
    by_type[event["event_type"]] = by_type.get(event["event_type"], 0) + 1
    by_repo[event["repo_name"]] = by_repo.get(event["repo_name"], 0) + 1


def top_repos(by_repo: dict, n: int = 5) -> list:
    repos = sorted(
        by_repo.items(),
        key=lambda item: (-item[1], item[0]),
    )

    return [[name, count] for name, count in repos[:n]]


def run_consumer() -> dict:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe([TOPIC])

    by_type = {}
    by_repo = {}
    total = 0

    last_message_time = time.monotonic()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            if time.monotonic() - last_message_time >= IDLE_LIMIT_SECONDS:
                break
            continue

        if msg.error():
            continue

        event = json.loads(msg.value())

        update_counts(by_type, by_repo, event)
        total += 1

        last_message_time = time.monotonic()

    consumer.close()

    stats = {
        "total": total,
        "by_type": by_type,
        "top_repos": top_repos(by_repo, 5),
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    return stats


if __name__ == "__main__":
    ic(run_consumer())
