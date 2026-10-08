"""Sink: Kafka -> landing-зона (NDJSON).

Єдине місце проєкту, де потік перетворюється на файли. Далі працюють Spark і dbt,
тому контракт landing-зони — це контракт усього пайплайну:

    data/landing/dt=YYYY-MM-DD/hour=HH/part-p{partition}-o{first}-o{last}.ndjson

`dt=` і `hour=` — час запису (processing time), а не час події.

Семантика доставки — at-least-once:
1. батч записується в тимчасовий файл і атомарно публікується через `os.replace`;
2. Kafka offsets комітяться тільки після успішного запису.

Якщо процес упаде між цими кроками, повідомлення можуть бути прочитані повторно,
але не будуть втрачені.
"""

from __future__ import annotations

import os
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException

from common import config

BATCH_MAX_MESSAGES = int(os.environ.get("BATCH_MAX_MESSAGES", "500"))
BATCH_MAX_SECONDS = float(os.environ.get("BATCH_MAX_SECONDS", "10"))
IDLE_EXIT_SECONDS = float(os.environ.get("IDLE_EXIT_SECONDS", "0"))
SESSION_TIMEOUT_MS = int(os.environ.get("SESSION_TIMEOUT_MS", "10000"))

_running = True


def _stop(signum: int, frame: FrameType | None) -> None:
    global _running
    _running = False


def landing_path(
    base: Path,
    ingested_at: datetime,
    partition: int,
    first_offset: int,
    last_offset: int,
) -> Path:
    """Повертає детермінований шлях для Kafka partition batch."""
    ingested_at = ingested_at.astimezone(UTC)

    return (
        base
        / f"dt={ingested_at:%Y-%m-%d}"
        / f"hour={ingested_at:%H}"
        / (f"part-p{partition}-o{first_offset:012d}-o{last_offset:012d}.ndjson")
    )


def write_batch(
    base: Path,
    ingested_at: datetime,
    records: list[tuple[int, int, bytes]],
) -> list[Path]:
    """Записує батч у landing і повертає шляхи фінальних файлів."""
    if not records:
        return []

    grouped: dict[int, list[tuple[int, bytes]]] = {}

    for partition, offset, value in records:
        grouped.setdefault(partition, []).append((offset, value))

    written: list[Path] = []
    temp_files: list[tuple[Path, Path]] = []
    published: list[Path] = []

    try:
        for partition in sorted(grouped):
            partition_records = sorted(
                grouped[partition],
                key=lambda item: item[0],
            )

            first_offset = partition_records[0][0]
            last_offset = partition_records[-1][0]

            target = landing_path(
                base,
                ingested_at,
                partition,
                first_offset,
                last_offset,
            )

            if target.exists():
                written.append(target)
                continue

            target.parent.mkdir(parents=True, exist_ok=True)

            temp = Path(f"{target}.tmp")
            temp_files.append((temp, target))

            with temp.open("wb") as file:
                for _, value in partition_records:
                    file.write(value.rstrip(b"\n"))
                    file.write(b"\n")

                file.flush()
                os.fsync(file.fileno())

            written.append(target)

        for temp, target in temp_files:
            os.replace(temp, target)
            published.append(target)

    except Exception:
        for temp, _ in temp_files:
            temp.unlink(missing_ok=True)

        for target in published:
            target.unlink(missing_ok=True)

        raise

    return written


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.KAFKA_GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            "topic.metadata.refresh.interval.ms": 5000,
        }
    )

    consumer.subscribe([config.KAFKA_TOPIC])

    print(f"consumer: {config.KAFKA_TOPIC} -> {config.LANDING_DIR} (group={config.KAFKA_GROUP_ID})")

    batch: list[tuple[int, int, bytes]] = []
    batch_started = time.monotonic()
    last_message_at = time.monotonic()
    total = 0
    waiting_for_topic = False

    def flush() -> None:
        nonlocal batch, batch_started, total

        if not batch:
            batch_started = time.monotonic()
            return

        files = write_batch(
            config.LANDING_DIR,
            datetime.now(UTC),
            batch,
        )

        consumer.commit(asynchronous=False)

        total += len(batch)

        print(f"  {len(batch)} подій -> {', '.join(p.name for p in files)} (всього {total})")

        batch = []
        batch_started = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(1.0)

            if msg is None:
                if time.monotonic() - batch_started >= BATCH_MAX_SECONDS:
                    flush()

                idle_limit = IDLE_EXIT_SECONDS + (0 if total else SESSION_TIMEOUT_MS / 1000)

                if IDLE_EXIT_SECONDS and time.monotonic() - last_message_at >= idle_limit:
                    print(f"топік порожній {idle_limit:.0f} с — виходжу")
                    break

                continue

            error = msg.error()

            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue

                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    if not waiting_for_topic:
                        print(f"топік {config.KAFKA_TOPIC} ще не існує — чекаю")
                        waiting_for_topic = True
                    continue

                raise KafkaException(error)

            waiting_for_topic = False

            partition = msg.partition()
            offset = msg.offset()
            value = msg.value()

            if partition is None or offset is None or value is None:
                continue

            last_message_at = time.monotonic()
            batch.append((partition, offset, value))

            if (
                len(batch) >= BATCH_MAX_MESSAGES
                or time.monotonic() - batch_started >= BATCH_MAX_SECONDS
            ):
                flush()

    finally:
        flush()
        consumer.close()
        print(f"consumer зупинено, записано {total} подій")


if __name__ == "__main__":
    main()
