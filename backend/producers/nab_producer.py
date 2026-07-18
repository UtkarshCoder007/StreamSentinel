"""
NAB Producer — replays a NAB CSV stream to Kafka at a configurable speed,
simulating a live financial/metrics feed.
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

TOPIC = "financial-stream"
BOOTSTRAP_SERVERS = ["localhost:9092"]
NAB_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_EVERY_N = 25  # avoid flooding the terminal at high replay speeds


def parse_args():
    parser = argparse.ArgumentParser(description="Replay a NAB CSV stream to Kafka.")
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("data/nab/ec2_cpu_utilization_5f5533.csv"),
        help="Path to the NAB CSV file (relative to backend/ if run from there).",
    )
    parser.add_argument(
        "--stream-id",
        type=str,
        default="realAWSCloudwatch/ec2_cpu_utilization_5f5533",
        help="Identifier for this stream, included in each message.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier (1 = real-time pacing, 100 = 100x faster).",
    )
    return parser.parse_args()


def load_rows(csv_path: Path):
    if not csv_path.exists():
        print(f"ERROR: CSV file not found at {csv_path.resolve()}", file=sys.stderr)
        sys.exit(1)

    rows = []
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "timestamp": row["timestamp"],
                    "value": float(row["value"]),
                }
            )
    if not rows:
        print(f"ERROR: No rows found in {csv_path}", file=sys.stderr)
        sys.exit(1)
    return rows


def build_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks=1,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
    except KafkaTimeoutError:
        print(
            f"ERROR: Could not connect to Kafka at {BOOTSTRAP_SERVERS}. "
            "Is the Docker Compose stack running? (docker compose ps)",
            file=sys.stderr,
        )
        sys.exit(1)


def make_envelope(stream_id: str, source: str, row: dict) -> dict:
    return {
        "source": source,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "stream_id": stream_id,
        "value": row["value"],
        "raw": row,
    }


def run(args):
    rows = load_rows(args.file)
    producer = build_producer()

    print(f"Loaded {len(rows)} rows from {args.file}")
    print(f"Replaying to topic '{TOPIC}' at {args.speed}x speed. Ctrl+C to stop early.\n")

    start_time = time.monotonic()
    sent_count = 0

    try:
        for i, row in enumerate(rows):
            envelope = make_envelope(args.stream_id, "nab", row)
            producer.send(TOPIC, value=envelope)
            sent_count += 1

            if sent_count % LOG_EVERY_N == 0 or sent_count == len(rows):
                print(
                    f"[{sent_count}/{len(rows)}] "
                    f"orig_ts={row['timestamp']} value={row['value']:.3f}"
                )

            # Sleep proportionally to the real gap between this row and the next,
            # scaled down by the speed multiplier. Skip sleep after the last row.
            if i < len(rows) - 1:
                t1 = datetime.strptime(row["timestamp"], NAB_TIMESTAMP_FORMAT)
                t2 = datetime.strptime(rows[i + 1]["timestamp"], NAB_TIMESTAMP_FORMAT)
                delta_seconds = (t2 - t1).total_seconds()
                sleep_for = max(0.0, delta_seconds / args.speed)
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        producer.flush()
        producer.close()

    elapsed = time.monotonic() - start_time
    print(f"\nDone. Sent {sent_count}/{len(rows)} messages in {elapsed:.1f}s wall-clock time.")


if __name__ == "__main__":
    run(parse_args())