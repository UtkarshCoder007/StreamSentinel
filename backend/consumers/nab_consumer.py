"""
NAB Consumer — polls financial-stream from Kafka, scores each message with
River's HalfSpaceTrees, and writes results to Redis.

Loop order (strictly prequential):
    poll → score_one() → learn_one() → write Redis → commit offset
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import redis as redis_lib
from kafka import KafkaConsumer
from river.anomaly import HalfSpaceTrees
from river import metrics

# ── Config ────────────────────────────────────────────────────────────────────
TOPIC = "financial-stream"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CONSUMER_GROUP = "streamsentinel-consumer"
HISTORY_CAP = 500
ANOMALY_THRESHOLD = 0.7
LOG_EVERY_N = 25

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── CLI args ──────────────────────────────────────────────────────────────────
def parse_args():
    parser = argparse.ArgumentParser(description="StreamSentinel NAB consumer.")
    parser.add_argument(
        "--stream-id",
        type=str,
        default="realAWSCloudwatch/ec2_cpu_utilization_5f5533",
        help="Stream ID to filter incoming Kafka messages by.",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=CONSUMER_GROUP,
        help="Kafka consumer group ID to use.",
    )
    parser.add_argument(
        "--no-flush",
        action="store_true",
        help="Skip flushing Redis keys on startup — resumes from previous run's history.",
    )
    parser.add_argument(
        "--labels",
        type=Path,
        default=None,
        help="Path to NAB combined_labels.json for ROCAUC tracking (optional).",
    )
    return parser.parse_args()


# ── Labels ────────────────────────────────────────────────────────────────────
def load_labels(labels_path: Path, stream_id: str) -> set:
    if labels_path is None or not labels_path.exists():
        return set()
    with labels_path.open() as f:
        all_labels = json.load(f)
    key = f"{stream_id}.csv"
    return set(all_labels.get(key, []))


# ── Connections ───────────────────────────────────────────────────────────────
def build_consumer(group_id: str) -> KafkaConsumer:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")) if value is not None else None,
        )
        consumer.subscribe([TOPIC])
        return consumer
    except Exception as e:
        log.error(f"Failed to connect to Kafka: {e}")
        sys.exit(1)


def build_redis() -> redis_lib.Redis:
    try:
        client = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
        client.ping()
        return client
    except redis_lib.ConnectionError as e:
        log.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)


# ── Redis helpers ─────────────────────────────────────────────────────────────
def flush_redis_keys(r: redis_lib.Redis, stream_id: str):
    latest_key = f"stream:{stream_id}:latest"
    history_key = f"stream:{stream_id}:history"
    deleted = r.delete(latest_key, history_key)
    log.info(f"Flushed {deleted} Redis key(s) for stream '{stream_id}'.")


def write_to_redis(r: redis_lib.Redis, stream_id: str, result: dict):
    history_key = f"stream:{stream_id}:history"
    pipe = r.pipeline()
    pipe.lpush(history_key, json.dumps(result))
    pipe.ltrim(history_key, 0, HISTORY_CAP - 1)
    pipe.execute()


def patch_and_update_latest(
    r: redis_lib.Redis,
    stream_id: str,
    result: dict,
    t_learned: float,
    t_written: float,
):
    result["write_latency_ms"] = round((t_written - t_learned) * 1000, 3)
    result["processing_latency_ms"] = round((t_written - result["_t_consume"]) * 1000, 3)
    result.pop("_t_consume")
    r.set(f"stream:{stream_id}:latest", json.dumps(result))


# ── Main loop ─────────────────────────────────────────────────────────────────
def run(args):
    anomaly_timestamps = load_labels(args.labels, args.stream_id)
    has_labels = len(anomaly_timestamps) > 0
    log.info(
        f"Labels: {'loaded ' + str(len(anomaly_timestamps)) + ' anomaly timestamp(s)' if has_labels else 'none — ROCAUC disabled'}"
    )

    model = HalfSpaceTrees(seed=42)
    rocauc = metrics.ROCAUC() if has_labels else None
    running_rocauc = None

    r = build_redis()
    if not args.no_flush:
        flush_redis_keys(r, args.stream_id)

    consumer = build_consumer(args.group_id)
    log.info(f"Listening on topic '{TOPIC}' | group '{args.group_id}' | stream '{args.stream_id}'")

    for _ in range(10):
        consumer.poll(timeout_ms=200)
        if consumer.assignment():
            break

    if consumer.assignment():
        consumer.seek_to_beginning(*consumer.assignment())
        log.info("Seeking to the beginning of the topic so replayed messages are processed.")

    processed = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for tp, messages in records.items():
                for kafka_msg in messages:

                    log.debug(f"Fetched raw message from partition {tp.partition} at offset {kafka_msg.offset}")

                    # ── Decode ────────────────────────────────────────────
                    msg = kafka_msg.value
                    if msg is None:
                        consumer.commit()
                        continue

                    # ── Filter by stream_id ───────────────────────────────
                    if msg.get("stream_id") != args.stream_id:
                        log.warning(
                            f"Skipping message: Topic stream_id '{msg.get('stream_id')}' "
                            f"does not match expected target '{args.stream_id}'"
                        )
                        consumer.commit()
                        continue

                    # ── t_consume ─────────────────────────────────────────
                    t_consume = time.monotonic()
                    event_time_str = msg["event_time"]
                    event_time_dt = datetime.fromisoformat(event_time_str)

                    # ── Feature extraction ────────────────────────────────
                    features = {"value": msg["value"]}

                    # ── score_one() → t_scored ────────────────────────────
                    anomaly_score = model.score_one(features)
                    t_scored = time.monotonic()

                    # ── learn_one() → t_learned ───────────────────────────
                    model.learn_one(features)
                    t_learned = time.monotonic()

                    # ── ROCAUC update ─────────────────────────────────────
                    if has_labels and rocauc is not None:
                        raw_ts = msg.get("raw", {}).get("timestamp", "")
                        is_true_anomaly = int(raw_ts in anomaly_timestamps)
                        rocauc.update(is_true_anomaly, anomaly_score)
                        running_rocauc = rocauc.get()

                    # ── e2e latency ───────────────────────────────────────
                    now_utc = datetime.now(timezone.utc)
                    e2e_ms = round((now_utc - event_time_dt).total_seconds() * 1000, 3)

                    # ── Build result ──────────────────────────────────────
                    result = {
                        "event_time": event_time_str,
                        "stream_id": msg["stream_id"],
                        "value": msg["value"],
                        "anomaly_score": round(anomaly_score, 6),
                        "is_anomaly": anomaly_score >= ANOMALY_THRESHOLD,
                        "running_rocauc": round(running_rocauc, 6) if running_rocauc is not None else None,
                        "score_latency_ms": round((t_scored - t_consume) * 1000, 3),
                        "learn_latency_ms": round((t_learned - t_scored) * 1000, 3),
                        "write_latency_ms": None,
                        "processing_latency_ms": None,
                        "e2e_latency_ms": e2e_ms,
                        "_t_consume": t_consume,
                    }

                    # ── Write to Redis ────────────────────────────────────
                    write_to_redis(r, args.stream_id, result)
                    t_written = time.monotonic()
                    patch_and_update_latest(r, args.stream_id, result, t_learned, t_written)

                    # ── Manual commit ─────────────────────────────────────
                    consumer.commit()
                    processed += 1

                    # ── Logging ───────────────────────────────────────────
                    if processed % LOG_EVERY_N == 0:
                        log.info(
                            f"[{processed}] consumed stream='{msg['stream_id']}' "
                            f"value={msg['value']:.4f} score={anomaly_score:.4f} "
                            f"anomaly={result['is_anomaly']} "
                            f"proc={result['processing_latency_ms']:.2f}ms "
                            f"e2e={e2e_ms:.1f}ms"
                            + (f" rocauc={running_rocauc:.4f}" if running_rocauc is not None else "")
                        )

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        consumer.close()
        log.info(f"Consumer shut down. Total messages processed: {processed}")


if __name__ == "__main__":
    run(parse_args())