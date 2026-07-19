"""
StreamSentinel — FastAPI serving layer.
Reads scored results from Redis and exposes them to the React dashboard.
"""

import json
import logging

import redis.exceptions
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import redis as redis_lib

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="StreamSentinel",
    description="Real-time anomaly detection on streaming financial data.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Redis ─────────────────────────────────────────────────────────────────────
r = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)


def get_redis():
    """Ping Redis on first use to fail fast if it's unreachable."""
    try:
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        log.error(f"Redis connection failed: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable.")


# ── Pydantic schemas ──────────────────────────────────────────────────────────
class StreamResult(BaseModel):
    event_time: str
    stream_id: str
    value: float
    anomaly_score: float
    is_anomaly: bool
    running_rocauc: float | None
    score_latency_ms: float
    learn_latency_ms: float
    write_latency_ms: float | None
    processing_latency_ms: float | None
    e2e_latency_ms: float
    drift_detected: bool
    model_generation: int


class HistoryResponse(BaseModel):
    stream_id: str
    count: int
    results: list[StreamResult]


class DriftEvent(BaseModel):
    detected_at: str
    trigger_score: float
    adwin_window_size: float
    message_count: int
    model_generation_killed: int


class DriftResponse(BaseModel):
    stream_id: str
    count: int
    events: list[DriftEvent]


class StreamsResponse(BaseModel):
    streams: list[str]


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/streams", response_model=StreamsResponse)
def list_streams():
    # MVP: hardcoded. Swap for redis.keys("stream:*:latest") to make dynamic.
    return StreamsResponse(
        streams=["realAWSCloudwatch/ec2_cpu_utilization_5f5533"]
    )


@app.get("/stream/{stream_id:path}/latest", response_model=StreamResult)
def get_latest(stream_id: str):
    redis = get_redis()
    raw = redis.get(f"stream:{stream_id}:latest")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for stream '{stream_id}'. Is the consumer running?"
        )
    return StreamResult(**json.loads(raw))


@app.get("/stream/{stream_id:path}/history", response_model=HistoryResponse)
def get_history(stream_id: str):
    redis = get_redis()
    raw_list = redis.lrange(f"stream:{stream_id}:history", 0, -1)
    if not raw_list:
        raise HTTPException(
            status_code=404,
            detail=f"No history found for stream '{stream_id}'. Is the consumer running?"
        )
    parsed = [StreamResult(**json.loads(item)) for item in raw_list]
    parsed.reverse()  # LPUSH means newest-first; reverse so Recharts plots left-to-right
    return HistoryResponse(stream_id=stream_id, count=len(parsed), results=parsed)


@app.get("/stream/{stream_id:path}/drift", response_model=DriftResponse)
def get_drift(stream_id: str):
    redis = get_redis()
    raw_list = redis.lrange(f"stream:{stream_id}:drift_events", 0, -1)
    parsed = [DriftEvent(**json.loads(item)) for item in raw_list]
    parsed.reverse()  # same ordering fix — oldest drift first for timeline
    return DriftResponse(stream_id=stream_id, count=len(parsed), events=parsed)