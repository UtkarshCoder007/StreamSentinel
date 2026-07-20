# StreamSentinel

Real-time anomaly detection on streaming financial data using online machine learning.
Detects anomalies and concept drift without retraining — sub-5ms end-to-end latency.

![Python](https://img.shields.io/badge/Python-3.11-3b82f6?style=flat-square)
![FastAPI](https://img.shields.io/badge/FastAPI-0.1-22c55e?style=flat-square)
![React](https://img.shields.io/badge/React-18-f97316?style=flat-square)
![Kafka](https://img.shields.io/badge/Kafka-7.5-a855f7?style=flat-square)
![River](https://img.shields.io/badge/River-0.25-ef4444?style=flat-square)

---

## What it does

StreamSentinel replays financial time-series data (NAB AWS CloudWatch metrics, IEEE-CIS
fraud transactions) through a Kafka topic and scores each event in real time using
River's HalfSpaceTrees online ML model. Unlike batch systems, the model learns
continuously from every incoming message without retraining. ADWIN monitors the anomaly
score stream and detects concept drift — automatically resetting the model when the
underlying data distribution changes. A FastAPI layer reads results from Redis and serves
a live React dashboard showing anomaly scores, latency metrics, and drift events as they
happen.

---

## Architecture

```
NAB CSV / IEEE-CIS
      │
      ▼
 nab_producer.py
 (configurable replay speed)
      │
      ▼  Kafka topic: financial-stream
 ─────────────────────────────────────
      │
      ▼
 nab_consumer.py
 ┌─────────────────────────────────┐
 │  poll message                   │
 │  → score_one()   [HST]          │
 │  → learn_one()   [HST]          │
 │  → adwin.update(score)  [ADWIN] │
 │  → write to Redis               │
 │  → commit offset                │
 └─────────────────────────────────┘
      │
      ▼
    Redis
 ┌─────────────────────────────────┐
 │  stream:{id}:latest             │
 │  stream:{id}:history  (500 cap) │
 │  stream:{id}:drift_events       │
 └─────────────────────────────────┘
      │
      ▼
  FastAPI  (uvicorn)
      │
      ▼
 React Dashboard  (Vite)
 ┌──────────────────────────────────────────┐
 │  StatusBar │ ScoreChart │ LatencyPanel   │
 │  MetricsPanel │ DriftTimeline            │
 └──────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka (Confluent 7.5, ZooKeeper) |
| Cache / state | Redis 7 |
| Online ML | River 0.25 — HalfSpaceTrees, ADWIN |
| API | FastAPI + Uvicorn, Pydantic |
| Frontend | React 18, Vite, Recharts, TypeScript |
| Infrastructure | Docker Compose |
| Language | Python 3.11, TypeScript |

---

## Key Design Decisions

**Online learning over batch ML**
HalfSpaceTrees updates incrementally on every message via `learn_one()` — no offline
training pipeline, no model deployment step, no staleness between retraining cycles.
The model is always current.

**Prequential evaluation**
Every data point is scored *before* the model learns from it (`score_one()` → `learn_one()`).
This gives an unbiased, real-time performance estimate — the model is always tested on
data it has not seen yet.

**ADWIN drift detection with warmup guard**
ADWIN monitors the anomaly score stream rather than raw values — detecting when the
model's behaviour changes, not just when the data changes. A warmup guard prevents
ADWIN from firing during HST's initial 250-message calibration window (when scores are
artificially 0.0), which would otherwise cause a false drift cascade.

**Model generation tracking**
Each HST reset increments a `model_generation` counter, written into every Redis entry
and drift event. The dashboard uses this to render Gen N → N+1 transitions on the
chart, making concept drift visually legible rather than an invisible internal event.

**Manual Kafka offset commits**
`enable_auto_commit=False` with per-message `consumer.commit()` after each successful
Redis write. This ensures at-least-once delivery — a consumer crash before committing
reprocesses the last message rather than silently losing it. For anomaly detection,
skipping a message is worse than processing it twice.

**Redis as a display cache, not a database**
Redis holds only derived results (scores, latencies, drift events) — fully re-derivable
by re-running the consumer. Keys are flushed on each consumer startup for a clean slate.
No TTL needed since the Docker stack itself serves as the lifecycle boundary.

**Sub-5ms end-to-end latency**
Measured from `event_time` (stamped by the producer at publish) to `t_written` (after
Redis write completes). p50 consistently 2–3ms, p95 under 5ms on local Docker —
well inside the 100ms design target. Score and learn latencies sub-millisecond on
univariate NAB data.

---

## Project Structure

```
StreamSentinel/
├── backend/
│   ├── app/
│   │   └── main.py              # FastAPI app, Pydantic schemas, Redis routes
│   ├── consumers/
│   │   └── nab_consumer.py      # Kafka consumer, HST scoring, ADWIN, Redis writes
│   ├── producers/
│   │   └── nab_producer.py      # NAB CSV replay producer, configurable speed
│   ├── data/
│   │   └── nab/                 # NAB CSVs (gitignored)
│   └── requirements.txt
├── frontend/
│   └── src/
│       ├── components/
│       │   ├── StatusBar.tsx
│       │   ├── ScoreChart.tsx
│       │   ├── LatencyPanel.tsx
│       │   ├── MetricsPanel.tsx
│       │   └── DriftTimeline.tsx
│       ├── App.tsx
│       ├── App.css
│       └── index.css
├── docker-compose.yml           # Kafka + ZooKeeper + Redis
└── README.md
```

---

## Prerequisites

- Docker Desktop (running)
- Python 3.11 with venv
- Node.js 18+ and npm

---

## Setup

**1. Clone the repo**
```bash
git clone https://github.com/UtkarshCoder007/StreamSentinel.git
cd StreamSentinel
```

**2. Start Kafka and Redis**
```bash
docker compose up -d
```

**3. Create the Kafka topic**
```bash
docker compose exec kafka kafka-topics \
  --create --topic financial-stream \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

**4. Set up the Python environment**
```bash
cd backend
python -m venv venv
venv\Scripts\Activate.ps1        # Windows PowerShell
pip install -r requirements.txt
```

**5. Download the NAB dataset**
```bash
curl -o data/nab/ec2_cpu_utilization_5f5533.csv \
  https://raw.githubusercontent.com/numenta/NAB/master/data/realAWSCloudwatch/ec2_cpu_utilization_5f5533.csv
```

**6. Install frontend dependencies**
```bash
cd ../frontend
npm install
```

---

## Running

Open four terminals from the `StreamSentinel/` root:

**Terminal 1 — Producer** (replay NAB data into Kafka)
```bash
cd backend
venv\Scripts\Activate.ps1
python producers/nab_producer.py --speed 500
```

**Terminal 2 — Consumer** (score messages, write to Redis)
```bash
cd backend
venv\Scripts\Activate.ps1
python consumers/nab_consumer.py
```

**Terminal 3 — API**
```bash
cd backend
venv\Scripts\Activate.ps1
uvicorn app.main:app --reload
```

**Terminal 4 — Dashboard**
```bash
cd frontend
npm run dev
```

Open **http://localhost:5173**

The dashboard auto-polls every 2 seconds. Anomaly scores appear after the HST warmup
period (~250 messages). Drift events appear in the timeline as ADWIN detects
distribution shifts and resets the model.

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/streams` | List active stream IDs |
| GET | `/stream/{stream_id}/latest` | Most recent scored message |
| GET | `/stream/{stream_id}/history` | Last 500 scored messages (chronological) |
| GET | `/stream/{stream_id}/drift` | All drift events (chronological) |

Interactive docs: **http://localhost:8000/docs**

---

## Roadmap

- [ ] IEEE-CIS fraud detection dataset (400+ features, class imbalance weighting)
- [ ] Dockerize FastAPI and React frontend for single-command startup
- [ ] NAB ground-truth labels integration for live ROCAUC tracking
- [ ] Configurable anomaly threshold via dashboard UI
- [ ] Multi-stream support with stream selector dropdown
