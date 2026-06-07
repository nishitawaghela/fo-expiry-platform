# Futures & Options Expiry Analytics Platform

> Real-time NSE F&O expiry intelligence — automated OI analysis, PCR tracking, max pain computation, and pre-market Excel reporting. Every Thursday, automatically.

[![Live Dashboard](https://img.shields.io/badge/Live_Dashboard-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://fo-expiry-platform-njbwcctmjdoalzksh68rad.streamlit.app/)

---

##  The Problem

Every Thursday, NSE F&O contracts expire. Before market opens at 9:15 AM, derivatives traders and analysts need to answer three questions:

- **Where is money concentrated?** — Open Interest by strike
- **Is the market bullish or bearish?** — Put-Call Ratio
- **Where will the market be pulled?** — Max Pain strike

Today, analysts answer these questions by manually downloading CSVs from NSE, cleaning them in Excel, and computing metrics by hand — a process that takes 1-2 hours and produces a report that's already stale by the time it's ready.

This platform automates the entire workflow in real time.

---

##  What It Does

- **Ingests** live NSE Nifty options chain data every 60 seconds (212 records across 101 strikes)
- **Computes** OI shift, Put-Call Ratio, and Max Pain continuously as data arrives
- **Detects** unusual institutional positioning using Isolation Forest anomaly detection
- **Stores** historical snapshots in a Delta Lake with ACID guarantees for ML backtesting
- **Serves** live metrics via a FastAPI REST API with Redis caching (~17ms response)
- **Delivers** a formatted 5-sheet Excel report every Thursday before market open — automatically

---

##  Architecture

```
NSE India (live F&O chain)
        ↓
pnsea (handles NSE session & rate limiting)
        ↓
Kafka Producer → Kafka Broker (Docker, 3 partitions)
        ↓
Python Stream Processor
  ├── compute_oi_shift()    per-strike OI change
  ├── compute_pcr()         PE OI ÷ CE OI
  └── compute_max_pain()    simulate losses at every strike
        ↓
    ┌────────────────────────────────────┐
    │                                    │
Delta Lake (local Parquet)        Supabase PostgreSQL
ACID, time travel                 raw snapshots + metrics
partitioned by date + expiry            ↓
        ↓                         dbt (6 models)
Isolation Forest                  stg → int → mart
OI anomaly detection                    ↓
        ↓                         Excel Report (openpyxl)
Redis Cache (90s TTL)             5 sheets, charts, max pain
        ↓
FastAPI REST API
/metrics/latest  /chain/live  /anomalies  /metrics/history
        ↓
Streamlit Dashboard (live)
```

---

##  Live Dashboard

The Streamlit dashboard reads directly from Supabase PostgreSQL and shows:

- **Market Summary** — Symbol, Expiry, Spot Price, PCR, Max Pain, Sentiment
- **OI by Strike** — Bar chart of CE vs PE open interest across all strikes
- **PCR Trend** — Line chart of put-call ratio movement across batches
- **Anomaly Flags** — Strikes with unusual OI shift velocity

---

##  Project Structure

```
fo-expiry-platform/
├── services/
│   ├── kafka_producer/
│   │   ├── nse_fetcher.py          # Fetches live NSE F&O chain via pnsea
│   │   └── producer.py             # Publishes 212 records/cycle to Kafka
│   ├── flink_processor/
│   │   ├── flink_consumer.py       # Stateful stream processor
│   │   └── anomaly_detector.py     # Isolation Forest on OI shift history
│   ├── fastapi_app/
│   │   ├── main.py                 # REST API (4 endpoints)
│   │   └── cache.py                # Redis cache manager
│   └── excel_reporter/
│       └── excel_reporter.py       # 5-sheet Excel report generator
├── dbt_models/fo_expiry/
│   └── models/
│       ├── staging/                # stg_oi_snapshots, stg_expiry_metrics
│       ├── intermediate/           # int_oi_by_strike, int_pcr_trend
│       └── marts/                  # mart_expiry_summary, mart_max_pain
├── docker/
│   └── docker-compose.yml          # Kafka + Zookeeper + Redis
├── streamlit_app.py                # Live dashboard
└── data/delta_lake/                # Local Parquet store
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Ingestion | Apache Kafka, pnsea, Python |
| Stream Processing | Python stateful processor (Flink concepts) |
| Storage | Delta Lake (delta-rs), Supabase PostgreSQL |
| Transformation | dbt (6 models — staging, intermediate, marts) |
| ML | Scikit-learn Isolation Forest |
| Caching | Redis (Docker, 90s TTL) |
| API | FastAPI, uvicorn |
| Reporting | openpyxl (5-sheet Excel report) |
| Dashboard | Streamlit |
| Orchestration | Docker Compose |

---

## Key Metrics (Measured)

| Metric | Value |
|---|---|
| Records per cycle | 212 (101 strikes × CE + PE) |
| Delta Lake write latency | ~80ms per batch |
| Isolation Forest training | ~65ms on 2,800+ records |
| Anomaly detection | ~6ms per 200-record batch |
| API response (Redis cache) | ~17ms average |
| dbt models | 6 (all passing) |
| Excel report sheets | 5 |

---

##  Running Locally

**Prerequisites:** Python 3.12, Docker

```bash
# 1. Clone the repo
git clone https://github.com/nishitawaghela/fo-expiry-platform.git
cd fo-expiry-platform

# 2. Start Kafka + Redis
docker compose -f docker/docker-compose.yml up -d

# 3. Set up environment
cp .env.example .env
# Fill in your Supabase DATABASE_URL in .env

# 4. Install dependencies
pip install -r services/kafka_producer/requirements.txt
pip install -r services/flink_processor/requirements.txt
pip install -r services/fastapi_app/requirements.txt

# 5. Run the pipeline (3 terminals)
# Terminal 1 — Producer
cd services/kafka_producer && python producer.py

# Terminal 2 — Consumer
cd services/flink_processor && python flink_consumer.py

# Terminal 3 — API
cd services/fastapi_app && python main.py

# 6. Generate Excel report
cd services/excel_reporter && python excel_reporter.py

# 7. Run Streamlit dashboard
streamlit run streamlit_app.py
```

---

##  Financial Context

**Open Interest (OI)** — Total outstanding contracts at each strike. High OI acts as a gravitational magnet — the market tends to close near high OI strikes on expiry day.

**Put-Call Ratio (PCR)** — Total PE OI ÷ Total CE OI. PCR > 1.2 = bearish, PCR < 0.7 = bullish, between = neutral.

**Max Pain** — The strike where total option buyer losses are maximum. Institutions (option sellers) profit most here, so the market is theoretically pulled toward this level on expiry.

**OI Shift** — Change in open interest between snapshots. A sudden spike at a specific strike signals a large player entering a position — this is what the Isolation Forest flags.

---

##  API Endpoints

```
GET /health           — Service health check
GET /metrics/latest   — Latest PCR, max pain, spot price (Redis cached)
GET /chain/live       — Full 200-record options chain
GET /anomalies        — Isolation Forest flagged strikes
GET /metrics/history  — PCR trend across last N batches
```

