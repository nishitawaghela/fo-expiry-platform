"""
main.py
FastAPI app serving live NSE F&O metrics.
Reads from Redis cache first — falls back to PostgreSQL if cache is stale.

Endpoints:
  GET /health          — service health check
  GET /metrics/latest  — latest PCR, max pain, spot price
  GET /chain/live      — full options chain from latest batch
  GET /anomalies       — flagged strikes from Isolation Forest
  GET /metrics/history — last N batches from PostgreSQL
"""

import os
import logging
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from cache import (
    get_cached_metrics,
    get_cached_chain,
    get_cached_anomalies,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(
    title="F&O Expiry Analytics API",
    description="Real-time NSE F&O options chain metrics — OI shift, PCR, Max Pain, Anomalies",
    version="1.0.0",
)

# Allow all origins for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── DB helper ─────────────────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Simple health check — confirms API is alive."""
    return {"status": "ok", "service": "fo-expiry-analytics"}


@app.get("/metrics/latest")
def get_latest_metrics():
    """
    Returns the latest computed batch metrics:
    PCR, max pain, spot price, total CE/PE OI.
    Reads from Redis cache — sub-millisecond response.
    Falls back to PostgreSQL if cache is empty.
    """
    # Try Redis first
    cached = get_cached_metrics()
    if cached:
        cached["source"] = "redis_cache"
        return cached

    # Fallback to PostgreSQL
    logger.info("Cache miss — querying PostgreSQL for latest metrics")
    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM expiry_metrics
            ORDER BY computed_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="No metrics found yet.")

        result = dict(row)
        result["source"] = "postgresql_fallback"
        return result

    except Exception as e:
        logger.error(f"PostgreSQL error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch metrics.")


@app.get("/chain/live")
def get_live_chain():
    """
    Returns the full options chain from the latest batch.
    212 records — one per strike per option type (CE/PE).
    Reads from Redis cache.
    """
    cached = get_cached_chain()
    if cached:
        return {
            "source": "redis_cache",
            "count":  len(cached),
            "chain":  cached,
        }

    # Fallback to PostgreSQL
    logger.info("Cache miss — querying PostgreSQL for latest chain")
    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT strike, option_type, open_interest, oi_shift,
                   last_price, implied_volatility, volume
            FROM oi_snapshots
            WHERE batch_id = (SELECT batch_id FROM expiry_metrics ORDER BY computed_at DESC LIMIT 1)
            ORDER BY strike, option_type
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {
            "source": "postgresql_fallback",
            "count":  len(rows),
            "chain":  [dict(r) for r in rows],
        }

    except Exception as e:
        logger.error(f"PostgreSQL error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch chain.")


@app.get("/anomalies")
def get_anomalies():
    """
    Returns strikes flagged as anomalous by the Isolation Forest model.
    Lower anomaly_score = more anomalous.
    """
    cached = get_cached_anomalies()
    if cached is not None:
        return {
            "source":  "redis_cache",
            "count":   len(cached),
            "anomalies": cached,
        }

    return {
        "source":    "no_data",
        "count":     0,
        "anomalies": [],
        "message":   "Anomaly detection runs after each batch. Check back shortly.",
    }


@app.get("/metrics/history")
def get_metrics_history(limit: int = 10):
    """
    Returns the last N batch metrics from PostgreSQL.
    Useful for PCR trend analysis and the Excel report.
    Default: last 10 batches.
    """
    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT batch_id, symbol, expiry, spot_price,
                   pcr, max_pain, total_ce_oi, total_pe_oi, computed_at
            FROM expiry_metrics
            ORDER BY computed_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        return {
            "count":   len(rows),
            "history": [dict(r) for r in rows],
        }

    except Exception as e:
        logger.error(f"PostgreSQL error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch history.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)