"""
cache.py
Manages Redis caching for latest F&O metrics.
FastAPI reads from Redis instead of hitting PostgreSQL on every request.
TTL of 90 seconds ensures stale data auto-expires between cycles.
"""

import json
import logging
import redis
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Redis connection — local Docker instance
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_TTL  = 90  # seconds — slightly longer than our 60s fetch interval

# Redis key names
KEY_LATEST_METRICS = "fo:metrics:latest"
KEY_LIVE_CHAIN     = "fo:chain:live"
KEY_ANOMALIES      = "fo:anomalies:latest"


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True  # return strings not bytes
    )


def cache_latest_metrics(metrics: dict):
    """
    Caches the latest batch metrics (PCR, max pain, spot price).
    Called by the Flink consumer after each batch is processed.
    """
    r = get_redis_client()
    metrics["cached_at"] = datetime.now(timezone.utc).isoformat()
    r.setex(KEY_LATEST_METRICS, REDIS_TTL, json.dumps(metrics))
    logger.info(f"Cached latest metrics to Redis (TTL {REDIS_TTL}s)")


def cache_live_chain(records: list):
    """
    Caches the full options chain from the latest batch.
    Only stores the most relevant columns for API response.
    """
    r = get_redis_client()
    slim_records = [
        {
            "strike":        r_["strike"],
            "option_type":   r_["option_type"],
            "open_interest": r_["open_interest"],
            "oi_shift":      r_.get("oi_shift", 0),
            "last_price":    r_["last_price"],
            "implied_volatility": r_["implied_volatility"],
            "volume":        r_["volume"],
        }
        for r_ in records
    ]
    r.setex(KEY_LIVE_CHAIN, REDIS_TTL, json.dumps(slim_records))
    logger.info(f"Cached {len(slim_records)} chain records to Redis")


def cache_anomalies(anomalies: list):
    """
    Caches the latest anomaly detection results.
    """
    r = get_redis_client()
    r.setex(KEY_ANOMALIES, REDIS_TTL, json.dumps(anomalies))
    logger.info(f"Cached {len(anomalies)} anomalies to Redis")


def get_cached_metrics() -> dict | None:
    r = get_redis_client()
    data = r.get(KEY_LATEST_METRICS)
    return json.loads(data) if data else None


def get_cached_chain() -> list | None:
    r = get_redis_client()
    data = r.get(KEY_LIVE_CHAIN)
    return json.loads(data) if data else None


def get_cached_anomalies() -> list | None:
    r = get_redis_client()
    data = r.get(KEY_ANOMALIES)
    return json.loads(data) if data else None