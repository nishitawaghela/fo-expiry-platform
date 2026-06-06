"""
flink_consumer.py
Reads NSE F&O options chain records from Kafka topic: nse-options-chain
Computes per batch:
  - OI shift per strike (current OI - previous OI)
  - PCR (total PE OI / total CE OI)
  - Max Pain (strike where total option buyer loss is maximum)
Writes results to:
  - Delta Lake (local) — raw OI snapshots with OI shift
  - Supabase PostgreSQL — aggregated metrics per batch
  - Redis — caches latest metrics and chain for FastAPI
"""

import json
import os
import sys
import logging
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import psycopg2
from deltalake import write_deltalake
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Fix import paths for sibling services
CURRENT_DIR  = os.path.dirname(os.path.abspath(__file__))
FASTAPI_DIR  = os.path.abspath(os.path.join(CURRENT_DIR, '../fastapi_app'))
if FASTAPI_DIR not in sys.path:
    sys.path.insert(0, FASTAPI_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from cache import cache_latest_metrics, cache_live_chain, cache_anomalies
from anomaly_detector import run_anomaly_detection

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "nse-options-chain")
DATABASE_URL            = os.getenv("DATABASE_URL")
DELTA_LAKE_PATH         = os.getenv("DELTA_LAKE_PATH", "../../data/delta_lake")
CONSUMER_GROUP_ID       = "fo-expiry-flink-processor"


# ── OI shift state ────────────────────────────────────────────────────────────
previous_oi_state: dict = {}


def compute_oi_shift(strike: float, option_type: str, current_oi: int) -> int:
    key = f"{strike}_{option_type}"
    previous_oi = previous_oi_state.get(key, current_oi)
    shift = current_oi - previous_oi
    previous_oi_state[key] = current_oi
    return shift


def compute_pcr(records: list):
    total_ce_oi = sum(r["open_interest"] for r in records if r["option_type"] == "CE")
    total_pe_oi = sum(r["open_interest"] for r in records if r["option_type"] == "PE")
    if total_ce_oi == 0:
        return 0.0, 0, 0
    pcr = round(total_pe_oi / total_ce_oi, 4)
    return pcr, total_ce_oi, total_pe_oi


def compute_max_pain(records: list) -> float:
    strikes = sorted(set(r["strike"] for r in records))
    ce_oi   = {r["strike"]: r["open_interest"] for r in records if r["option_type"] == "CE"}
    pe_oi   = {r["strike"]: r["open_interest"] for r in records if r["option_type"] == "PE"}

    max_pain_strike = None
    max_total_loss  = 0

    for candidate in strikes:
        total_loss = 0
        for strike, oi in ce_oi.items():
            if candidate > strike:
                total_loss += (candidate - strike) * oi
        for strike, oi in pe_oi.items():
            if candidate < strike:
                total_loss += (strike - candidate) * oi
        if total_loss > max_total_loss:
            max_total_loss  = total_loss
            max_pain_strike = candidate

    return float(max_pain_strike) if max_pain_strike else 0.0


def write_to_delta_lake(records: list, batch_id: str):
    df = pd.DataFrame(records)
    df["partition_date"]   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df["partition_expiry"] = df["expiry"].str.replace("-", "_")
    delta_path = os.path.abspath(DELTA_LAKE_PATH)
    write_deltalake(delta_path, df, mode="append",
                    partition_by=["partition_date", "partition_expiry"])
    logger.info(f"Delta Lake: wrote {len(df)} records to {delta_path}")


def write_to_postgres(records: list, metrics: dict):
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()
    for r in records:
        cur.execute("""
            INSERT INTO oi_snapshots
                (batch_id, symbol, expiry, strike, option_type,
                 open_interest, oi_change, oi_shift, last_price,
                 implied_volatility, volume, spot_price, fetched_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            r["batch_id"], r["symbol"], r["expiry"], r["strike"],
            r["option_type"], r["open_interest"], r["oi_change"],
            r["oi_shift"], r["last_price"], r["implied_volatility"],
            r["volume"], r["spot_price"], r["fetched_at"]
        ))
    cur.execute("""
        INSERT INTO expiry_metrics
            (batch_id, symbol, expiry, spot_price,
             pcr, max_pain, total_ce_oi, total_pe_oi)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        metrics["batch_id"], metrics["symbol"], metrics["expiry"],
        metrics["spot_price"], metrics["pcr"], metrics["max_pain"],
        metrics["total_ce_oi"], metrics["total_pe_oi"]
    ))
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"PostgreSQL: wrote {len(records)} snapshots + 1 metrics row")


def process_batch(batch_id: str, records: list):
    logger.info(f"Processing batch {batch_id} with {len(records)} records")

    # 1. Compute OI shift per record
    for r in records:
        r["oi_shift"] = compute_oi_shift(r["strike"], r["option_type"], r["open_interest"])

    # 2. Compute PCR
    pcr, total_ce_oi, total_pe_oi = compute_pcr(records)

    # 3. Compute Max Pain
    max_pain = compute_max_pain(records)

    # 4. Log summary
    sample = records[0]
    logger.info(
        f"Batch summary | "
        f"Spot: {sample['spot_price']} | "
        f"Expiry: {sample['expiry']} | "
        f"PCR: {pcr} | "
        f"Max Pain: {max_pain} | "
        f"CE OI: {total_ce_oi:,} | "
        f"PE OI: {total_pe_oi:,}"
    )

    # 5. Build metrics dict
    metrics = {
        "batch_id":    batch_id,
        "symbol":      sample["symbol"],
        "expiry":      sample["expiry"],
        "spot_price":  sample["spot_price"],
        "pcr":         pcr,
        "max_pain":    max_pain,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
    }

    # 6. Write to Delta Lake and PostgreSQL
    write_to_delta_lake(records, batch_id)
    write_to_postgres(records, metrics)

    # 7. Cache to Redis
    cache_latest_metrics(metrics)
    cache_live_chain(records)

    # 8. Run anomaly detection and cache results
    anomalies = run_anomaly_detection()
    cache_anomalies(anomalies)

    logger.info(f"Batch {batch_id} complete.\n")


def main():
    logger.info("Starting F&O Expiry Analytics Flink Consumer...")

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           CONSUMER_GROUP_ID,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
    logger.info(f"Consumer group: {CONSUMER_GROUP_ID}")

    batch_buffer: dict = defaultdict(list)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                for batch_id, records in list(batch_buffer.items()):
                    if len(records) >= 200:
                        process_batch(batch_id, records)
                        del batch_buffer[batch_id]
                        consumer.commit()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"End of partition: {msg.partition()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                record  = json.loads(msg.value().decode("utf-8"))
                batch_id = record.get("batch_id", "unknown")
                batch_buffer[batch_id].append(record)

                if len(batch_buffer[batch_id]) >= 200:
                    process_batch(batch_id, batch_buffer[batch_id])
                    del batch_buffer[batch_id]
                    consumer.commit()

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
                continue

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()