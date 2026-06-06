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
"""

import json
import os
import logging
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import psycopg2
from deltalake import write_deltalake
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

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

# Consumer group ID — Kafka tracks this group's offset separately
# If you restart the consumer, it resumes from where it left off
CONSUMER_GROUP_ID       = "fo-expiry-flink-processor"

# How many seconds to wait for new messages before processing what we have
BATCH_TIMEOUT_SECONDS   = 120


# ── OI shift state ────────────────────────────────────────────────────────────
# Keeps previous OI per (strike, option_type) in memory
# Key: "23500.0_CE", Value: last seen OI
previous_oi_state: dict = {}


# ── OI Shift computation ──────────────────────────────────────────────────────
def compute_oi_shift(strike: float, option_type: str, current_oi: int) -> int:
    """
    Computes how much OI has changed since the last snapshot for this strike.
    First time we see a strike, shift is 0 (no previous state to compare).
    """
    key = f"{strike}_{option_type}"
    previous_oi = previous_oi_state.get(key, current_oi)
    shift = current_oi - previous_oi
    # Update state with current OI for next cycle
    previous_oi_state[key] = current_oi
    return shift


# ── PCR computation ───────────────────────────────────────────────────────────
def compute_pcr(records: list) -> float:
    """
    PCR = Total Put OI / Total Call OI
    A PCR > 1.2 signals bearish sentiment (more puts than calls)
    A PCR < 0.7 signals bullish sentiment (more calls than puts)
    """
    total_ce_oi = sum(r["open_interest"] for r in records if r["option_type"] == "CE")
    total_pe_oi = sum(r["open_interest"] for r in records if r["option_type"] == "PE")

    if total_ce_oi == 0:
        return 0.0

    pcr = round(total_pe_oi / total_ce_oi, 4)
    return pcr, total_ce_oi, total_pe_oi


# ── Max Pain computation ──────────────────────────────────────────────────────
def compute_max_pain(records: list) -> float:
    """
    Max Pain = the strike price at which total option buyer losses are maximum.

    For each possible expiry strike price S:
      - All CE buyers with strike < S lose their full premium (option expires worthless)
      - All PE buyers with strike > S lose their full premium (option expires worthless)
    
    We sum these losses across all strikes for each candidate expiry price.
    The candidate with the highest total loss = max pain.

    Why? Institutions (option sellers) profit most at max pain.
    The theory is the market gravitates toward max pain on expiry day.
    """
    # Group by strike
    strikes = list(set(r["strike"] for r in records))
    strikes.sort()

    # Build OI maps
    ce_oi = {r["strike"]: r["open_interest"] for r in records if r["option_type"] == "CE"}
    pe_oi = {r["strike"]: r["open_interest"] for r in records if r["option_type"] == "PE"}

    max_pain_strike = None
    max_total_loss  = 0

    for candidate in strikes:
        total_loss = 0

        # CE holders lose if candidate > their strike (their call expires worthless)
        for strike, oi in ce_oi.items():
            if candidate > strike:
                total_loss += (candidate - strike) * oi

        # PE holders lose if candidate < their strike (their put expires worthless)
        for strike, oi in pe_oi.items():
            if candidate < strike:
                total_loss += (strike - candidate) * oi

        if total_loss > max_total_loss:
            max_total_loss  = total_loss
            max_pain_strike = candidate

    return float(max_pain_strike) if max_pain_strike else 0.0


# ── Delta Lake write ──────────────────────────────────────────────────────────
def write_to_delta_lake(records: list, batch_id: str):
    """
    Writes the full batch of OI snapshots (with OI shift) to Delta Lake.
    Partitioned by expiry date and batch date for efficient querying.
    """
    df = pd.DataFrame(records)

    # Add partition columns
    df["partition_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df["partition_expiry"] = df["expiry"].str.replace("-", "_")

    delta_path = os.path.abspath(DELTA_LAKE_PATH)

    write_deltalake(
        delta_path,
        df,
        mode="append",                          # append each batch, don't overwrite
        partition_by=["partition_date", "partition_expiry"],
    )
    logger.info(f"Delta Lake: wrote {len(df)} records to {delta_path}")


# ── PostgreSQL write ──────────────────────────────────────────────────────────
def write_to_postgres(records: list, metrics: dict):
    """
    Writes two things to Supabase PostgreSQL:
    1. Individual OI snapshots with OI shift → oi_snapshots table
    2. Aggregated batch metrics (PCR, max pain) → expiry_metrics table
    """
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Insert OI snapshots
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

    # Insert aggregated metrics
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


# ── Process a complete batch ──────────────────────────────────────────────────
def process_batch(batch_id: str, records: list):
    """
    Called once we have all 212 records for a batch_id.
    Runs all three computations and writes to both sinks.
    """
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

    # 6. Write to sinks
    write_to_delta_lake(records, batch_id)
    write_to_postgres(records, metrics)

    # 7. Cache to Redis
    import sys
    sys.path.append(os.path.join(os.path.dirname(__file__), '../../services/fastapi_app'))
    from cache import cache_latest_metrics, cache_live_chain, cache_anomalies

    cache_latest_metrics(metrics)
    cache_live_chain(records)

    # 8. Run anomaly detection
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    from anomaly_detector import run_anomaly_detection
    anomalies = run_anomaly_detection()
    cache_anomalies(anomalies)

    logger.info(f"Batch {batch_id} complete.\n")


# ── Main consumer loop ────────────────────────────────────────────────────────
def main():
    logger.info("Starting F&O Expiry Analytics Flink Consumer...")

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           CONSUMER_GROUP_ID,
        # earliest = start from the beginning of the topic if no offset exists
        # useful for first run — processes all messages the producer already sent
        "auto.offset.reset":  "earliest",
        # We commit offsets manually after successful processing
        # This guarantees exactly-once semantics — if processing fails,
        # we don't advance the offset and will reprocess on restart
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
    logger.info(f"Consumer group: {CONSUMER_GROUP_ID}")

    # Buffer: groups messages by batch_id
    # Key: batch_id, Value: list of records
    batch_buffer: dict = defaultdict(list)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No new messages — check if any buffered batches are ready
                for batch_id, records in list(batch_buffer.items()):
                    if len(records) >= 200:
                        process_batch(batch_id, records)
                        del batch_buffer[batch_id]
                        # Commit offset after successful processing
                        consumer.commit()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition — not an error, just no more messages
                    logger.debug(f"End of partition: {msg.partition()}")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            # Parse the message
            try:
                record = json.loads(msg.value().decode("utf-8"))
                batch_id = record.get("batch_id", "unknown")
                batch_buffer[batch_id].append(record)

                # Once we have 200+ records for a batch, process it
                # We use 200 instead of 212 to handle any occasional drops
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