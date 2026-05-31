"""
producer.py
Fetches live NSE F&O options chain data and publishes
each record as a JSON message to the Kafka topic: nse-options-chain.

Run this file directly to start producing:
    python producer.py
"""

import json
import time
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from nse_fetcher import fetch_fo_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Kafka config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME              = "nse-options-chain"
NUM_PARTITIONS          = 3
REPLICATION_FACTOR      = 1

# How often to fetch and publish (in seconds)
# NSE rate limits ~3-4 requests/min, so 60s is safe
FETCH_INTERVAL_SECONDS  = 60


# ── Topic creation ────────────────────────────────────────────────────────────
def create_topic_if_not_exists(bootstrap_servers: str, topic: str):
    """
    Creates the Kafka topic if it doesn't already exist.
    AdminClient is used for cluster management operations
    (creating/deleting topics) — separate from the Producer.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing_topics = admin.list_topics(timeout=10).topics

    if topic in existing_topics:
        logger.info(f"Topic '{topic}' already exists.")
        return

    new_topic = NewTopic(
        topic,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )
    futures = admin.create_topics([new_topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
            raise


# ── Delivery callback ─────────────────────────────────────────────────────────
def delivery_callback(err, msg):
    """
    Called automatically by Kafka after each message is
    acknowledged by the broker — either confirmed or failed.
    This is how we know a message was actually stored in Kafka.
    """
    if err:
        logger.error(f"Message delivery FAILED: {err}")
    else:
        logger.debug(
            f"Delivered → topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# ── Message key ───────────────────────────────────────────────────────────────
def make_key(record: dict) -> str:
    """
    Partition key: symbol + strike + option_type
    Example: 'NIFTY_23500.0_CE'

    Using strike as part of the key ensures all records
    for the same strike always go to the same partition.
    This guarantees ordered processing per strike in Flink.
    """
    return f"{record['symbol']}_{record['strike']}_{record['option_type']}"


# ── Core publish function ─────────────────────────────────────────────────────
def publish_batch(producer: Producer, records: list, batch_id: str):
    """
    Publishes a list of records to Kafka.
    Each record becomes one Kafka message.
    We add batch_id so Flink can group all 212 records
    from the same fetch cycle together.
    """
    published = 0
    for record in records:
        # Tag each record with batch metadata
        record["batch_id"] = batch_id
        record["published_at"] = datetime.now(timezone.utc).isoformat()

        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=make_key(record),
                value=json.dumps(record),
                callback=delivery_callback
            )
            published += 1

            # Poll every 10 messages to trigger delivery callbacks
            # Without this, callbacks queue up and memory grows
            if published % 10 == 0:
                producer.poll(0)

        except KafkaException as e:
            logger.error(f"Failed to produce message: {e}")
            continue

    # Flush waits until all queued messages are delivered to the broker
    # This ensures no messages are lost when the function returns
    producer.flush()
    logger.info(f"Batch {batch_id}: published {published}/{len(records)} records")


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    logger.info("Starting F&O Expiry Analytics Producer...")

    # Create kafka topic if it doesn't exist
    create_topic_if_not_exists(KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME)

    # Initialize producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Retry up to 3 times on transient failures
        "retries": 3,
        # Wait up to 5ms to batch messages before sending
        # Small batches = lower latency for near-real-time data
        "linger.ms": 5,
        # Compress messages with snappy for lower network overhead
        "compression.type": "snappy",
    })

    logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Publishing to topic: {TOPIC_NAME}")
    logger.info(f"Fetch interval: {FETCH_INTERVAL_SECONDS}s")

    cycle = 0
    while True:
        cycle += 1
        batch_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_cycle{cycle}"

        try:
            logger.info(f"--- Cycle {cycle} | Fetching NSE data ---")
            data = fetch_fo_data()

            logger.info(
                f"Fetched {len(data['records'])} records | "
                f"Spot: {data['spot_price']} | "
                f"Expiry: {data['expiry']}"
            )

            publish_batch(producer, data["records"], batch_id)

        except Exception as e:
            logger.error(f"Cycle {cycle} failed: {e}")
            # Don't crash the loop on a single failure
            # NSE sometimes blocks briefly — next cycle will retry

        logger.info(f"Sleeping {FETCH_INTERVAL_SECONDS}s until next fetch...\n")
        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()