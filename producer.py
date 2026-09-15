#!/usr/bin/env python3
"""
JSON Kafka producer for the vitals data feed.

Sends simulated patient vitals to the `vitals` topic as plain JSON.
Patient IDs ('P####') match the patient_id column added by the
V2__enrich_patients_for_kafka migration, enabling the Trino federated
join (live Kafka stream <-> MySQL patients).

For the Avro / Schema Registry variant, see produce_vitals.py.
"""

import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Any

from confluent_kafka import SerializingProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "vitals")

# Patient IDs populated by scripts/migration/V2__enrich_patients_for_kafka.sql.
# Emitting only these IDs guarantees every vitals record joins to a row in the
# MySQL `patients` table, so the Trino federated join is always 1:1.
PATIENT_IDS = os.getenv(
    "VITALS_PATIENT_IDS",
    "P1001,P2002,P3003,P3194,P4004,P5005,P5469,P8250",
).split(",")


def generate_vitals() -> dict[str, Any]:
    """Generate a random patient vitals record.

    Returns:
        A dictionary of vital signs with a patient ID from the mapped set.
    """
    return {
        "patient_id": random.choice(PATIENT_IDS),
        "heart_rate": random.randint(60, 100),
        "blood_pressure_systolic": random.randint(110, 140),
        "blood_pressure_diastolic": random.randint(70, 90),
        "temperature": round(random.uniform(36.5, 37.5), 1),
        "timestamp": int(datetime.now().timestamp() * 1000),
    }


def delivery_callback(err: Any, msg: Any) -> None:
    """Called once per produced message to indicate delivery result.

    Args:
        err: Error message if delivery failed, None on success.
        msg: Delivered message metadata.
    """
    if err is not None:
        logger.error("Delivery failed: %s", err)
    else:
        logger.info(
            "Delivered to %s [Partition: %d, Offset: %d]",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


def run_producer() -> None:
    """Produce vitals records to Kafka until interrupted."""
    producer = SerializingProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "value.serializer": lambda v, ctx: json.dumps(v).encode("utf-8"),
        }
    )

    logger.info("Starting producer for topic: %s", TOPIC_NAME)
    logger.info("Bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Press Ctrl+C to stop")

    message_count = 0
    try:
        while True:
            vitals_data = generate_vitals()
            producer.produce(
                topic=TOPIC_NAME,
                value=vitals_data,
                on_delivery=delivery_callback,
            )
            producer.flush()
            message_count += 1
            logger.info("[%d] Sent: %s", message_count, vitals_data)
            time.sleep(2)
    except KeyboardInterrupt:
        logger.info("Stopping producer. Total messages sent: %d", message_count)
    finally:
        producer.flush(timeout=5)


def main() -> None:
    """Main entry point."""
    run_producer()


if __name__ == "__main__":
    main()
