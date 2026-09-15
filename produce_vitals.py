#!/usr/bin/env python3
"""
Kafka vitals producer with Avro serialization.

Sends simulated patient vitals to Kafka with Schema Registry validation.
Patient IDs are drawn from the set populated by the V2__enrich_patients_for_kafka
migration, so every record joins to a row in the MySQL `patients` table (the
Trino federated join reads this topic via `kafka.default."telemetry.vitals"`).

For the plain-JSON variant, see producer.py.
"""

import logging
import os
import random
import time
from datetime import datetime
from typing import Any

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from producer import PATIENT_IDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry.vitals")

# Avro schema for patient vitals. Trino auto-discovers this schema from the
# Schema Registry subject (kafka.table-description-supplier=CONFLUENT), so the
# columns exposed to Trino match this definition exactly.
VITALS_SCHEMA = """
{
    "namespace": "healthcare.analytics",
    "type": "record",
    "name": "Vitals",
    "fields": [
        {"name": "patient_id", "type": "string"},
        {"name": "heart_rate", "type": "int"},
        {"name": "blood_pressure_systolic", "type": "int"},
        {"name": "blood_pressure_diastolic", "type": "int"},
        {"name": "temperature", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}
"""


def delivery_callback(err: Any, msg: Any) -> None:
    """Called once for each message to indicate delivery result.

    Args:
        err: Error message if delivery failed, None on success.
        msg: The delivered message metadata.
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


def create_producer() -> SerializingProducer:
    """Create and return a configured Kafka SerializingProducer.

    Returns:
        A SerializingProducer with Avro serialization configured.
    """
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, VITALS_SCHEMA)

    return SerializingProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "value.serializer": avro_serializer,
        }
    )


def generate_vitals() -> dict[str, Any]:
    """Generate random patient vital signs data.

    Returns:
        A dictionary containing vitals for a patient in the mapped ID set.
    """
    return {
        "patient_id": random.choice(PATIENT_IDS),
        "heart_rate": random.randint(60, 120),
        "blood_pressure_systolic": random.randint(110, 140),
        "blood_pressure_diastolic": random.randint(70, 90),
        "temperature": round(random.uniform(36.5, 37.5), 1),
        "timestamp": int(datetime.now().timestamp() * 1000),
    }


def run_producer() -> None:
    """Run the vitals producer in an infinite loop.

    Produces simulated vitals data to Kafka until interrupted.
    """
    producer = create_producer()
    logger.info("Vitals producer started. Sending to topic: %s", KAFKA_TOPIC)
    logger.info("Bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Schema registry: %s", SCHEMA_REGISTRY_URL)
    logger.info("Press Ctrl+C to stop")

    message_count = 0
    try:
        while True:
            data = generate_vitals()
            producer.produce(
                topic=KAFKA_TOPIC,
                value=data,
                on_delivery=delivery_callback,
            )
            producer.poll(0)
            message_count += 1
            logger.info("[%d] Sent: %s", message_count, data)
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping producer. Total messages sent: %d", message_count)
    finally:
        producer.flush(timeout=5)


def main() -> None:
    """Main entry point."""
    run_producer()


if __name__ == "__main__":
    main()
