#!/usr/bin/env python3
"""
Kafka vitals producer with Avro serialization.

Sends simulated patient vitals to Kafka with Schema Registry validation.
Supports both local and Docker environments via environment variables.
"""

import logging
import os
import random
import time
from typing import Any

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry.vitals")

# Avro schema for patient vitals
VITALS_SCHEMA = """
{
    "namespace": "healthcare.analytics",
    "type": "record",
    "name": "Vitals",
    "fields": [
        {"name": "patient_id", "type": "int"},
        {"name": "heart_rate", "type": "int"},
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
        A dictionary containing patient_id, heart_rate, and timestamp.
    """
    return {
        "patient_id": random.randint(1, 10),
        "heart_rate": random.randint(60, 120),
        "timestamp": int(time.time()),
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
