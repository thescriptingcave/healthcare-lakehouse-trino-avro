#!/usr/bin/env python3
"""
Docker vitals producer with Avro serialization.

Identical to produce_vitals.py but defaults to Docker-internal hostnames, so it
can be run from anywhere on the compose network.

Use this script when running from within the Docker network.
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
from confluent_kafka.serialization import StringSerializer

from produce_vitals import VITALS_SCHEMA
from producer import PATIENT_IDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration for Docker internal network
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telemetry.vitals")


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
            "Delivered to %s [Partition: %d]",
            msg.topic(),
            msg.partition(),
        )


def create_producer() -> SerializingProducer:
    """Create and return a configured Kafka SerializingProducer for Docker.

    Returns:
        A SerializingProducer with Avro serialization configured.
    """
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, VITALS_SCHEMA)
    string_serializer = StringSerializer("utf_8")

    return SerializingProducer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "key.serializer": string_serializer,
            "value.serializer": avro_serializer,
            "client.id": "docker-vitals-producer",
        }
    )


def generate_vitals() -> dict[str, Any]:
    """Generate random patient vital signs data.

    Returns:
        A dictionary containing vitals for a patient in the mapped ID set.
    """
    return {
        "patient_id": random.choice(PATIENT_IDS),
        "heart_rate": random.randint(60, 100),
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
    logger.info("Docker vitals producer started. Sending to topic: %s", KAFKA_TOPIC)
    logger.info("Bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Press Ctrl+C to stop")

    message_count = 0
    try:
        while True:
            data = generate_vitals()
            producer.produce(
                topic=KAFKA_TOPIC,
                key=data["patient_id"],
                value=data,
                on_delivery=delivery_callback,
            )
            producer.flush()
            message_count += 1
            logger.info("[%d] Sent: %s", message_count, data)
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
