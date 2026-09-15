#!/usr/bin/env python3
"""
Kafka Avro consumer utility.

Consumes and deserializes Avro-encoded messages from a Kafka topic.
Useful for verifying Schema Registry integration and message format.
"""

import logging
import os
import time

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "healthcare_server.healthcare.patients")


def run_consumer() -> None:
    """Run the Avro consumer to read and deserialize messages."""
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "avro-tester-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC_NAME])

    logger.info("Monitoring topic: %s", TOPIC_NAME)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error() is not None:
                error = msg.error()
                # UNKNOWN_TOPIC_OR_PART = 3 (confluent-kafka has no type stubs)
                if error is not None and error.code() == 3:
                    logger.warning("Topic not found yet. Waiting for data to appear...")
                    time.sleep(5)
                else:
                    logger.error("Kafka error: %s", error)
                continue

            try:
                decoded_data = avro_deserializer(msg.value(), None)
                if decoded_data and "after" in decoded_data:
                    patient = decoded_data["after"]
                    logger.info(
                        "Patient found: %s %s",
                        patient.get("FIRST", "N/A"),
                        patient.get("LAST", "N/A"),
                    )
                    break
            except SerializationError:
                continue  # Skip non-Avro messages

    except KeyboardInterrupt:
        logger.info("Consumer stopped")
    finally:
        consumer.close()


def main() -> None:
    """Main entry point."""
    run_consumer()


if __name__ == "__main__":
    main()
