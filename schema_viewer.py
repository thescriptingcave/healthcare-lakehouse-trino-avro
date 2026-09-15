#!/usr/bin/env python3
"""
Schema Registry viewer utility.

Fetches and displays the latest schema version for a given subject
from the Confluent Schema Registry.
"""

import json
import logging
import os
import sys
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration from environment or defaults
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SUBJECT = os.getenv("SCHEMA_SUBJECT", "healthcare_server.healthcare.patients-value")


def view_schema(subject: str) -> dict[str, Any]:
    """Fetch and return the latest schema for a subject.

    Args:
        subject: The Schema Registry subject name.

    Returns:
        The parsed Avro schema as a dictionary.

    Raises:
        SystemExit: If the schema cannot be fetched.
    """
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    try:
        schema_obj = sr_client.get_latest_version(subject)
    except Exception as e:
        logger.error("Failed to fetch schema for subject '%s': %s", subject, e)
        sys.exit(1)

    if schema_obj is None:
        logger.error("No schema found for subject '%s'", subject)
        sys.exit(1)

    if schema_obj.schema is None or schema_obj.schema.schema_str is None:
        logger.error("Schema object has no schema string for subject '%s'", subject)
        sys.exit(1)

    schema_dict: dict[str, Any] = json.loads(schema_obj.schema.schema_str)
    logger.info("Schema version %d for subject '%s'", schema_obj.version, subject)
    return schema_dict


def main() -> None:
    """Main entry point."""
    schema = view_schema(SUBJECT)
    print(json.dumps(schema, indent=2))


if __name__ == "__main__":
    main()
