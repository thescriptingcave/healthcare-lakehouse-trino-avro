#!/usr/bin/env python3
"""
Offline Avro file writer for patient vitals.

Generates a sample Avro file with patient vitals data that can be
loaded into MinIO/S3 for testing.
"""

import logging
import os
import time
from typing import Any

from fastavro import parse_schema, writer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Avro schema for patient vitals (string patient_id for MySQL UUID join)
VITALS_SCHEMA = {
    "doc": "Patient Vital Signs",
    "name": "vitals",
    "type": "record",
    "fields": [
        {"name": "patient_id", "type": "string"},
        {"name": "heart_rate", "type": "int"},
        {"name": "blood_pressure_sys", "type": "int"},
        {"name": "ts", "type": "long"},
    ],
}


def generate_sample_records() -> list[dict[str, Any]]:
    """Generate sample patient vital signs records.

    Returns:
        A list of vitals dictionaries.
    """
    return [
        {
            "patient_id": "110339b10-3cd1-4ac3-ac13-ec26728cb592",
            "heart_rate": 78,
            "blood_pressure_sys": 125,
            "ts": int(time.time()),
        }
    ]


def write_avro_file(output_path: str, records: list[dict[str, Any]]) -> None:
    """Write records to an Avro file.

    Args:
        output_path: Path to the output Avro file.
        records: List of records to write.
    """
    parsed_schema = parse_schema(VITALS_SCHEMA)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "wb") as out:
        writer(out, parsed_schema, records)

    logger.info("Avro file created at %s with %d records", output_path, len(records))


def main() -> None:
    """Main entry point."""
    output_path = os.getenv(
        "AVRO_OUTPUT_PATH",
        "./minio-data/healthcare-avro/vitals/patient_data.avro",
    )
    records = generate_sample_records()
    write_avro_file(output_path, records)


if __name__ == "__main__":
    main()
