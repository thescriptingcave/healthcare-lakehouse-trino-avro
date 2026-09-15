"""Tests for generate_vitals.py - Offline Avro file writer."""

import json
import os
import tempfile
from typing import cast

import fastavro

from generate_vitals import (
    VITALS_SCHEMA,
    generate_sample_records,
    write_avro_file,
)


def get_schema_fields() -> list[dict[str, str]]:
    """Return the field specifications from the vitals schema."""
    fields = cast("list[dict[str, str]]", VITALS_SCHEMA["fields"])
    return fields


class TestGenerateSampleRecords:
    """Tests for the generate_sample_records function."""

    def test_returns_list(self) -> None:
        """Should return a list of records."""
        records = generate_sample_records()
        assert isinstance(records, list)

    def test_records_have_required_fields(self) -> None:
        """Each record should have all required fields."""
        records = generate_sample_records()
        for record in records:
            assert "patient_id" in record
            assert "heart_rate" in record
            assert "blood_pressure_sys" in record
            assert "ts" in record

    def test_patient_id_is_string(self) -> None:
        """Patient ID should be a string (UUID format)."""
        records = generate_sample_records()
        for record in records:
            assert isinstance(record["patient_id"], str)
            assert len(record["patient_id"]) > 0

    def test_heart_rate_is_int(self) -> None:
        """Heart rate should be an integer."""
        records = generate_sample_records()
        for record in records:
            assert isinstance(record["heart_rate"], int)

    def test_blood_pressure_is_int(self) -> None:
        """Blood pressure should be an integer."""
        records = generate_sample_records()
        for record in records:
            assert isinstance(record["blood_pressure_sys"], int)


class TestWriteAvroFile:
    """Tests for the write_avro_file function."""

    def test_creates_file(self) -> None:
        """Should create an Avro file at the specified path."""
        records = generate_sample_records()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test.avro")
            write_avro_file(output_path, records)
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0

    def test_file_is_valid_avro(self) -> None:
        """Written file should be readable as valid Avro."""
        records = generate_sample_records()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "test.avro")
            write_avro_file(output_path, records)

            with open(output_path, "rb") as f:
                read_records = list(fastavro.reader(f))
                assert len(read_records) == len(records)

    def test_creates_parent_directories(self) -> None:
        """Should create parent directories if they don't exist."""
        records = generate_sample_records()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "nested", "dir", "test.avro")
            write_avro_file(output_path, records)
            assert os.path.exists(output_path)


class TestVitalsSchemaDefinition:
    """Tests for the Avro schema definition."""

    def test_schema_is_record_type(self) -> None:
        """Schema should be of type 'record'."""
        assert VITALS_SCHEMA["type"] == "record"

    def test_schema_is_valid_json(self) -> None:
        """Schema should serialize to valid JSON."""
        json.dumps(VITALS_SCHEMA)

    def test_schema_has_all_fields(self) -> None:
        """Schema should define all required fields."""
        field_names = [f["name"] for f in get_schema_fields()]
        assert "patient_id" in field_names
        assert "heart_rate" in field_names
        assert "blood_pressure_sys" in field_names
        assert "ts" in field_names

    def test_patient_id_is_string_type(self) -> None:
        """patient_id field should be of type 'string'."""
        fields = {f["name"]: f["type"] for f in get_schema_fields()}
        assert fields["patient_id"] == "string"
