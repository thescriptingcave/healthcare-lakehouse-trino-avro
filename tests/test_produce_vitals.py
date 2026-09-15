"""Tests for produce_vitals.py - Kafka vitals producer."""

import time
from unittest.mock import MagicMock

from produce_vitals import VITALS_SCHEMA, delivery_callback, generate_vitals
from producer import PATIENT_IDS


class TestGenerateVitals:
    """Tests for the generate_vitals function."""

    def test_returns_dict(self) -> None:
        """Should return a dictionary."""
        result = generate_vitals()
        assert isinstance(result, dict)

    def test_contains_required_keys(self) -> None:
        """Should contain patient_id, heart_rate, and timestamp."""
        result = generate_vitals()
        assert "patient_id" in result
        assert "heart_rate" in result
        assert "timestamp" in result

    def test_patient_id_in_mapped_set(self) -> None:
        """Patient ID should be a P#### string from the mapped Synthea set."""
        for _ in range(100):
            result = generate_vitals()
            assert isinstance(result["patient_id"], str)
            assert result["patient_id"] in PATIENT_IDS

    def test_heart_rate_in_range(self) -> None:
        """Heart rate should be between 60 and 120."""
        for _ in range(100):
            result = generate_vitals()
            assert 60 <= result["heart_rate"] <= 120

    def test_blood_pressure_in_range(self) -> None:
        """Blood pressure values should be within normal ranges."""
        for _ in range(100):
            result = generate_vitals()
            assert 110 <= result["blood_pressure_systolic"] <= 140
            assert 70 <= result["blood_pressure_diastolic"] <= 90

    def test_temperature_in_range(self) -> None:
        """Temperature should be a float around normal body temperature."""
        for _ in range(100):
            result = generate_vitals()
            assert isinstance(result["temperature"], float)
            assert 36.5 <= result["temperature"] <= 37.5

    def test_timestamp_is_epoch_ms(self) -> None:
        """Timestamp should be a reasonable epoch time in milliseconds."""
        result = generate_vitals()
        now = time.time() * 1000
        # Should be within 5 seconds (5000 ms) of now
        assert abs(result["timestamp"] - now) < 5000

    def test_timestamp_is_int(self) -> None:
        """Timestamp should be an integer."""
        result = generate_vitals()
        assert isinstance(result["timestamp"], int)


class TestDeliveryCallback:
    """Tests for the delivery_callback function."""

    def test_success_no_error(self) -> None:
        """Successful delivery should log info, not error."""
        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        # Should not raise
        delivery_callback(None, msg)

    def test_failure_with_error(self) -> None:
        """Failed delivery should log error."""
        err = MagicMock()
        err.__str__ = lambda self: "test error"  # type: ignore[misc, assignment, method-assign]
        msg = MagicMock()
        # Should not raise
        delivery_callback(err, msg)


class TestVitalsSchema:
    """Tests for the Avro schema definition."""

    def test_schema_is_valid_json(self) -> None:
        """Schema should be valid JSON."""
        import json

        parsed = json.loads(VITALS_SCHEMA)
        assert "type" in parsed
        assert "fields" in parsed

    def test_schema_has_required_fields(self) -> None:
        """Schema should define all vitals fields."""
        import json

        parsed = json.loads(VITALS_SCHEMA)
        field_names = [f["name"] for f in parsed["fields"]]
        assert "patient_id" in field_names
        assert "heart_rate" in field_names
        assert "blood_pressure_systolic" in field_names
        assert "blood_pressure_diastolic" in field_names
        assert "temperature" in field_names
        assert "timestamp" in field_names
