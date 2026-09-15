"""Tests for producer.py - JSON vitals producer used by the demo."""

import time
from unittest.mock import MagicMock

from producer import PATIENT_IDS, delivery_callback, generate_vitals


class TestGenerateVitals:
    """Tests for the generate_vitals function."""

    def test_returns_dict(self) -> None:
        """Should return a dictionary."""
        result = generate_vitals()
        assert isinstance(result, dict)

    def test_contains_required_keys(self) -> None:
        """Should contain all vitals fields."""
        result = generate_vitals()
        assert "patient_id" in result
        assert "heart_rate" in result
        assert "blood_pressure_systolic" in result
        assert "blood_pressure_diastolic" in result
        assert "temperature" in result
        assert "timestamp" in result

    def test_patient_id_from_mapped_set(self) -> None:
        """Patient ID should come from the V2 migration mapped set."""
        for _ in range(100):
            result = generate_vitals()
            assert result["patient_id"] in PATIENT_IDS

    def test_heart_rate_in_range(self) -> None:
        """Heart rate should be between 60 and 100."""
        for _ in range(100):
            result = generate_vitals()
            assert 60 <= result["heart_rate"] <= 100

    def test_blood_pressure_ranges(self) -> None:
        """Blood pressure should be in plausible ranges."""
        for _ in range(100):
            result = generate_vitals()
            assert 110 <= result["blood_pressure_systolic"] <= 140
            assert 70 <= result["blood_pressure_diastolic"] <= 90

    def test_temperature_is_float_in_range(self) -> None:
        """Temperature should be a plausible body temperature."""
        for _ in range(100):
            result = generate_vitals()
            assert isinstance(result["temperature"], float)
            assert 36.5 <= result["temperature"] <= 37.5

    def test_timestamp_is_millisecond_epoch(self) -> None:
        """Timestamp should be millisecond epoch, close to now."""
        result = generate_vitals()
        now_ms = time.time() * 1000
        assert isinstance(result["timestamp"], int)
        assert abs(result["timestamp"] - now_ms) < 5000


class TestDeliveryCallback:
    """Tests for the delivery_callback function."""

    def test_success_no_error(self) -> None:
        """Successful delivery should not raise."""
        msg = MagicMock()
        msg.topic.return_value = "vitals"
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        delivery_callback(None, msg)

    def test_failure_with_error(self) -> None:
        """Failed delivery should log error and not raise."""
        err = MagicMock()
        err.__str__ = lambda self: "test error"  # type: ignore[misc, assignment, method-assign]
        msg = MagicMock()
        delivery_callback(err, msg)
