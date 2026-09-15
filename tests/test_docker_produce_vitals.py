"""Tests for docker_produce_vitals.py - Docker Kafka producer."""

from unittest.mock import MagicMock

from docker_produce_vitals import VITALS_SCHEMA, delivery_callback, generate_vitals


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

    def test_patient_id_in_range(self) -> None:
        """Patient ID should be between 1 and 10."""
        for _ in range(100):
            result = generate_vitals()
            assert 1 <= result["patient_id"] <= 10

    def test_heart_rate_in_range(self) -> None:
        """Heart rate should be between 60 and 110."""
        for _ in range(100):
            result = generate_vitals()
            assert 60 <= result["heart_rate"] <= 110


class TestDeliveryCallback:
    """Tests for the delivery_callback function."""

    def test_success_no_error(self) -> None:
        """Successful delivery should not raise."""
        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0
        delivery_callback(None, msg)

    def test_failure_with_error(self) -> None:
        """Failed delivery should not raise."""
        err = MagicMock()
        delivery_callback(err, MagicMock())


class TestVitalsSchema:
    """Tests for the Docker producer Avro schema."""

    def test_schema_matches_local_producer(self) -> None:
        """Docker schema should match the local producer schema."""
        import json

        from produce_vitals import VITALS_SCHEMA as LOCAL_SCHEMA

        local_parsed = json.loads(LOCAL_SCHEMA)
        docker_parsed = json.loads(VITALS_SCHEMA)

        assert local_parsed["fields"] == docker_parsed["fields"]
