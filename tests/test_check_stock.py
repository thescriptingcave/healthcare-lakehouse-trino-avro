"""Tests for check_stock.py - Stack health checker."""

from unittest.mock import MagicMock, patch

from check_stock import (
    KAFKA_UI_URL,
    SUPERSET_URL,
    TRINO_URL,
    check_service,
    run_health_checks,
)


class TestCheckService:
    """Tests for the check_service function."""

    @patch("check_stock.requests.get")
    def test_healthy_service(self, mock_get: MagicMock) -> None:
        """Should return True for 200 status."""
        mock_get.return_value = MagicMock(status_code=200)
        result = check_service("Test Service", "http://localhost:8080")
        assert result is True

    @patch("check_stock.requests.get")
    def test_unhealthy_service(self, mock_get: MagicMock) -> None:
        """Should return False for non-200 status."""
        mock_get.return_value = MagicMock(status_code=503)
        result = check_service("Test Service", "http://localhost:8080")
        assert result is False

    @patch("check_stock.requests.get")
    def test_unreachable_service(self, mock_get: MagicMock) -> None:
        """Should return False for connection errors."""
        import requests

        mock_get.side_effect = requests.ConnectionError("Connection refused")
        result = check_service("Test Service", "http://localhost:8080")
        assert result is False

    @patch("check_stock.requests.get")
    def test_timeout_service(self, mock_get: MagicMock) -> None:
        """Should return False for timeout."""
        import requests

        mock_get.side_effect = requests.Timeout("Timed out")
        result = check_service("Test Service", "http://localhost:8080", timeout=1)
        assert result is False


class TestRunHealthChecks:
    """Tests for the run_health_checks function."""

    def _fail(self, urls: set[str]) -> MagicMock:
        mock_get = MagicMock()
        mock_get.side_effect = lambda url, timeout: (
            MagicMock(status_code=503)
            if any(url.startswith(u) for u in urls)
            else MagicMock(status_code=200)
        )
        return mock_get

    @patch("check_stock.requests.get")
    def test_all_required_healthy(self, mock_get: MagicMock) -> None:
        """Should pass when all services (including optionals) respond."""
        mock_get.return_value = MagicMock(status_code=200)
        assert run_health_checks() is True

    @patch("check_stock.requests.get")
    def test_failed_optional_services_still_pass(self, mock_get: MagicMock) -> None:
        """Indisposed Kafka UI / Superset (optional) should not fail the check."""
        mock_get.side_effect = self._fail({KAFKA_UI_URL, SUPERSET_URL}).side_effect
        assert run_health_checks() is True

    @patch("check_stock.requests.get")
    def test_failed_required_service_fails(self, mock_get: MagicMock) -> None:
        """An unavailable required service (e.g. Trino) must fail the check."""
        mock_get.side_effect = self._fail({TRINO_URL}).side_effect
        assert run_health_checks() is False
