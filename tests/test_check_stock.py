"""Tests for check_stock.py - Stack health checker."""

from unittest.mock import MagicMock, patch

from check_stock import check_service


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
