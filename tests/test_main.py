"""Tests for main.py - CSV to Parquet loader."""

from main import sql_escape


class TestSqlEscape:
    """Tests for the sql_escape function."""

    def test_string_value(self) -> None:
        """String values should be quoted."""
        assert sql_escape("hello") == "'hello'"

    def test_integer_value(self) -> None:
        """Integer values should be string-quoted."""
        assert sql_escape(42) == "'42'"

    def test_float_value(self) -> None:
        """Float values should be string-quoted."""
        assert sql_escape(3.14) == "'3.14'"

    def test_none_value(self) -> None:
        """None values should become NULL."""
        assert sql_escape(None) == "NULL"

    def test_nan_value(self) -> None:
        """NaN values should become NULL."""
        assert sql_escape(float("nan")) == "NULL"

    def test_empty_string(self) -> None:
        """Empty strings should be quoted."""
        assert sql_escape("") == "''"

    def test_sql_injection_prevention(self) -> None:
        """Single quotes should be escaped to prevent SQL injection."""
        assert sql_escape("O'Brien") == "'O''Brien'"

    def test_special_characters(self) -> None:
        """Special characters should be preserved."""
        assert sql_escape("test@example.com") == "'test@example.com'"


class TestMainModule:
    """Tests for main module configuration."""

    def test_csv_path_default(self) -> None:
        """CSV path should have a default value."""
        from main import CSV_PATH

        # Should be set from env or default
        assert isinstance(CSV_PATH, str)

    def test_batch_size_positive(self) -> None:
        """Batch size should be a positive integer."""
        from main import BATCH_SIZE

        assert BATCH_SIZE > 0
