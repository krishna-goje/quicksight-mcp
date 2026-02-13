"""Test server module components and custom exceptions.

Note: The server module imports tool modules and client at module level.
These tests focus on components that can be tested independently:
- Custom exception classes
- Learning components (tested separately in test_learning.py)
- Server initialization patterns (tested via mocking)
"""

import pytest


class TestExceptions:
    """Test custom exception classes."""

    def test_concurrent_modification_error_attributes(self):
        """Test ConcurrentModificationError has correct attributes."""
        from quicksight_mcp.exceptions import ConcurrentModificationError

        err = ConcurrentModificationError("an-001", "2026-01-01", "2026-01-02")
        assert err.analysis_id == "an-001"
        assert err.expected_time == "2026-01-01"
        assert err.actual_time == "2026-01-02"

    def test_concurrent_modification_error_message(self):
        """Test ConcurrentModificationError message content."""
        from quicksight_mcp.exceptions import ConcurrentModificationError

        err = ConcurrentModificationError("an-001", "2026-01-01", "2026-01-02")
        msg = str(err)
        assert "an-001" in msg
        assert "modified by another session" in msg
        assert "2026-01-01" in msg
        assert "2026-01-02" in msg

    def test_change_verification_error_attributes(self):
        """Test ChangeVerificationError has correct attributes."""
        from quicksight_mcp.exceptions import ChangeVerificationError

        err = ChangeVerificationError(
            "add_calculated_field", "an-001", "Field not found after creation"
        )
        assert err.operation == "add_calculated_field"
        assert err.resource_id == "an-001"
        assert err.details == "Field not found after creation"

    def test_change_verification_error_message(self):
        """Test ChangeVerificationError message content."""
        from quicksight_mcp.exceptions import ChangeVerificationError

        err = ChangeVerificationError("update_sql", "ds-001", "SQL unchanged")
        msg = str(err)
        assert "verification failed" in msg
        assert "update_sql" in msg
        assert "ds-001" in msg
        assert "not reflected" in msg

    def test_destructive_change_error_attributes(self):
        """Test DestructiveChangeError has correct attributes."""
        from quicksight_mcp.exceptions import DestructiveChangeError

        current = {"sheets": 3, "visuals": 15, "calculated_fields": 10}
        new = {"sheets": 0, "visuals": 0, "calculated_fields": 0}
        err = DestructiveChangeError(
            "an-001", "Would delete all sheets", current, new
        )
        assert err.analysis_id == "an-001"
        assert err.current_counts == current
        assert err.new_counts == new

    def test_destructive_change_error_message(self):
        """Test DestructiveChangeError message includes BLOCKED warning."""
        from quicksight_mcp.exceptions import DestructiveChangeError

        current = {"sheets": 3, "visuals": 15}
        new = {"sheets": 0, "visuals": 0}
        err = DestructiveChangeError(
            "an-001", "Would delete all content", current, new
        )
        msg = str(err)
        assert "BLOCKED" in msg
        assert "an-001" in msg
        assert "allow_destructive=True" in msg

    def test_exceptions_are_catchable_as_exception(self):
        """Test that all custom exceptions inherit from Exception."""
        from quicksight_mcp.exceptions import (
            ConcurrentModificationError,
            ChangeVerificationError,
            DestructiveChangeError,
        )

        assert issubclass(ConcurrentModificationError, Exception)
        assert issubclass(ChangeVerificationError, Exception)
        assert issubclass(DestructiveChangeError, Exception)

    def test_exceptions_can_be_caught_in_try_block(self):
        """Test that custom exceptions work in try/except."""
        from quicksight_mcp.exceptions import ConcurrentModificationError

        with pytest.raises(ConcurrentModificationError):
            raise ConcurrentModificationError("an-001", "t1", "t2")

    def test_destructive_change_error_with_allow_hint(self):
        """Test that the error message tells user how to override."""
        from quicksight_mcp.exceptions import DestructiveChangeError

        err = DestructiveChangeError(
            "an-001", "Details", {"sheets": 1}, {"sheets": 0}
        )
        assert "allow_destructive=True" in str(err)


class TestVersionInfo:
    """Test package version metadata."""

    def test_version_is_set(self):
        """Test that __version__ is defined."""
        from quicksight_mcp import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)

    def test_version_format(self):
        """Test that version follows semver format."""
        from quicksight_mcp import __version__

        parts = __version__.split(".")
        assert len(parts) == 3
        # All parts should be numeric
        for part in parts:
            assert part.isdigit()

    def test_version_is_0_2_0(self):
        """Test version is 0.2.0."""
        from quicksight_mcp import __version__

        assert __version__ == "0.2.0"
