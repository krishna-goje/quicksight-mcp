"""Tests for structured logging and correlation IDs."""

import json
import logging
import os

from quicksight_mcp.logging_config import (
    StructuredJsonFormatter,
    new_correlation_id,
    get_correlation_id,
    set_tool_name,
    get_tool_name,
    log_tool_start,
    log_tool_complete,
    setup_logging,
    _sanitize_params,
)


class TestCorrelationIds:
    """Tests for correlation ID management."""

    def test_new_correlation_id_format(self):
        cid = new_correlation_id()
        assert cid.startswith("cid_")
        assert len(cid) == 12  # "cid_" + 8 hex chars

    def test_correlation_id_persists_in_context(self):
        cid = new_correlation_id()
        assert get_correlation_id() == cid

    def test_new_id_replaces_old(self):
        cid1 = new_correlation_id()
        cid2 = new_correlation_id()
        assert cid1 != cid2
        assert get_correlation_id() == cid2

    def test_set_and_get_tool_name(self):
        set_tool_name("list_datasets")
        assert get_tool_name() == "list_datasets"


class TestStructuredJsonFormatter:
    """Tests for the JSON log formatter."""

    def test_format_produces_valid_json(self):
        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="Test message", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "Test message"
        assert "timestamp" in parsed

    def test_format_includes_correlation_id(self):
        cid = new_correlation_id()
        set_tool_name("update_dataset_sql")

        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="Tool call", args=(), exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["correlation_id"] == cid
        assert parsed["tool_name"] == "update_dataset_sql"

    def test_format_includes_extra_fields(self):
        formatter = StructuredJsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="",
            lineno=0, msg="Done", args=(), exc_info=None,
        )
        record.event = "tool_call_complete"
        record.duration_ms = 1234.5
        record.success = True

        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["event"] == "tool_call_complete"
        assert parsed["duration_ms"] == 1234.5
        assert parsed["success"] is True


class TestSetupLogging:
    """Tests for the logging setup function."""

    def test_setup_creates_log_dir(self, tmp_path):
        log_dir = str(tmp_path / "logs")
        setup_logging(log_dir=log_dir, log_level="DEBUG")

        assert os.path.isdir(log_dir)

        # Clean up handlers to avoid polluting other tests
        root = logging.getLogger()
        for handler in root.handlers[:]:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                root.removeHandler(handler)
                handler.close()

    def test_setup_adds_file_handler(self, tmp_path):
        log_dir = str(tmp_path / "logs")
        setup_logging(log_dir=log_dir)

        root = logging.getLogger()
        file_handlers = [
            h for h in root.handlers
            if isinstance(h, logging.handlers.RotatingFileHandler)
        ]
        assert len(file_handlers) >= 1

        # Clean up
        for h in file_handlers:
            root.removeHandler(h)
            h.close()


class TestLogHelpers:
    """Tests for the structured log helper functions."""

    def test_log_tool_start_does_not_raise(self):
        new_correlation_id()
        set_tool_name("test_tool")
        # Should not raise
        log_tool_start("test_tool", {"dataset_id": "ds-123"})

    def test_log_tool_complete_does_not_raise(self):
        new_correlation_id()
        set_tool_name("test_tool")
        log_tool_complete(
            "test_tool", 42.5, success=True, resource_id="ds-123"
        )

    def test_log_tool_complete_failure(self):
        new_correlation_id()
        log_tool_complete(
            "update_sql", 100.0, success=False,
            error="Token expired", error_type="auth_expired",
        )


class TestEndToEndJsonlFile:
    """Test that structured logs end up as valid JSONL in the file."""

    def test_log_writes_valid_jsonl(self, tmp_path):
        import json
        log_dir = str(tmp_path / "logs")
        setup_logging(log_dir=log_dir, log_level="DEBUG")

        # Set context
        new_correlation_id()
        set_tool_name("test_tool")

        # Emit a structured log
        log_tool_start("test_tool", {"dataset_id": "ds-123"})
        log_tool_complete("test_tool", 42.5, success=True, resource_id="ds-123")

        # Flush all handlers
        root = logging.getLogger()
        for h in root.handlers:
            h.flush()

        # Read the file and parse each line as JSON
        log_file = str(tmp_path / "logs" / "mcp_server.jsonl")
        with open(log_file) as f:
            lines = [line.strip() for line in f if line.strip()]

        assert len(lines) >= 2, f"Expected >= 2 log lines, got {len(lines)}"
        for line in lines:
            parsed = json.loads(line)  # Must be valid JSON
            assert "timestamp" in parsed
            assert "level" in parsed

        # Check that the structured fields are present
        complete_line = json.loads(lines[-1])
        assert complete_line.get("event") == "tool_call_complete"
        assert complete_line.get("duration_ms") == 42.5
        assert complete_line.get("success") is True

        # Clean up handlers
        for handler in root.handlers[:]:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                root.removeHandler(handler)
                handler.close()


class TestSanitizeParams:
    """Tests for parameter sanitization."""

    def test_short_values_preserved(self):
        params = {"dataset_id": "ds-123", "name": "WBR"}
        result = _sanitize_params(params)
        assert result == params

    def test_long_strings_truncated(self):
        params = {"sql": "SELECT " + "x" * 300}
        result = _sanitize_params(params)
        assert len(result["sql"]) < 200
        assert "chars)" in result["sql"]

    def test_non_string_values_preserved(self):
        params = {"limit": 10, "backup": True}
        result = _sanitize_params(params)
        assert result == params

    def test_empty_dict(self):
        assert _sanitize_params({}) == {}
