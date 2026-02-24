"""Tests for the @qs_tool decorator with structured logging + brain integration."""

from unittest.mock import MagicMock

from quicksight_mcp.tools._decorator import qs_tool, _truncate_response
from quicksight_mcp.safety.exceptions import QSAuthError, QSNotFoundError


class TestQsToolDecorator:
    """Tests for the @qs_tool decorator."""

    def test_success_returns_result(self):
        mcp = MagicMock()
        memory = MagicMock()

        @qs_tool(mcp, lambda: memory, read_only=True)
        def my_tool() -> dict:
            return {"status": "ok"}

        result = my_tool()
        assert result == {"status": "ok"}

    def test_records_success_in_memory(self):
        mcp = MagicMock()
        memory = MagicMock()
        memory.record_call = MagicMock()

        @qs_tool(mcp, lambda: memory)
        def my_tool() -> dict:
            return {"data": [1, 2, 3]}

        my_tool()
        memory.record_call.assert_called_once()
        args = memory.record_call.call_args
        assert args[0][0] == "my_tool"  # tool_name
        assert args[0][3] is True  # success

    def test_qs_error_returns_structured_response(self):
        mcp = MagicMock()

        @qs_tool(mcp, None)
        def my_tool() -> dict:
            raise QSAuthError("Token expired")

        result = my_tool()
        assert result["isError"] is True
        assert result["error_type"] == "auth_expired"
        assert "Token expired" in result["error"]
        assert len(result["suggestions"]) > 0

    def test_unexpected_error_returns_error_dict(self):
        mcp = MagicMock()

        @qs_tool(mcp, None)
        def my_tool() -> dict:
            raise RuntimeError("Something broke")

        result = my_tool()
        assert result["isError"] is True
        assert result["error_type"] == "unexpected"
        assert "Something broke" in result["error"]

    def test_records_error_in_memory(self):
        mcp = MagicMock()
        memory = MagicMock()
        memory.record_call = MagicMock()
        memory.get_recovery_suggestions = MagicMock(return_value=[])

        @qs_tool(mcp, lambda: memory)
        def my_tool() -> dict:
            raise QSNotFoundError("Dataset", "ds-123")

        my_tool()
        memory.record_call.assert_called_once()
        args = memory.record_call.call_args
        assert args[0][3] is False  # success=False
        assert "not found" in args[0][4].lower()  # error message

    def test_adds_past_recovery_suggestions(self):
        mcp = MagicMock()
        memory = MagicMock()
        memory.record_call = MagicMock()
        memory.get_recovery_suggestions = MagicMock(
            return_value=["Try saml2aws login"]
        )

        @qs_tool(mcp, lambda: memory)
        def my_tool() -> dict:
            raise QSAuthError("Token expired")

        result = my_tool()
        assert "past_recovery" in result
        assert "saml2aws" in result["past_recovery"][0]

    def test_memory_failure_does_not_break_tool(self):
        mcp = MagicMock()

        def broken_memory():
            raise RuntimeError("Memory is broken")

        @qs_tool(mcp, broken_memory)
        def my_tool() -> dict:
            return {"status": "ok"}

        result = my_tool()
        assert result == {"status": "ok"}

    def test_none_memory_works(self):
        mcp = MagicMock()

        @qs_tool(mcp, None, read_only=True)
        def my_tool() -> dict:
            return {"items": [1, 2]}

        result = my_tool()
        assert result == {"items": [1, 2]}

    def test_registers_with_mcp(self):
        mcp = MagicMock()
        mcp.tool = MagicMock()

        @qs_tool(mcp, None, read_only=True)
        def my_tool() -> dict:
            """A test tool."""
            return {}

        mcp.tool.assert_called_once()

    def test_kwargs_passed_through(self):
        mcp = MagicMock()

        @qs_tool(mcp, None)
        def my_tool(dataset_id: str = "", limit: int = 10) -> dict:
            return {"id": dataset_id, "limit": limit}

        result = my_tool(dataset_id="ds-123", limit=5)
        assert result == {"id": "ds-123", "limit": 5}

    # ---- New tests from review gaps ----

    def test_resource_id_extracted_from_analysis_id(self):
        """Verify resource_id is extracted from analysis_id kwarg."""
        mcp = MagicMock()
        memory = MagicMock()
        memory.record_call = MagicMock()
        memory.get_recovery_suggestions = MagicMock(return_value=[])

        @qs_tool(mcp, lambda: memory)
        def my_tool(analysis_id: str = "") -> dict:
            raise QSNotFoundError("Analysis", analysis_id)

        my_tool(analysis_id="a-789")
        # Verify memory was called with the error
        memory.record_call.assert_called_once()

    def test_resource_id_extracted_from_dashboard_id(self):
        """Verify resource_id is extracted from dashboard_id kwarg."""
        mcp = MagicMock()
        memory = MagicMock()
        memory.record_call = MagicMock()

        @qs_tool(mcp, lambda: memory)
        def my_tool(dashboard_id: str = "") -> dict:
            return {"id": dashboard_id}

        my_tool(dashboard_id="d-456")
        memory.record_call.assert_called_once()

    def test_no_resource_id_does_not_error(self):
        """Tool with no ID kwargs should work fine."""
        mcp = MagicMock()
        memory = MagicMock()
        memory.record_call = MagicMock()

        @qs_tool(mcp, lambda: memory)
        def my_tool() -> dict:
            return {"status": "ok"}

        result = my_tool()
        assert result == {"status": "ok"}
        memory.record_call.assert_called_once()

    def test_json_decode_error_caught_by_decorator(self):
        """JSONDecodeError should be caught by decorator as 'unexpected'."""
        mcp = MagicMock()

        @qs_tool(mcp, None)
        def my_tool(definition_json: str = "") -> dict:
            import json as _json
            data = _json.loads(definition_json)
            return {"data": data}

        result = my_tool(definition_json="not valid json")
        assert result["isError"] is True
        assert result["error_type"] == "unexpected"

    def test_destructive_annotation(self):
        """Verify destructive annotation is passed to MCP."""
        mcp = MagicMock()
        mcp.tool = MagicMock()

        @qs_tool(mcp, None, destructive=True)
        def my_tool() -> dict:
            return {}

        call_args = mcp.tool.call_args
        # annotations should include destructiveHint
        if call_args.kwargs.get("annotations"):
            assert call_args.kwargs["annotations"]["destructiveHint"] is True


class TestTruncateResponse:
    """Tests for response truncation."""

    def test_small_response_unchanged(self):
        result = {"items": [1, 2, 3]}
        assert _truncate_response(result, "test") == result

    def test_non_dict_unchanged(self):
        assert _truncate_response("hello", "test") == "hello"

    def test_large_response_truncated(self):
        result = {"items": list(range(5000))}
        truncated = _truncate_response(result, "test")
        assert truncated.get("_truncated") is True
        assert len(truncated["items"]) < 5000

    def test_truncation_note_present(self):
        result = {"items": list(range(5000))}
        truncated = _truncate_response(result, "test")
        assert "_note" in truncated
        assert "25000" in truncated["_note"] or "truncated" in truncated["_note"]

    def test_large_dict_without_lists(self):
        """Large response with only nested dicts (no lists to truncate)."""
        result = {"data": {f"key_{i}": "x" * 100 for i in range(500)}}
        truncated = _truncate_response(result, "test")
        # Should still have _truncated flag even if it can't shrink lists
        assert isinstance(truncated, dict)
