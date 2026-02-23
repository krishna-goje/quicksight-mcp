"""Response formatting utilities for the MCP tool layer.

Provides pagination, truncation, and structured error formatting.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

CHARACTER_LIMIT = 25_000


def paginate_list(
    items: List[Any],
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """Apply limit/offset pagination to a list of items.

    Returns:
        dict with ``items``, ``total_count``, ``has_more``, ``next_offset``.
    """
    limit = max(1, limit)
    offset = max(0, offset)
    total = len(items)
    page = items[offset : offset + limit]
    has_more = offset + limit < total

    result: Dict[str, Any] = {
        "items": page,
        "total_count": total,
        "count": len(page),
        "has_more": has_more,
    }
    if has_more:
        result["next_offset"] = offset + limit

    return result


def truncate_if_needed(result: Dict[str, Any], tool_name: str = "") -> Dict[str, Any]:
    """Truncate oversized dict responses to fit within CHARACTER_LIMIT."""
    import json as _json

    try:
        serialized = _json.dumps(result, default=str)
    except (TypeError, ValueError):
        return result

    if len(serialized) <= CHARACTER_LIMIT:
        return result

    # Truncate the largest list values
    for key in sorted(result, key=lambda k: len(str(result[k])), reverse=True):
        if isinstance(result[key], list) and len(result[key]) > 5:
            result[key] = result[key][: len(result[key]) // 2]
            result["_truncated"] = True
            result["_note"] = (
                f"Response truncated to fit {CHARACTER_LIMIT} character limit. "
                f"Use limit/offset parameters for full results."
            )
            try:
                if len(_json.dumps(result, default=str)) <= CHARACTER_LIMIT:
                    return result
            except (TypeError, ValueError):
                pass

    return result


def format_error_response(
    error: Exception,
    error_type: str = "unexpected",
    suggestions: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    past_recovery: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build a structured error response dict for MCP clients.

    Args:
        error: The exception that occurred.
        error_type: Machine-readable error category.
        suggestions: Actionable recovery steps.
        metadata: Structured debugging context.
        past_recovery: Recovery suggestions from memory.
    """
    response: Dict[str, Any] = {
        "isError": True,
        "error_type": error_type,
        "error": str(error),
    }
    if suggestions:
        response["suggestions"] = suggestions
    if metadata:
        response["metadata"] = metadata
    if past_recovery:
        response["past_recovery"] = past_recovery
    return response
