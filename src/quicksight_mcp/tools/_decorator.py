"""@qs_tool decorator — eliminates ~930 lines of repeated boilerplate.

Wraps every MCP tool function with:
- Timing (records duration in milliseconds)
- Memory recording (tool call + params)
- Structured error formatting with recovery suggestions
- Tool annotation registration (read-only, destructive, idempotent hints)
"""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable, Optional

from quicksight_mcp.safety.exceptions import QSError

logger = logging.getLogger(__name__)

# Character limit for tool responses
CHARACTER_LIMIT = 25_000


def qs_tool(
    mcp: Any,
    get_memory: Optional[Callable] = None,
    *,
    read_only: bool = False,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
):
    """Decorator that registers a function as an MCP tool with standard wrappers.

    Args:
        mcp: The FastMCP server instance.
        get_memory: Callable that returns the MemoryManager (lazy init).
        read_only: Tool only reads data, never modifies.
        destructive: Tool may delete or overwrite data.
        idempotent: Calling the tool twice with the same args has the same effect.
        open_world: Tool may interact with external systems.

    Usage::

        @qs_tool(mcp, get_memory, read_only=True)
        def list_datasets() -> dict:
            \"\"\"List all datasets.\"\"\"
            return services.datasets.list_all()
    """

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> dict:
            tool_name = fn.__name__
            start = time.time()

            try:
                result = fn(*args, **kwargs)
                duration_ms = (time.time() - start) * 1000

                # Record success in memory
                if get_memory:
                    try:
                        memory = get_memory()
                        if memory:
                            memory.record_call(
                                tool_name, kwargs, duration_ms, True
                            )
                    except Exception:
                        pass  # Never let memory recording break the tool

                # Truncate if too long
                result = _truncate_response(result, tool_name)
                return result

            except QSError as e:
                duration_ms = (time.time() - start) * 1000

                # Record error in memory
                if get_memory:
                    try:
                        memory = get_memory()
                        if memory:
                            memory.record_call(
                                tool_name, kwargs, duration_ms, False, str(e)
                            )
                    except Exception:
                        pass

                # Build structured error response
                error_response = {
                    "isError": True,
                    "error_type": e.error_type,
                    "error": str(e),
                    "suggestions": e.suggestions,
                    "metadata": e.metadata,
                }

                # Add recovery suggestions from memory
                if get_memory:
                    try:
                        memory = get_memory()
                        if memory:
                            past = memory.get_recovery_suggestions(
                                e.resource_id, e.error_type
                            )
                            if past:
                                error_response["past_recovery"] = past
                    except Exception:
                        pass

                return error_response

            except Exception as e:
                duration_ms = (time.time() - start) * 1000

                # Record error in memory
                if get_memory:
                    try:
                        memory = get_memory()
                        if memory:
                            memory.record_call(
                                tool_name, kwargs, duration_ms, False, str(e)
                            )
                    except Exception:
                        pass

                return {
                    "isError": True,
                    "error_type": "unexpected",
                    "error": str(e),
                }

        # Register with MCP using tool annotations
        annotations = {}
        if read_only:
            annotations["readOnlyHint"] = True
        if destructive:
            annotations["destructiveHint"] = True
        if idempotent:
            annotations["idempotentHint"] = True
        if open_world:
            annotations["openWorldHint"] = True

        # Register with FastMCP — pass annotations if supported
        try:
            mcp.tool(wrapper, annotations=annotations)
        except TypeError:
            # Fallback for FastMCP versions without annotations param
            mcp.tool(wrapper)

        return wrapper

    return decorator


def _truncate_response(result: Any, tool_name: str) -> Any:
    """Truncate overly large responses with guidance."""
    if not isinstance(result, dict):
        return result

    import json

    try:
        serialized = json.dumps(result, default=str)
    except (TypeError, ValueError):
        return result

    if len(serialized) <= CHARACTER_LIMIT:
        return result

    # Actually truncate: try removing list items from the largest list value
    # until we're under the limit, then add truncation metadata
    truncated = dict(result)
    for key in sorted(
        truncated.keys(),
        key=lambda k: len(json.dumps(truncated[k], default=str))
        if isinstance(truncated[k], (list, dict))
        else 0,
        reverse=True,
    ):
        val = truncated[key]
        if isinstance(val, list) and len(val) > 1:
            # Keep only enough items to stay under limit
            while len(val) > 1:
                val = val[: len(val) // 2]
                truncated[key] = val
                check = json.dumps(truncated, default=str)
                if len(check) <= CHARACTER_LIMIT - 200:
                    break
            if len(json.dumps(truncated, default=str)) <= CHARACTER_LIMIT:
                break

    truncated["_truncated"] = True
    truncated["_note"] = (
        f"Response exceeded {CHARACTER_LIMIT} characters and was truncated. "
        f"Use more specific queries or limit/offset parameters."
    )
    return truncated
