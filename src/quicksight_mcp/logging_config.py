"""Structured JSON logging for the QuickSight MCP server.

Provides:
- StructuredJsonFormatter: JSON-line output with correlation_id, tool_name, duration
- RotatingFileHandler: 10MB x 5 files to ~/.quicksight-mcp/logs/mcp_server.jsonl
- Human-readable stderr preserved for MCP transport (FastMCP needs it)
- contextvars for correlation IDs (generated per tool call in @qs_tool)
"""

from __future__ import annotations

import contextvars
import json
import logging
import logging.handlers
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Correlation ID via contextvars (thread-safe, async-safe)
# ---------------------------------------------------------------------------
_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default=""
)
_tool_name: contextvars.ContextVar[str] = contextvars.ContextVar(
    "tool_name", default=""
)


def new_correlation_id() -> str:
    """Generate and set a new correlation ID for the current context."""
    cid = f"cid_{uuid.uuid4().hex[:8]}"
    _correlation_id.set(cid)
    return cid


def get_correlation_id() -> str:
    """Get the correlation ID for the current context."""
    return _correlation_id.get()


def set_tool_name(name: str) -> None:
    """Set the tool name for the current context."""
    _tool_name.set(name)


def get_tool_name() -> str:
    """Get the tool name for the current context."""
    return _tool_name.get()


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------
class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON (JSONL).

    Output includes:
    - timestamp (ISO 8601)
    - level
    - correlation_id (from contextvars)
    - tool_name (from contextvars)
    - logger name
    - message
    - Any extra fields passed via `extra={}` in the log call
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S")
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add correlation context
        cid = _correlation_id.get()
        if cid:
            log_entry["correlation_id"] = cid
        tn = _tool_name.get()
        if tn:
            log_entry["tool_name"] = tn

        # Add structured extras (set by log calls via extra={})
        for key in (
            "event", "duration_ms", "success", "params",
            "resource_id", "error_type", "error",
        ):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val

        return json.dumps(log_entry, default=str, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Setup function
# ---------------------------------------------------------------------------
def setup_logging(
    log_dir: str = "",
    log_level: str = "INFO",
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
) -> None:
    """Configure structured logging for the MCP server.

    - JSON file handler → ~/.quicksight-mcp/logs/mcp_server.jsonl
    - Human-readable stderr handler preserved for MCP transport
    """
    if not log_dir:
        log_dir = os.path.expanduser("~/.quicksight-mcp/logs")

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = os.path.join(log_dir, "mcp_server.jsonl")

    # Root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # JSON file handler (rotating)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(StructuredJsonFormatter())
    file_handler.setLevel(logging.DEBUG)
    root.addHandler(file_handler)

    # Human-readable stderr (MCP transport needs it)
    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    stderr_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    root.addHandler(stderr_handler)


# ---------------------------------------------------------------------------
# Structured log helpers (used by decorator and services)
# ---------------------------------------------------------------------------
_structured_logger = logging.getLogger("quicksight_mcp.structured")


def log_tool_start(tool_name: str, params: Optional[Dict] = None) -> None:
    """Log the start of a tool call with correlation context."""
    _structured_logger.info(
        f"Tool call started: {tool_name}",
        extra={
            "event": "tool_call_start",
            "params": _sanitize_params(params or {}),
        },
    )


def log_tool_complete(
    tool_name: str,
    duration_ms: float,
    success: bool,
    resource_id: str = "",
    error: str = "",
    error_type: str = "",
) -> None:
    """Log the completion of a tool call."""
    extra: Dict[str, Any] = {
        "event": "tool_call_complete",
        "duration_ms": round(duration_ms, 1),
        "success": success,
    }
    if resource_id:
        extra["resource_id"] = resource_id
    if error:
        extra["error"] = error[:500]
    if error_type:
        extra["error_type"] = error_type

    level = logging.INFO if success else logging.WARNING
    _structured_logger.log(
        level,
        f"Tool call {'completed' if success else 'failed'}: {tool_name} "
        f"({duration_ms:.0f}ms)",
        extra=extra,
    )


def _sanitize_params(params: Dict) -> Dict:
    """Remove large values from params for logging."""
    sanitized = {}
    for k, v in params.items():
        if isinstance(v, str) and len(v) > 200:
            sanitized[k] = v[:100] + f"...({len(v)} chars)"
        else:
            sanitized[k] = v
    return sanitized
