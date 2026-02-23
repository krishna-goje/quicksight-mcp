"""QuickSight MCP Server v1.1 — The most comprehensive AWS QuickSight MCP server.

Architecture:
    core/       — AwsClient, TTLCache, shared types
    services/   — 11 focused service modules (datasets, analyses, dashboards, etc.)
    safety/     — Structured exceptions, verification, destructive-change guard
    memory/     — Full context memory + brain (usage, analysis, error, knowledge graph)
    brain/      — Self-improvement engine (BrainAnalyzer: patterns, latency, workflows)
    tools/      — MCP tool definitions with @qs_tool decorator and Pydantic validation
    config.py   — Settings dataclass (single source of truth for configuration)
    logging_config.py — Structured JSON logging with correlation IDs
"""

__version__ = "1.1.0"

__all__ = [
    "__version__",
    "QuickSightClient",
    "ChangeVerificationError",
    "ConcurrentModificationError",
    "DestructiveChangeError",
    "Settings",
    "QSError",
]

# Backward-compatible re-exports
from quicksight_mcp.client import QuickSightClient as QuickSightClient
from quicksight_mcp.exceptions import (
    ChangeVerificationError as ChangeVerificationError,
    ConcurrentModificationError as ConcurrentModificationError,
    DestructiveChangeError as DestructiveChangeError,
)

# New v1.0 exports
from quicksight_mcp.config import Settings as Settings
from quicksight_mcp.safety.exceptions import QSError as QSError
