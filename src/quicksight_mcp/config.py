"""Configuration for QuickSight MCP Server.

Centralises all settings that were previously scattered across environment
variable reads, hardcoded constants, and constructor defaults.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Settings:
    """Server-wide configuration — single source of truth.

    Values come from environment variables with sensible defaults.
    """

    # AWS
    aws_profile: str = field(
        default_factory=lambda: os.environ.get("AWS_PROFILE", "")
    )
    aws_region: str = field(
        default_factory=lambda: os.environ.get("AWS_REGION", "us-east-1")
    )
    aws_account_id: str = field(
        default_factory=lambda: os.environ.get("AWS_ACCOUNT_ID", "")
    )
    saml_role: str = field(
        default_factory=lambda: os.environ.get("QUICKSIGHT_SAML_ROLE", "")
    )

    # Caching
    cache_ttl_seconds: int = 300  # 5 minutes

    # Backups
    backup_dir: str = field(
        default_factory=lambda: os.environ.get(
            "QUICKSIGHT_BACKUP_DIR",
            os.path.expanduser("~/.quicksight-mcp/backups"),
        )
    )

    # Learning / Memory
    learning_dir: str = field(
        default_factory=lambda: os.environ.get(
            "QUICKSIGHT_MCP_LEARNING_DIR",
            os.path.expanduser("~/.quicksight-mcp"),
        )
    )
    learning_enabled: bool = field(
        default_factory=lambda: os.environ.get(
            "QUICKSIGHT_MCP_LEARNING", "true"
        ).lower()
        == "true"
    )

    # Memory
    memory_dir: str = field(
        default_factory=lambda: os.environ.get(
            "QUICKSIGHT_MCP_MEMORY_DIR",
            os.path.expanduser("~/.quicksight-mcp/memory"),
        )
    )
    memory_max_entries: int = 1000
    memory_max_file_bytes: int = 5 * 1024 * 1024  # 5 MB

    # Logging
    log_dir: str = field(
        default_factory=lambda: os.environ.get(
            "QUICKSIGHT_MCP_LOG_DIR",
            os.path.expanduser("~/.quicksight-mcp/logs"),
        )
    )
    log_level: str = field(
        default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO")
    )

    # Brain (self-improvement)
    brain_analyze_interval: int = 50  # analyze every N tool calls
    brain_max_call_log: int = 2000
    brain_max_knowledge: int = 5000

    # Safety
    verify_by_default: bool = True
    optimistic_locking_by_default: bool = True

    # API retry
    max_api_retries: int = 3
    retry_mode: str = "adaptive"

    # Polling
    update_poll_interval_seconds: float = 2.0
    update_timeout_seconds: int = 60

    # Response formatting
    character_limit: int = 25_000

    def ensure_dirs(self) -> None:
        """Create required directories if they don't exist."""
        for d in (self.backup_dir, self.learning_dir, self.memory_dir, self.log_dir):
            Path(d).mkdir(parents=True, exist_ok=True)
