"""QuickSight MCP Server - Entry point.

Creates the FastMCP instance, lazily initializes shared dependencies
(ServiceContainer, MemoryManager), and registers all tool modules.

v1.1: Wired to use service layer + memory system instead of monolithic client.
"""

import logging

from fastmcp import FastMCP

from quicksight_mcp.config import Settings
from quicksight_mcp.services import ServiceContainer
from quicksight_mcp.memory.manager import MemoryManager

# Import tool registration functions
from quicksight_mcp.tools.datasets import register_dataset_tools
from quicksight_mcp.tools.analyses import register_analysis_tools
from quicksight_mcp.tools.calculated_fields import register_calculated_field_tools
from quicksight_mcp.tools.dashboards import register_dashboard_tools
from quicksight_mcp.tools.backup import register_backup_tools
from quicksight_mcp.tools.learning import register_learning_tools
from quicksight_mcp.tools.sheets import register_sheet_tools
from quicksight_mcp.tools.visuals import register_visual_tools
from quicksight_mcp.tools.parameters import register_parameter_tools
from quicksight_mcp.tools.filters import register_filter_tools

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared configuration
# ---------------------------------------------------------------------------
_settings = Settings()

# ---------------------------------------------------------------------------
# Structured logging (must be set up before any tool calls)
# ---------------------------------------------------------------------------
from quicksight_mcp.logging_config import setup_logging  # noqa: E402

setup_logging(
    log_dir=_settings.log_dir,
    log_level=_settings.log_level,
)
logger.info("QuickSight MCP v1.1 structured logging initialized")

# Create the MCP server
mcp = FastMCP("QuickSight MCP")

# ---------------------------------------------------------------------------
# Lazy-initialized globals
# ---------------------------------------------------------------------------
_services: ServiceContainer | None = None
_memory: MemoryManager | None = None

# Backward-compat: old client + tracker for tool files not yet migrated
_client = None
_tracker = None


def get_services() -> ServiceContainer:
    """Get or create the ServiceContainer (lazy init)."""
    global _services
    if _services is None:
        _services = ServiceContainer(_settings)
    return _services


def get_memory() -> MemoryManager:
    """Get or create the MemoryManager (lazy init)."""
    global _memory
    if _memory is None:
        _memory = MemoryManager(
            storage_dir=_settings.memory_dir,
            enabled=_settings.learning_enabled,
            max_entries=_settings.memory_max_entries,
            max_file_bytes=_settings.memory_max_file_bytes,
        )
    return _memory


def get_client():
    """Backward-compat: get a QuickSightClient for tool files not yet migrated."""
    global _client
    if _client is None:
        from quicksight_mcp.client import QuickSightClient
        _client = QuickSightClient()
    return _client


def get_tracker():
    """Backward-compat: proxy to MemoryManager.usage for tool files not yet migrated."""
    global _tracker
    if _tracker is None:
        from quicksight_mcp.learning.tracker import UsageTracker
        _tracker = UsageTracker()
    return _tracker


def get_optimizer():
    """Backward-compat: get an Optimizer for the learning tools."""
    from quicksight_mcp.learning.optimizer import Optimizer
    return Optimizer(get_tracker())


# ---------------------------------------------------------------------------
# Register all tools with the MCP server
# ---------------------------------------------------------------------------
register_dataset_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_analysis_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_calculated_field_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_dashboard_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_backup_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_learning_tools(mcp, get_tracker, get_optimizer, get_memory=get_memory)
register_sheet_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_visual_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_parameter_tools(mcp, get_client, get_tracker, get_memory=get_memory)
register_filter_tools(mcp, get_client, get_tracker, get_memory=get_memory)


def main():
    """Entry point for the quicksight-mcp CLI command."""
    mcp.run()


if __name__ == "__main__":
    main()
