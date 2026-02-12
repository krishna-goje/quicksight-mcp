"""QuickSight MCP Server - Entry point.

This is the main server module that creates the FastMCP instance,
lazily initializes shared dependencies (client, tracker, optimizer),
and registers all tool modules.
"""

import logging
import os

from fastmcp import FastMCP

from quicksight_mcp.client import QuickSightClient
from quicksight_mcp.learning.tracker import UsageTracker
from quicksight_mcp.learning.optimizer import Optimizer

# Import tool registration functions
from quicksight_mcp.tools.datasets import register_dataset_tools
from quicksight_mcp.tools.analyses import register_analysis_tools
from quicksight_mcp.tools.calculated_fields import register_calculated_field_tools
from quicksight_mcp.tools.dashboards import register_dashboard_tools
from quicksight_mcp.tools.backup import register_backup_tools
from quicksight_mcp.tools.learning import register_learning_tools

logger = logging.getLogger(__name__)

# Create the MCP server
mcp = FastMCP("QuickSight MCP")

# ---------------------------------------------------------------------------
# Lazy-initialized globals
# ---------------------------------------------------------------------------
_client: QuickSightClient | None = None
_tracker: UsageTracker | None = None
_optimizer: Optimizer | None = None


def get_client() -> QuickSightClient:
    """Get or create the QuickSight client (lazy init)."""
    global _client
    if _client is None:
        _client = QuickSightClient()
    return _client


def get_tracker() -> UsageTracker:
    """Get or create the usage tracker (lazy init)."""
    global _tracker
    if _tracker is None:
        _tracker = UsageTracker()
    return _tracker


def get_optimizer() -> Optimizer:
    """Get or create the optimizer (lazy init)."""
    global _optimizer
    if _optimizer is None:
        _optimizer = Optimizer(get_tracker())
    return _optimizer


# ---------------------------------------------------------------------------
# Register all tools with the MCP server
# ---------------------------------------------------------------------------
register_dataset_tools(mcp, get_client, get_tracker)
register_analysis_tools(mcp, get_client, get_tracker)
register_calculated_field_tools(mcp, get_client, get_tracker)
register_dashboard_tools(mcp, get_client, get_tracker)
register_backup_tools(mcp, get_client, get_tracker)
register_learning_tools(mcp, get_tracker, get_optimizer)


def main():
    """Entry point for the quicksight-mcp CLI command."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    mcp.run()


if __name__ == "__main__":
    main()
