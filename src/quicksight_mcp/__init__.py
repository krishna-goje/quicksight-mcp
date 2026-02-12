"""QuickSight MCP Server - The most comprehensive AWS QuickSight MCP server."""

__version__ = "0.1.0"

from quicksight_mcp.client import QuickSightClient
from quicksight_mcp.exceptions import (
    ConcurrentModificationError,
    ChangeVerificationError,
    DestructiveChangeError,
)
