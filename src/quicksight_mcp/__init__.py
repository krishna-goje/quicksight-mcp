"""QuickSight MCP Server - The most comprehensive AWS QuickSight MCP server."""

__version__ = "0.2.0"

from quicksight_mcp.client import QuickSightClient as QuickSightClient
from quicksight_mcp.exceptions import (
    ChangeVerificationError as ChangeVerificationError,
    ConcurrentModificationError as ConcurrentModificationError,
    DestructiveChangeError as DestructiveChangeError,
)
