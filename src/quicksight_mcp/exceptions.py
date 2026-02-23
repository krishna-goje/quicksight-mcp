"""Backward-compatible re-exports from the new safety.exceptions module.

All existing code that imports from ``quicksight_mcp.exceptions`` continues
to work unchanged.  New code should import from ``quicksight_mcp.safety.exceptions``.
"""

# Re-export the original three exceptions with the same class identity
# so that ``except ConcurrentModificationError`` still catches them.
from quicksight_mcp.safety.exceptions import (  # noqa: F401
    ChangeVerificationError,
    ConcurrentModificationError,
    DestructiveChangeError,
)

__all__ = [
    "ConcurrentModificationError",
    "ChangeVerificationError",
    "DestructiveChangeError",
]
