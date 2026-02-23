"""Safety modules: structured exceptions, verification, destructive-change guard."""

from quicksight_mcp.safety.exceptions import (
    ChangeVerificationError,
    ConcurrentModificationError,
    DestructiveChangeError,
    QSApiError,
    QSAuthError,
    QSError,
    QSNotFoundError,
    QSRateLimitError,
    QSValidationError,
)

__all__ = [
    "QSError",
    "QSAuthError",
    "QSNotFoundError",
    "QSValidationError",
    "QSApiError",
    "QSRateLimitError",
    "ConcurrentModificationError",
    "ChangeVerificationError",
    "DestructiveChangeError",
]
