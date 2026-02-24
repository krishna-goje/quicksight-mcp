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

from quicksight_mcp.safety.destructive_guard import validate_definition_not_destructive

from quicksight_mcp.safety.verification import (
    verify_filter_group_deleted,
    verify_filter_group_exists,
    verify_parameter_deleted,
    verify_parameter_exists,
    verify_sheet_deleted,
    verify_sheet_exists,
    verify_sheet_visual_count,
    verify_visual_deleted,
    verify_visual_exists,
    verify_visual_title,
)

__all__ = [
    # Exceptions
    "QSError",
    "QSAuthError",
    "QSNotFoundError",
    "QSValidationError",
    "QSApiError",
    "QSRateLimitError",
    "ConcurrentModificationError",
    "ChangeVerificationError",
    "DestructiveChangeError",
    # Destructive guard
    "validate_definition_not_destructive",
    # Verification functions
    "verify_sheet_exists",
    "verify_sheet_deleted",
    "verify_visual_exists",
    "verify_visual_deleted",
    "verify_visual_title",
    "verify_parameter_exists",
    "verify_parameter_deleted",
    "verify_filter_group_exists",
    "verify_filter_group_deleted",
    "verify_sheet_visual_count",
]
