"""Structured exception hierarchy for QuickSight MCP operations.

Every exception carries ``error_type``, ``suggestions``, and ``metadata``
so the tool layer can return rich, actionable error responses to MCP clients.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class QSError(Exception):
    """Base exception for all QuickSight MCP errors.

    Attributes:
        error_type: Machine-readable error category.
        resource_id: The QuickSight resource involved (if any).
        suggestions: Actionable recovery steps for the MCP client.
        metadata: Structured context for debugging.
    """

    error_type: str = "unknown"

    def __init__(
        self,
        message: str,
        *,
        resource_id: str = "",
        suggestions: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.resource_id = resource_id
        self.suggestions = suggestions or []
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """Serialise for MCP error responses."""
        return {
            "error_type": self.error_type,
            "error": str(self),
            "resource_id": self.resource_id,
            "suggestions": self.suggestions,
            "metadata": self.metadata,
        }


# -----------------------------------------------------------------------
# Concrete exceptions
# -----------------------------------------------------------------------


class QSAuthError(QSError):
    """Credentials are expired or invalid."""

    error_type = "auth_expired"

    def __init__(self, message: str = "AWS credentials expired", **kwargs: Any):
        super().__init__(
            message,
            suggestions=[
                "Run 'saml2aws login' to refresh credentials",
                "Or set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars",
            ],
            **kwargs,
        )


class QSNotFoundError(QSError):
    """A requested resource (dataset, analysis, visual, etc.) was not found."""

    error_type = "not_found"

    def __init__(
        self, resource_type: str, resource_id: str, **kwargs: Any
    ):
        super().__init__(
            f"{resource_type} '{resource_id}' not found",
            resource_id=resource_id,
            suggestions=[
                f"Verify the {resource_type.lower()} ID is correct",
                f"Use list/search tools to find valid "
                f"{resource_type.lower()} IDs",
            ],
            metadata={"resource_type": resource_type},
            **kwargs,
        )


class QSValidationError(QSError):
    """Input validation failed (bad SQL, missing required field, etc.)."""

    error_type = "validation"

    def __init__(self, message: str, **kwargs: Any):
        super().__init__(message, **kwargs)


class QSApiError(QSError):
    """An AWS API call failed for a reason other than auth or not-found."""

    error_type = "api_error"


class QSRateLimitError(QSError):
    """AWS throttled the request."""

    error_type = "rate_limited"

    def __init__(self, message: str = "Rate limited by AWS", **kwargs: Any):
        super().__init__(
            message,
            suggestions=[
                "Wait a few seconds and retry",
                "Reduce the frequency of API calls",
            ],
            **kwargs,
        )


class ConcurrentModificationError(QSError):
    """Analysis was modified by another session since it was read.

    Implements optimistic locking: the client checks LastUpdatedTime
    before writing.
    """

    error_type = "concurrent_modification"

    def __init__(
        self,
        analysis_id: str,
        expected_time: Any,
        actual_time: Any,
    ):
        super().__init__(
            f"Analysis {analysis_id} was modified by another session. "
            f"Expected LastUpdatedTime: {expected_time}, Actual: {actual_time}. "
            f"Fetch the latest definition and retry.",
            resource_id=analysis_id,
            suggestions=[
                "Fetch the latest definition with describe_analysis",
                "Re-apply your changes on top of the latest version",
                "If this keeps happening, check for concurrent editors",
            ],
            metadata={
                "expected_time": str(expected_time),
                "actual_time": str(actual_time),
            },
        )
        self.analysis_id = analysis_id
        self.expected_time = expected_time
        self.actual_time = actual_time


class ChangeVerificationError(QSError):
    """A change was applied (HTTP 200) but post-write verification failed."""

    error_type = "verification_failed"

    def __init__(self, operation: str, resource_id: str, details: str):
        super().__init__(
            f"Change verification failed for {operation} on {resource_id}: {details}. "
            f"The API call succeeded but the change was not reflected. "
            f"Check the QuickSight console and retry if needed.",
            resource_id=resource_id,
            suggestions=[
                "Retry the operation",
                "Check the QuickSight console for the actual state",
                "If persists, restore from backup",
            ],
            metadata={"operation": operation, "details": details},
        )
        self.operation = operation
        self.details = details


class DestructiveChangeError(QSError):
    """An update would delete major content (sheets, visuals, calc fields)."""

    error_type = "destructive_blocked"

    def __init__(
        self,
        analysis_id: str,
        details: str,
        current_counts: Dict[str, int],
        new_counts: Dict[str, int],
    ):
        super().__init__(
            f"BLOCKED: Update to {analysis_id} would delete major content. {details}\n"
            f"Current: {current_counts}\n"
            f"After update: {new_counts}\n"
            f"If this is intentional, use allow_destructive=True",
            resource_id=analysis_id,
            suggestions=[
                "If this is intentional, use allow_destructive=True",
                "Review the definition changes before retrying",
                "Back up the analysis first with backup_analysis",
            ],
            metadata={
                "current_counts": current_counts,
                "new_counts": new_counts,
            },
        )
        self.analysis_id = analysis_id
        self.details = details
        self.current_counts = current_counts
        self.new_counts = new_counts
