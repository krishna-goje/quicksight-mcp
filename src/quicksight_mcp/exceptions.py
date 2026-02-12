"""Custom exceptions for QuickSight MCP operations.

These exceptions provide structured error information for common QuickSight
failure modes, enabling MCP clients to handle errors programmatically.
"""

from typing import Dict


class ConcurrentModificationError(Exception):
    """Raised when an analysis was modified by another session since it was read.

    This implements optimistic locking: before updating an analysis, the client
    checks that the analysis hasn't been modified since it was last read. If it
    has, this exception is raised to prevent overwriting someone else's changes.

    Attributes:
        analysis_id: The ID of the analysis that was concurrently modified.
        expected_time: The LastUpdatedTime the client expected.
        actual_time: The actual LastUpdatedTime found on the server.
    """

    def __init__(self, analysis_id: str, expected_time, actual_time):
        self.analysis_id = analysis_id
        self.expected_time = expected_time
        self.actual_time = actual_time
        super().__init__(
            f"Analysis {analysis_id} was modified by another session. "
            f"Expected LastUpdatedTime: {expected_time}, Actual: {actual_time}. "
            f"Fetch the latest definition and retry."
        )


class ChangeVerificationError(Exception):
    """Raised when a change was not properly applied to QuickSight.

    After making an API call that succeeds (HTTP 200), the client verifies
    that the change actually took effect. If verification fails, this exception
    is raised. This catches silent failures where the API accepts the request
    but does not apply the change.

    Attributes:
        operation: The operation that was attempted (e.g., 'add_calculated_field').
        resource_id: The ID of the resource being modified.
        details: Human-readable description of the verification failure.
    """

    def __init__(self, operation: str, resource_id: str, details: str):
        self.operation = operation
        self.resource_id = resource_id
        self.details = details
        super().__init__(
            f"Change verification failed for {operation} on {resource_id}: {details}. "
            f"The API call succeeded but the change was not reflected. "
            f"Check the QuickSight console and retry if needed."
        )


class DestructiveChangeError(Exception):
    """Raised when an update would delete major content (sheets, visuals, etc.).

    Before applying an analysis update, the client compares the current and new
    definitions. If the new definition would remove all sheets, more than 50%
    of visuals, or more than 50% of calculated fields, this exception is raised
    to prevent accidental data loss.

    Attributes:
        analysis_id: The ID of the analysis being updated.
        details: Human-readable description of what would be deleted.
        current_counts: Dict with current counts of sheets, visuals, calculated_fields.
        new_counts: Dict with counts after the proposed update.
    """

    def __init__(self, analysis_id: str, details: str, current_counts: Dict, new_counts: Dict):
        self.analysis_id = analysis_id
        self.details = details
        self.current_counts = current_counts
        self.new_counts = new_counts
        super().__init__(
            f"BLOCKED: Update to {analysis_id} would delete major content. {details}\n"
            f"Current: {current_counts}\n"
            f"After update: {new_counts}\n"
            f"If this is intentional, use allow_destructive=True"
        )
