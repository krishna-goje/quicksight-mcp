"""Self-learning MCP tools for QuickSight.

Provides tools that expose insights the server has accumulated from
tracking usage patterns, errors, and performance over time. These are
a key differentiator: the MCP server gets smarter the more you use it.
"""

import time
import logging
from typing import Callable

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_learning_tools(
    mcp: FastMCP, get_tracker: Callable, get_optimizer: Callable
):
    """Register learning and insights MCP tools.

    Note: This function receives get_tracker and get_optimizer (not
    get_client) because learning tools operate on usage data, not
    QuickSight resources directly.
    """

    @mcp.tool
    def get_learning_insights() -> dict:
        """Show what the server has learned from your QuickSight usage patterns.

        This server tracks every tool call -- what you use most, what
        fails, what takes longest -- and surfaces actionable insights.

        Returns:
        - most_used_tools: Tools you call most frequently
        - slowest_tools: Tools with highest average latency
        - error_rate: Per-tool failure percentages
        - recommendations: Suggestions based on your usage patterns
          (e.g., "You search datasets often -- consider using list_datasets
          with caching instead")

        The more you use the server, the better the insights become.
        """
        start = time.time()
        tracker = get_tracker()
        optimizer = get_optimizer()
        try:
            insights = tracker.get_insights()
            recommendations = optimizer.get_recommendations()
            tracker.record_call(
                "get_learning_insights",
                {},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "insights": insights,
                "recommendations": [
                    {
                        "type": r.get("type"),
                        "message": r.get("message"),
                        "priority": r.get("priority"),
                    }
                    for r in recommendations
                ],
                "note": (
                    "These insights are generated from your actual usage. "
                    "The more you use the server, the better they get."
                ),
            }
        except Exception as e:
            tracker.record_call(
                "get_learning_insights",
                {},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def get_error_patterns() -> dict:
        """Show common QuickSight errors and their known fixes.

        Analyzes your error history to identify recurring failure patterns
        and provides specific remediation steps. This is especially useful
        for diagnosing SPICE refresh failures, permission issues, and
        API throttling.

        Returns:
        - patterns: Grouped error types with frequency and last occurrence
        - known_fixes: Documented fixes for each error pattern
        - recent_errors: The most recent errors with context

        Call this when something goes wrong to see if it is a known issue
        with a known fix.
        """
        start = time.time()
        tracker = get_tracker()
        try:
            patterns = tracker.get_error_patterns()
            tracker.record_call(
                "get_error_patterns",
                {},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "error_patterns": patterns,
                "note": (
                    "Error patterns are derived from your actual usage history. "
                    "Recurring errors may indicate a systemic issue worth "
                    "investigating in the QuickSight console."
                ),
            }
        except Exception as e:
            tracker.record_call(
                "get_error_patterns",
                {},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}
