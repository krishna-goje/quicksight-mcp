"""BrainAnalyzer — the self-improvement engine.

Reviews accumulated tool calls, errors, and latency data to generate
actionable insights. Designed to make the MCP server smarter over time.

Triggers:
- Every N tool calls (via ``maybe_analyze()`` called from the decorator)
- Manual via ``get_learning_insights`` MCP tool
- On server startup (analyze overnight accumulation)

What it analyzes:
1. Error patterns: recurring failures + recovery scoring
2. Workflow optimization: repeated sequences that could be simplified
3. Latency degradation: tools getting slower over time
4. Resource context: frequently-failing resources with known fixes
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from quicksight_mcp.memory.manager import MemoryManager

logger = logging.getLogger(__name__)

# Common workflow optimizations the brain can suggest
WORKFLOW_SUGGESTIONS = {
    "search_datasets -> get_dataset_sql": (
        "You frequently search then get SQL. Consider using "
        "get_dataset_sql directly if you know the dataset ID."
    ),
    "search_datasets -> get_dataset_sql -> update_dataset_sql": (
        "Common pattern: search → get SQL → update SQL. "
        "Try modify_dataset_sql for find-and-replace operations."
    ),
    "describe_analysis -> list_calculated_fields": (
        "describe_analysis already includes calculated field counts. "
        "Use list_calculated_fields only when you need expressions."
    ),
    "backup_analysis -> update_calculated_field": (
        "Good practice: backing up before modifying calc fields. "
        "The @qs_tool decorator auto-backs up for write operations."
    ),
    "snapshot_analysis -> diff_analysis": (
        "Snapshot/diff is the proper QA workflow. Well done."
    ),
}


class BrainAnalyzer:
    """Analyzes accumulated memory data to generate insights.

    Args:
        memory: The MemoryManager instance to analyze.
        analyze_interval: Run auto-analysis every N tool calls.
    """

    def __init__(
        self,
        memory: MemoryManager,
        analyze_interval: int = 50,
    ):
        self._memory = memory
        self._interval = analyze_interval
        self._calls_since_analysis = 0
        self._last_analysis_time: float = 0
        self._cached_insights: Optional[Dict[str, Any]] = None

    def maybe_analyze(self) -> None:
        """Called after every tool call. Triggers analysis every N calls."""
        self._calls_since_analysis += 1
        if self._calls_since_analysis >= self._interval:
            self._calls_since_analysis = 0
            try:
                self._cached_insights = self.analyze()
            except Exception as e:
                logger.warning("Brain analysis failed: %s", e)

    def analyze(self) -> Dict[str, Any]:
        """Run full analysis and return structured insights."""
        if not self._memory.enabled:
            return {"status": "disabled", "insights": []}

        self._last_analysis_time = time.time()

        insights: List[Dict[str, Any]] = []

        # 1. Error pattern analysis
        insights.extend(self._analyze_errors())

        # 2. Workflow optimization
        insights.extend(self._analyze_workflows())

        # 3. Latency degradation
        insights.extend(self._analyze_latency())

        # 4. Resource context (most-troubled resources)
        insights.extend(self._analyze_resources())

        # Sort by priority (high first)
        insights.sort(key=lambda x: -x.get("priority", 0))

        return {
            "status": "ok",
            "analyzed_at": self._last_analysis_time,
            "insight_count": len(insights),
            "insights": insights,
            "usage_summary": self._memory.usage.get_insights(),
        }

    def get_insights(self) -> Dict[str, Any]:
        """Get cached or fresh insights."""
        if self._cached_insights is not None and (
            time.time() - self._last_analysis_time < 300  # 5 min cache
        ):
            return self._cached_insights
        result = self.analyze()
        self._cached_insights = result
        return self._cached_insights

    # -----------------------------------------------------------------
    # Analysis methods
    # -----------------------------------------------------------------

    def _analyze_errors(self) -> List[Dict[str, Any]]:
        """Analyze error patterns and recovery effectiveness."""
        insights = []
        patterns = self._memory.errors.get_patterns()
        error_entries = patterns.get("patterns", {})

        # Group by error type
        by_type: Dict[str, int] = {}
        for key, entry in error_entries.items():
            parts = key.split(":")
            if len(parts) >= 3:
                error_type = parts[-1]
                by_type[error_type] = by_type.get(error_type, 0) + entry.get("count", 0)

        # Flag high-frequency error types
        for error_type, count in by_type.items():
            if count >= 3:
                insight = {
                    "type": "error_pattern",
                    "priority": min(count, 10),
                    "error_type": error_type,
                    "occurrence_count": count,
                    "message": (
                        f"'{error_type}' errors occurred {count} times. "
                    ),
                }

                # Add recovery suggestion based on type
                if error_type == "auth_expired":
                    insight["message"] += (
                        "Refresh credentials before batch operations "
                        "(saml2aws login)."
                    )
                    insight["recovery"] = "saml2aws login"
                elif error_type == "rate_limited":
                    insight["message"] += (
                        "Add delays between rapid API calls, "
                        "or batch operations."
                    )
                elif error_type == "concurrent_modification":
                    insight["message"] += (
                        "Multiple editors may be active. "
                        "Coordinate before making changes."
                    )
                elif error_type == "not_found":
                    insight["message"] += (
                        "Resources may have been deleted or IDs may be stale. "
                        "Re-run list/search to get current IDs."
                    )

                insights.append(insight)

        return insights

    def _analyze_workflows(self) -> List[Dict[str, Any]]:
        """Detect repeated workflows and suggest optimizations."""
        insights = []
        usage_insights = self._memory.usage.get_insights()

        for wf in usage_insights.get("common_workflows", []):
            seq = wf["sequence"]
            count = wf["count"]

            if count < 3:
                continue

            # Check if we have a known optimization
            suggestion = WORKFLOW_SUGGESTIONS.get(seq)
            if suggestion:
                insights.append({
                    "type": "workflow_optimization",
                    "priority": min(count // 2, 8),
                    "sequence": seq,
                    "occurrence_count": count,
                    "message": suggestion,
                })
            elif count >= 5:
                # Generic suggestion for any frequent sequence
                insights.append({
                    "type": "workflow_pattern",
                    "priority": 3,
                    "sequence": seq,
                    "occurrence_count": count,
                    "message": (
                        f"Frequent workflow ({count}x): {seq}. "
                        f"Consider a compound operation or script."
                    ),
                })

        return insights

    def _analyze_latency(self) -> List[Dict[str, Any]]:
        """Detect latency degradation by comparing recent vs historical."""
        insights = []

        for tool_name in self._memory.latency.get_all_tools():
            samples = self._memory.latency.get_samples(tool_name)
            if len(samples) < 10:
                continue

            # Split into first half (baseline) and second half (recent)
            mid = len(samples) // 2
            baseline = [s["ms"] for s in samples[:mid]]
            recent = [s["ms"] for s in samples[mid:]]

            baseline_avg = sum(baseline) / len(baseline)
            recent_avg = sum(recent) / len(recent)

            if baseline_avg == 0:
                continue

            ratio = recent_avg / baseline_avg
            if ratio > 2.0:
                insights.append({
                    "type": "latency_degradation",
                    "priority": 7 if ratio > 3.0 else 5,
                    "tool_name": tool_name,
                    "baseline_avg_ms": round(baseline_avg, 1),
                    "recent_avg_ms": round(recent_avg, 1),
                    "slowdown_ratio": round(ratio, 1),
                    "message": (
                        f"{tool_name} is {ratio:.1f}x slower than baseline "
                        f"({baseline_avg:.0f}ms → {recent_avg:.0f}ms). "
                        f"Check for API throttling or large result sets."
                    ),
                })

        return insights

    def _analyze_resources(self) -> List[Dict[str, Any]]:
        """Identify frequently-failing resources."""
        insights = []

        # Look for resources with multiple error types
        for entity_type in ("dataset", "analysis", "dashboard"):
            entities = self._memory.knowledge.get_entities_by_type(entity_type)
            for entity in entities:
                if not entity.get("last_error_type"):
                    continue

                # Find all error relationships for this resource
                rels = self._memory.knowledge.find_relationships(
                    "error_on", target=entity["id"],
                )
                total_errors = sum(r.get("count", 0) for r in rels)

                if total_errors >= 3:
                    insights.append({
                        "type": "resource_issues",
                        "priority": min(total_errors, 8),
                        "resource_type": entity_type,
                        "resource_id": entity["id"],
                        "total_errors": total_errors,
                        "last_error": entity.get("last_error", ""),
                        "message": (
                            f"{entity_type.title()} '{entity['id']}' has "
                            f"{total_errors} errors. Last: "
                            f"{entity.get('last_error_type', 'unknown')}. "
                            f"Consider investigating in the QuickSight console."
                        ),
                    })

        return insights
