"""Tests for the brain analyzer (self-improvement engine)."""

from quicksight_mcp.brain.analyzer import BrainAnalyzer
from quicksight_mcp.memory.manager import MemoryManager


class TestBrainAnalyzerBasic:
    """Basic brain analyzer tests."""

    def test_analyze_empty_memory(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        assert result["status"] == "ok"
        assert result["insight_count"] == 0

    def test_analyze_disabled_memory(self, tmp_path):
        mgr = MemoryManager(str(tmp_path), enabled=False)
        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        assert result["status"] == "disabled"

    def test_get_insights_caches(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        brain = BrainAnalyzer(mgr)
        # First call via get_insights analyzes fresh and caches
        r1 = brain.get_insights()
        # Verify the cache is populated
        assert brain._cached_insights is not None
        # Second call should return the same cached object
        r2 = brain.get_insights()
        assert r1 is r2  # Same object reference = cache hit


class TestBrainErrorAnalysis:
    """Tests for error pattern analysis."""

    def test_detects_auth_expired_pattern(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # Simulate 5 auth_expired errors
        for i in range(5):
            mgr.record_call(
                "update_dataset_sql",
                {"dataset_id": f"ds-{i}"},
                100.0, False,
                "ExpiredToken: credentials expired",
            )

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        error_insights = [
            i for i in result["insights"] if i["type"] == "error_pattern"
        ]
        assert len(error_insights) >= 1
        auth_insight = next(
            (i for i in error_insights if i["error_type"] == "auth_expired"),
            None,
        )
        assert auth_insight is not None
        assert auth_insight["occurrence_count"] == 5
        assert "saml2aws" in auth_insight["message"]

    def test_ignores_infrequent_errors(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # Only 1 error - should not trigger insight
        mgr.record_call(
            "list_datasets", {}, 100.0, False, "Some random error",
        )

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        error_insights = [
            i for i in result["insights"] if i["type"] == "error_pattern"
        ]
        assert len(error_insights) == 0


class TestBrainWorkflowAnalysis:
    """Tests for workflow optimization analysis."""

    def test_detects_frequent_sequence(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # Simulate repeated search → get_sql pattern
        for _ in range(6):
            mgr.record_call("search_datasets", {"name": "x"}, 50.0, True)
            mgr.record_call("get_dataset_sql", {"dataset_id": "d1"}, 30.0, True)

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        wf_insights = [
            i for i in result["insights"]
            if i["type"] in ("workflow_optimization", "workflow_pattern")
        ]
        assert len(wf_insights) >= 1

    def test_ignores_infrequent_workflows(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call("search_datasets", {}, 50.0, True)
        mgr.record_call("get_dataset_sql", {"dataset_id": "d1"}, 30.0, True)

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        wf_insights = [
            i for i in result["insights"]
            if i["type"] == "workflow_optimization"
        ]
        assert len(wf_insights) == 0


class TestBrainLatencyAnalysis:
    """Tests for latency degradation detection."""

    def test_detects_slowdown(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # First 10 calls: fast (100ms)
        for _ in range(10):
            mgr.record_call("list_analyses", {}, 100.0, True)
        # Next 10 calls: slow (500ms) = 5x degradation
        for _ in range(10):
            mgr.record_call("list_analyses", {}, 500.0, True)

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        lat_insights = [
            i for i in result["insights"]
            if i["type"] == "latency_degradation"
        ]
        assert len(lat_insights) >= 1
        assert lat_insights[0]["slowdown_ratio"] >= 2.0

    def test_no_alert_for_stable_latency(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        for _ in range(20):
            mgr.record_call("list_datasets", {}, 100.0, True)

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        lat_insights = [
            i for i in result["insights"]
            if i["type"] == "latency_degradation"
        ]
        assert len(lat_insights) == 0


class TestBrainResourceAnalysis:
    """Tests for resource issue detection."""

    def test_flags_troubled_resource(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # Same resource, 5 errors
        for _ in range(5):
            mgr.record_call(
                "update_dataset_sql",
                {"dataset_id": "ds-troubled"},
                100.0, False,
                "Update failed: concurrent modification",
            )

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        resource_insights = [
            i for i in result["insights"]
            if i["type"] == "resource_issues"
        ]
        assert len(resource_insights) >= 1
        assert resource_insights[0]["resource_id"] == "ds-troubled"


class TestBrainMaybeAnalyze:
    """Tests for the auto-analyze trigger."""

    def test_auto_triggers_after_interval(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        brain = BrainAnalyzer(mgr, analyze_interval=5)

        for i in range(5):
            brain.maybe_analyze()

        # Should have auto-analyzed
        assert brain._cached_insights is not None
        assert brain._cached_insights["status"] == "ok"

    def test_no_trigger_before_interval(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        brain = BrainAnalyzer(mgr, analyze_interval=10)

        for i in range(3):
            brain.maybe_analyze()

        assert brain._cached_insights is None


class TestBrainLatencyEdgeCases:
    """Edge case tests for latency analysis."""

    def test_fewer_than_10_samples_skipped(self, tmp_path):
        """Tools with < 10 latency samples should not trigger insights."""
        mgr = MemoryManager(str(tmp_path))
        for _ in range(9):
            mgr.record_call("tool_a", {}, 100.0, True)

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        lat_insights = [
            i for i in result["insights"]
            if i["type"] == "latency_degradation"
        ]
        assert len(lat_insights) == 0

    def test_zero_baseline_does_not_divide_by_zero(self, tmp_path):
        """baseline_avg == 0 should not cause ZeroDivisionError."""
        mgr = MemoryManager(str(tmp_path))
        # First 5 calls: 0ms, next 5: 100ms
        for _ in range(5):
            mgr.record_call("tool_a", {}, 0.0, True)
        for _ in range(5):
            mgr.record_call("tool_a", {}, 100.0, True)

        brain = BrainAnalyzer(mgr)
        # Should not raise
        result = brain.analyze()
        assert result["status"] == "ok"


class TestBrainMultipleErrorTypes:
    """Test analysis with multiple different error types."""

    def test_multiple_error_types_all_flagged(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # 5 auth errors
        for _ in range(5):
            mgr.record_call("t", {"dataset_id": "d"}, 10.0, False, "ExpiredToken")
        # 4 rate limit errors
        for _ in range(4):
            mgr.record_call("t", {"dataset_id": "d"}, 10.0, False, "Rate limited")
        # 1 not_found (below threshold)
        mgr.record_call("t", {"dataset_id": "d"}, 10.0, False, "not found 404")

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        error_insights = [
            i for i in result["insights"] if i["type"] == "error_pattern"
        ]
        error_types = {i["error_type"] for i in error_insights}
        assert "auth_expired" in error_types
        assert "rate_limited" in error_types
        assert "not_found" not in error_types  # only 1, below threshold of 3


class TestBrainInsightPriority:
    """Test that insights are sorted by priority."""

    def test_insights_sorted_by_priority(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        # Create multiple types of issues
        for _ in range(10):
            mgr.record_call(
                "update_dataset_sql",
                {"dataset_id": "ds-1"},
                100.0, False,
                "ExpiredToken: creds expired",
            )
        # Also create some successful calls to get workflow data
        for _ in range(6):
            mgr.record_call("search_datasets", {"name": "x"}, 50.0, True)
            mgr.record_call("get_dataset_sql", {"dataset_id": "d1"}, 30.0, True)

        brain = BrainAnalyzer(mgr)
        result = brain.analyze()
        priorities = [i["priority"] for i in result["insights"]]
        assert priorities == sorted(priorities, reverse=True)
