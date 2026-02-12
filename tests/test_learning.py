"""Test self-learning engine."""

import pytest
import tempfile
import os

from quicksight_mcp.learning.tracker import UsageTracker
from quicksight_mcp.learning.optimizer import Optimizer
from quicksight_mcp.learning.knowledge import KnowledgeStore


class TestUsageTracker:
    """Test usage tracking."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tracker = UsageTracker(storage_dir=self.tmpdir)

    def test_record_call_increments_count(self):
        """Test that recording calls increments tool counts."""
        self.tracker.record_call("list_datasets", {}, 100.0, True)
        self.tracker.record_call("list_datasets", {}, 150.0, True)

        insights = self.tracker.get_insights()
        assert insights["total_calls"] == 2

    def test_record_call_tracks_different_tools(self):
        """Test that different tools are tracked separately."""
        self.tracker.record_call("list_datasets", {}, 100.0, True)
        self.tracker.record_call("search_datasets", {"name": "WBR"}, 80.0, True)
        self.tracker.record_call("get_dataset_sql", {"id": "123"}, 120.0, True)

        insights = self.tracker.get_insights()
        assert insights["total_calls"] == 3
        tool_names = [t["tool"] for t in insights["most_used_tools"]]
        assert "list_datasets" in tool_names
        assert "search_datasets" in tool_names

    def test_record_call_tracks_sequences(self):
        """Test that tool call sequences are tracked."""
        self.tracker.record_call("search_datasets", {"name": "WBR"}, 50.0, True)
        self.tracker.record_call("get_dataset_sql", {"id": "123"}, 80.0, True)

        insights = self.tracker.get_insights()
        workflows = [w["sequence"] for w in insights.get("common_workflows", [])]
        assert any("search_datasets -> get_dataset_sql" in w for w in workflows)

    def test_error_recording(self):
        """Test that errors are recorded and classified."""
        self.tracker.record_call(
            "update_dataset_sql", {}, 200.0, False, "Token expired"
        )

        errors = self.tracker.get_error_patterns()
        assert errors["total_errors"] == 1

    def test_multiple_errors_accumulate(self):
        """Test that multiple errors of the same type accumulate."""
        for _ in range(5):
            self.tracker.record_call(
                "list_datasets", {}, 100.0, False, "Token expired"
            )

        errors = self.tracker.get_error_patterns()
        assert errors["total_errors"] == 5

    def test_error_classification_auth(self):
        """Test error classification for auth errors."""
        self.tracker.record_call("test", {}, 100.0, False, "Token expired")
        self.tracker.record_call("test", {}, 100.0, False, "credential not valid")

        patterns = self.tracker.get_error_patterns()["patterns"]
        assert any("auth_expired" in key for key in patterns)

    def test_error_classification_not_found(self):
        """Test error classification for not-found errors."""
        self.tracker.record_call("test", {}, 100.0, False, "Resource not found")

        patterns = self.tracker.get_error_patterns()["patterns"]
        assert any("not_found" in key for key in patterns)

    def test_error_classification_rate_limit(self):
        """Test error classification for rate-limit errors."""
        self.tracker.record_call("test", {}, 100.0, False, "Rate limit exceeded")

        patterns = self.tracker.get_error_patterns()["patterns"]
        assert any("rate_limited" in key for key in patterns)

    def test_error_classification_sql_syntax(self):
        """Test error classification for SQL syntax errors."""
        self.tracker.record_call(
            "test", {}, 100.0, False, "SQL syntax error near 'rows'"
        )

        patterns = self.tracker.get_error_patterns()["patterns"]
        assert any("sql_syntax" in key for key in patterns)

    def test_disabled_tracker_does_nothing(self):
        """Test that disabled tracker doesn't record."""
        old_val = os.environ.get("QUICKSIGHT_MCP_LEARNING")
        os.environ["QUICKSIGHT_MCP_LEARNING"] = "false"
        try:
            tracker = UsageTracker(storage_dir=self.tmpdir)
            tracker.record_call("test", {}, 100.0, True)

            assert tracker.get_insights()["total_calls"] == 0
        finally:
            if old_val is not None:
                os.environ["QUICKSIGHT_MCP_LEARNING"] = old_val
            else:
                os.environ.pop("QUICKSIGHT_MCP_LEARNING", None)

    def test_persist_and_reload(self):
        """Test that data persists across tracker instances."""
        self.tracker.record_call("list_datasets", {}, 100.0, True)
        self.tracker.flush()

        # Create new tracker from same directory
        tracker2 = UsageTracker(storage_dir=self.tmpdir)
        insights = tracker2.get_insights()
        assert insights["total_calls"] >= 1

    def test_average_duration_tracking(self):
        """Test that average durations are tracked correctly."""
        self.tracker.record_call("list_datasets", {}, 100.0, True)
        self.tracker.record_call("list_datasets", {}, 200.0, True)

        # Stored internally in patterns
        avg_data = self.tracker._patterns["avg_durations"]["list_datasets"]
        assert avg_data["avg"] == 150.0
        assert avg_data["count"] == 2

    def test_sequence_buffer_limited_to_5(self):
        """Test that the sequence buffer doesn't grow beyond 5."""
        for i in range(10):
            self.tracker.record_call(f"tool_{i}", {}, 50.0, True)

        assert len(self.tracker._sequence_buffer) == 5


class TestOptimizer:
    """Test optimization suggestions."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.tracker = UsageTracker(storage_dir=self.tmpdir)
        self.optimizer = Optimizer(self.tracker)

    def test_empty_recommendations(self):
        """Test optimizer with no data returns empty list."""
        recs = self.optimizer.get_recommendations()
        assert isinstance(recs, list)
        assert len(recs) == 0

    def test_auth_error_recommendation(self):
        """Test optimizer detects auth errors and recommends fix."""
        for _ in range(5):
            self.tracker.record_call(
                "list_datasets", {}, 100.0, False, "Token expired"
            )

        recs = self.optimizer.get_recommendations()
        auth_recs = [r for r in recs if r["type"] == "auth"]
        assert len(auth_recs) > 0
        assert auth_recs[0]["priority"] == "high"

    def test_rate_limit_recommendation(self):
        """Test optimizer detects rate limiting."""
        for _ in range(3):
            self.tracker.record_call(
                "list_datasets", {}, 100.0, False, "Rate limit exceeded, throttled"
            )

        recs = self.optimizer.get_recommendations()
        rate_recs = [r for r in recs if r["type"] == "rate_limit"]
        assert len(rate_recs) > 0
        assert rate_recs[0]["priority"] == "medium"

    def test_sql_hint_recommendation(self):
        """Test optimizer detects SQL syntax errors."""
        self.tracker.record_call(
            "update_dataset_sql",
            {},
            100.0,
            False,
            "SQL syntax error: reserved keyword",
        )

        recs = self.optimizer.get_recommendations()
        sql_recs = [r for r in recs if r["type"] == "sql_hint"]
        assert len(sql_recs) > 0

    def test_recommendations_sorted_by_priority(self):
        """Test that recommendations are sorted by priority (high first)."""
        # Create high priority error
        for _ in range(5):
            self.tracker.record_call("t", {}, 100.0, False, "Token expired")
        # Create medium priority error
        for _ in range(3):
            self.tracker.record_call("t", {}, 100.0, False, "Rate limit hit")

        recs = self.optimizer.get_recommendations()
        if len(recs) >= 2:
            priorities = [r["priority"] for r in recs]
            # high should come before medium
            if "high" in priorities and "medium" in priorities:
                assert priorities.index("high") < priorities.index("medium")


class TestKnowledgeStore:
    """Test knowledge storage."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.store = KnowledgeStore(storage_dir=self.tmpdir)

    def test_get_set(self):
        """Test basic get/set operations."""
        self.store.set("test_key", "test_value")
        assert self.store.get("test_key") == "test_value"

    def test_get_default(self):
        """Test get with default value for missing key."""
        assert self.store.get("nonexistent", "default") == "default"

    def test_get_none_default(self):
        """Test get returns None by default for missing key."""
        assert self.store.get("nonexistent") is None

    def test_overwrite_value(self):
        """Test that setting a key twice overwrites the first value."""
        self.store.set("key", "value1")
        self.store.set("key", "value2")
        assert self.store.get("key") == "value2"

    def test_different_value_types(self):
        """Test storing different value types."""
        self.store.set("string", "hello")
        self.store.set("number", 42)
        self.store.set("list", [1, 2, 3])
        self.store.set("dict", {"a": 1})

        assert self.store.get("string") == "hello"
        assert self.store.get("number") == 42
        assert self.store.get("list") == [1, 2, 3]
        assert self.store.get("dict") == {"a": 1}

    def test_cache_hints(self):
        """Test cache hint operations."""
        self.store.update_cache_hint("dataset", "ds-001", 25)
        hints = self.store.get_cache_hints()
        assert "dataset:ds-001" in hints
        assert hints["dataset:ds-001"]["access_count"] == 25

    def test_cache_hints_multiple(self):
        """Test multiple cache hints."""
        self.store.update_cache_hint("dataset", "ds-001", 25)
        self.store.update_cache_hint("dataset", "ds-002", 10)
        self.store.update_cache_hint("analysis", "an-001", 5)

        hints = self.store.get_cache_hints()
        assert len(hints) == 3
        assert hints["dataset:ds-001"]["access_count"] == 25
        assert hints["analysis:an-001"]["resource_type"] == "analysis"

    def test_cache_hint_update_overwrites(self):
        """Test that updating a cache hint overwrites the previous value."""
        self.store.update_cache_hint("dataset", "ds-001", 10)
        self.store.update_cache_hint("dataset", "ds-001", 50)

        hints = self.store.get_cache_hints()
        assert hints["dataset:ds-001"]["access_count"] == 50

    def test_persistence_across_instances(self):
        """Test that data persists across KnowledgeStore instances."""
        self.store.set("persistent_key", "persistent_value")

        store2 = KnowledgeStore(storage_dir=self.tmpdir)
        assert store2.get("persistent_key") == "persistent_value"
