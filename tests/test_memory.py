"""Unit tests for Phase 4 memory system."""

import json
import time


from quicksight_mcp.memory.store import MemoryStore
from quicksight_mcp.memory.manager import (
    AnalysisMemory,
    ErrorMemory,
    MemoryManager,
    PreferenceMemory,
    UsageTracker,
)


class TestMemoryStore:
    """Tests for the JSON-backed memory store."""

    def test_set_and_get(self, tmp_path):
        store = MemoryStore(str(tmp_path / "test.json"))
        store.set("key1", {"data": [1, 2, 3]})
        assert store.get("key1") == {"data": [1, 2, 3]}

    def test_get_missing(self, tmp_path):
        store = MemoryStore(str(tmp_path / "test.json"))
        assert store.get("missing") is None
        assert store.get("missing", "default") == "default"

    def test_delete(self, tmp_path):
        store = MemoryStore(str(tmp_path / "test.json"))
        store.set("key1", "value")
        store.delete("key1")
        assert store.get("key1") is None

    def test_persistence(self, tmp_path):
        path = str(tmp_path / "persist.json")
        store1 = MemoryStore(path)
        store1.set("key1", "value1")
        store1.flush()

        store2 = MemoryStore(path)
        assert store2.get("key1") == "value1"

    def test_eviction(self, tmp_path):
        store = MemoryStore(str(tmp_path / "evict.json"), max_entries=5)
        for i in range(6):
            store.set(f"key{i}", f"value{i}")
            time.sleep(0.01)  # Ensure distinct timestamps
        # Should have evicted oldest entries
        assert store.size <= 5

    def test_atomic_write(self, tmp_path):
        path = str(tmp_path / "atomic.json")
        store = MemoryStore(path)
        store.set("key1", "value1")
        store.flush()

        # File should exist and be valid JSON
        with open(path) as f:
            data = json.load(f)
        assert "key1" in data

    def test_keys_values_items(self, tmp_path):
        store = MemoryStore(str(tmp_path / "test.json"))
        store.set("a", 1)
        store.set("b", 2)
        assert set(store.keys()) == {"a", "b"}
        assert set(store.values()) == {1, 2}
        items = dict(store.items())
        assert items["a"] == 1 and items["b"] == 2


class TestUsageTracker:
    """Tests for the usage tracker."""

    def test_record_call_updates_counts(self, tmp_path):
        store = MemoryStore(str(tmp_path / "usage.json"))
        tracker = UsageTracker(store)
        tracker.record_call("list_datasets", {}, 100.0, True)
        tracker.record_call("list_datasets", {}, 120.0, True)

        counts = store.get("tool_counts")
        assert counts["list_datasets"] == 2

    def test_get_insights(self, tmp_path):
        store = MemoryStore(str(tmp_path / "usage.json"))
        tracker = UsageTracker(store)
        tracker.record_call("search_datasets", {"name": "wbr"}, 50.0, True)
        tracker.record_call("get_dataset_sql", {"id": "d1"}, 30.0, True)

        insights = tracker.get_insights()
        assert insights["total_calls"] == 2
        assert len(insights["most_used_tools"]) == 2
        assert len(insights["slowest_tools"]) > 0

    def test_sequence_detection(self, tmp_path):
        store = MemoryStore(str(tmp_path / "usage.json"))
        tracker = UsageTracker(store)
        tracker.record_call("search_datasets", {}, 50.0, True)
        tracker.record_call("get_dataset_sql", {}, 30.0, True)

        sequences = store.get("sequences")
        assert "search_datasets -> get_dataset_sql" in sequences


class TestAnalysisMemory:
    """Tests for the analysis structure memory."""

    def test_remember_and_get(self, tmp_path):
        store = MemoryStore(str(tmp_path / "analyses.json"))
        mem = AnalysisMemory(store)
        mem.remember_structure(
            "a-123",
            sheets=[{"id": "s1", "name": "Summary"}],
            calc_fields=["Revenue", "Cost"],
        )
        structure = mem.get_structure("a-123")
        assert structure is not None
        assert len(structure["sheets"]) == 1
        assert "Revenue" in structure["calc_fields"]

    def test_forget(self, tmp_path):
        store = MemoryStore(str(tmp_path / "analyses.json"))
        mem = AnalysisMemory(store)
        mem.remember_structure("a-123", sheets=[], calc_fields=[])
        mem.forget("a-123")
        assert mem.get_structure("a-123") is None


class TestErrorMemory:
    """Tests for the error pattern memory."""

    def test_record_and_get_patterns(self, tmp_path):
        store = MemoryStore(str(tmp_path / "errors.json"))
        mem = ErrorMemory(store)
        mem.record_error("a-123", "auth_expired", "Token expired")
        mem.record_error("a-123", "auth_expired", "Token expired again")

        patterns = mem.get_patterns()
        assert patterns["total_errors"] == 2

    def test_recovery_suggestions(self, tmp_path):
        store = MemoryStore(str(tmp_path / "errors.json"))
        mem = ErrorMemory(store)
        mem.record_error(
            "a-123", "update_failed", "Update failed",
            recovery_used="restore from backup",
            recovery_worked=True,
        )

        suggestions = mem.get_recovery_suggestions("a-123", "update_failed")
        assert len(suggestions) > 0
        assert "restore from backup" in suggestions[0]


class TestPreferenceMemory:
    """Tests for user preference storage."""

    def test_set_and_get(self, tmp_path):
        store = MemoryStore(str(tmp_path / "prefs.json"))
        mem = PreferenceMemory(store)
        mem.set_preference("backup_first", True)
        assert mem.get_preference("backup_first") is True

    def test_get_default(self, tmp_path):
        store = MemoryStore(str(tmp_path / "prefs.json"))
        mem = PreferenceMemory(store)
        assert mem.get_preference("missing", "default") == "default"


class TestMemoryManager:
    """Tests for the full memory system."""

    def test_init_creates_stores(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        assert mgr.enabled
        assert mgr.usage is not None
        assert mgr.analyses is not None
        assert mgr.errors is not None
        assert mgr.preferences is not None

    def test_record_call_delegates(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call("list_datasets", {}, 100.0, True)
        insights = mgr.usage.get_insights()
        assert insights["total_calls"] == 1

    def test_record_error_on_failure(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call(
            "update_dataset_sql",
            {"dataset_id": "ds-123"},
            200.0,
            False,
            "ExpiredToken: credentials expired",
        )
        patterns = mgr.errors.get_patterns()
        assert patterns["total_errors"] == 1

    def test_flush_persists(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call("list_datasets", {}, 50.0, True)
        mgr.flush()

        # Reload and verify
        mgr2 = MemoryManager(str(tmp_path))
        insights = mgr2.usage.get_insights()
        assert insights["total_calls"] == 1

    def test_disabled_is_noop(self, tmp_path):
        mgr = MemoryManager(str(tmp_path), enabled=False)
        mgr.record_call("list_datasets", {}, 50.0, True)
        assert mgr.get_recovery_suggestions("", "") == []

    def test_classify_error(self):
        assert MemoryManager._classify_error("ExpiredToken") == "auth_expired"
        assert MemoryManager._classify_error("Resource not found") == "not_found"
        assert MemoryManager._classify_error("Rate limited") == "rate_limited"
        assert MemoryManager._classify_error("Something weird") == "unknown"
