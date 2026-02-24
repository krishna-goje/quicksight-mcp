"""Unit tests for memory system (original v1.0 + brain v1.1)."""

import json
import time


from quicksight_mcp.memory.store import MemoryStore
from quicksight_mcp.memory.manager import (
    AnalysisMemory,
    ErrorMemory,
    KnowledgeGraph,
    LatencyTracker,
    MemoryManager,
    PreferenceMemory,
    ToolCallLog,
    UsageTracker,
)


# =========================================================================
# Original tests (v1.0)
# =========================================================================


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


# =========================================================================
# Brain v1.1 tests
# =========================================================================


class TestToolCallLog:
    """Tests for the append-only call log."""

    def test_append_and_get_recent(self, tmp_path):
        store = MemoryStore(str(tmp_path / "log.json"))
        log = ToolCallLog(store)
        log.append("list_datasets", {}, 100.0, True)
        log.append("search_datasets", {"name": "wbr"}, 50.0, True)

        recent = log.get_recent(10)
        assert len(recent) == 2
        assert recent[0]["tool"] == "list_datasets"
        assert recent[1]["tool"] == "search_datasets"

    def test_max_entries_respected(self, tmp_path):
        store = MemoryStore(str(tmp_path / "log.json"))
        log = ToolCallLog(store, max_entries=5)
        for i in range(10):
            log.append(f"tool_{i}", {}, float(i), True)

        assert log.total_calls == 5  # ring buffer capped

    def test_params_summary_only_ids(self, tmp_path):
        store = MemoryStore(str(tmp_path / "log.json"))
        log = ToolCallLog(store)
        log.append(
            "update_dataset_sql",
            {"dataset_id": "ds-123", "new_sql": "SELECT * FROM big_table"},
            100.0, True,
        )
        recent = log.get_recent(1)
        # Should only have dataset_id, not new_sql
        assert "dataset_id" in recent[0]["params_summary"]
        assert "new_sql" not in recent[0]["params_summary"]

    def test_persistence_via_flush(self, tmp_path):
        store = MemoryStore(str(tmp_path / "log.json"))
        log = ToolCallLog(store)
        log.append("tool_a", {}, 10.0, True)
        log.flush_to_store()
        store.flush()

        # Reload
        store2 = MemoryStore(str(tmp_path / "log.json"))
        log2 = ToolCallLog(store2)
        assert log2.total_calls == 1


class TestLatencyTracker:
    """Tests for the latency time-series tracker."""

    def test_record_and_get(self, tmp_path):
        store = MemoryStore(str(tmp_path / "latency.json"))
        lt = LatencyTracker(store)
        lt.record("list_datasets", 100.0)
        lt.record("list_datasets", 150.0)

        samples = lt.get_samples("list_datasets")
        assert len(samples) == 2

    def test_max_samples(self, tmp_path):
        store = MemoryStore(str(tmp_path / "latency.json"))
        lt = LatencyTracker(store, max_samples=5)
        for i in range(10):
            lt.record("tool_a", float(i * 10))

        samples = lt.get_samples("tool_a")
        assert len(samples) == 5

    def test_get_stats(self, tmp_path):
        store = MemoryStore(str(tmp_path / "latency.json"))
        lt = LatencyTracker(store)
        for ms in [100.0, 200.0, 150.0, 300.0, 250.0]:
            lt.record("tool_a", ms)

        stats = lt.get_stats("tool_a")
        assert stats["count"] == 5
        assert stats["avg_ms"] == 200.0
        assert stats["min_ms"] == 100.0
        assert stats["max_ms"] == 300.0

    def test_get_all_tools(self, tmp_path):
        store = MemoryStore(str(tmp_path / "latency.json"))
        lt = LatencyTracker(store)
        lt.record("tool_a", 10.0)
        lt.record("tool_b", 20.0)

        tools = lt.get_all_tools()
        assert set(tools) == {"tool_a", "tool_b"}

    def test_empty_stats(self, tmp_path):
        store = MemoryStore(str(tmp_path / "latency.json"))
        lt = LatencyTracker(store)
        stats = lt.get_stats("nonexistent")
        assert stats["count"] == 0


class TestKnowledgeGraph:
    """Tests for the entity-relationship knowledge graph."""

    def test_add_and_get_entity(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_entity("dataset", "ds-123", {"name": "WBR"})

        entity = kg.get_entity("dataset", "ds-123")
        assert entity is not None
        assert entity["name"] == "WBR"
        assert "last_updated" in entity

    def test_entity_update_merges(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_entity("dataset", "ds-123", {"name": "WBR"})
        kg.add_entity("dataset", "ds-123", {"last_error": "auth_expired"})

        entity = kg.get_entity("dataset", "ds-123")
        assert entity["name"] == "WBR"
        assert entity["last_error"] == "auth_expired"

    def test_add_and_get_relationship(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_relationship("error_on", "auth_expired", "ds-123")

        rel = kg.get_relationship("error_on", "auth_expired", "ds-123")
        assert rel is not None
        assert rel["count"] == 1

    def test_relationship_count_increments(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_relationship("error_on", "auth_expired", "ds-123")
        kg.add_relationship("error_on", "auth_expired", "ds-123")

        rel = kg.get_relationship("error_on", "auth_expired", "ds-123")
        assert rel["count"] == 2

    def test_find_relationships(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_relationship("error_on", "auth_expired", "ds-123")
        kg.add_relationship("error_on", "auth_expired", "ds-456")
        kg.add_relationship("error_on", "not_found", "ds-789")

        # Find all auth_expired errors
        rels = kg.find_relationships("error_on", source="auth_expired")
        assert len(rels) == 2

    def test_get_entities_by_type(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_entity("dataset", "ds-1", {"name": "A"})
        kg.add_entity("dataset", "ds-2", {"name": "B"})
        kg.add_entity("analysis", "a-1", {"name": "C"})

        datasets = kg.get_entities_by_type("dataset")
        assert len(datasets) == 2

    def test_get_missing_entity(self, tmp_path):
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        assert kg.get_entity("dataset", "nope") is None


class TestMemoryManagerBrainIntegration:
    """Tests that MemoryManager wires brain components correctly."""

    def test_call_log_populated(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call("list_datasets", {}, 100.0, True)
        mgr.record_call("search_datasets", {"name": "wbr"}, 50.0, True)

        assert mgr.call_log.total_calls == 2

    def test_latency_tracked(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call("list_datasets", {}, 100.0, True)
        mgr.record_call("list_datasets", {}, 200.0, True)

        stats = mgr.latency.get_stats("list_datasets")
        assert stats["count"] == 2
        assert stats["avg_ms"] == 150.0

    def test_knowledge_graph_entity_on_call(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call(
            "get_dataset_sql", {"dataset_id": "ds-abc"}, 50.0, True,
        )

        entity = mgr.knowledge.get_entity("dataset", "ds-abc")
        assert entity is not None
        assert entity["last_tool"] == "get_dataset_sql"

    def test_knowledge_graph_error_relationship(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call(
            "update_dataset_sql",
            {"dataset_id": "ds-abc"},
            100.0, False,
            "ExpiredToken: credentials expired",
        )

        entity = mgr.knowledge.get_entity("dataset", "ds-abc")
        assert entity["last_error_type"] == "auth_expired"

        rels = mgr.knowledge.find_relationships(
            "error_on", source="auth_expired",
        )
        assert len(rels) == 1
        assert rels[0]["target"] == "ds-abc"

    def test_brain_flush_persists_all(self, tmp_path):
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call("list_datasets", {"dataset_id": "ds-abc"}, 100.0, True)
        mgr.flush()

        # Reload and verify ALL brain components
        mgr2 = MemoryManager(str(tmp_path))
        assert mgr2.call_log.total_calls == 1
        assert mgr2.latency.get_stats("list_datasets")["count"] == 1
        # KnowledgeGraph must also persist
        entity = mgr2.knowledge.get_entity("dataset", "ds-abc")
        assert entity is not None
        assert entity["last_tool"] == "list_datasets"

    def test_record_call_with_analysis_id_entity_type(self, tmp_path):
        """Entity type should be 'analysis' when analysis_id param is used."""
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call(
            "describe_analysis", {"analysis_id": "a-xyz"}, 50.0, True,
        )
        entity = mgr.knowledge.get_entity("analysis", "a-xyz")
        assert entity is not None
        assert entity["last_tool"] == "describe_analysis"

    def test_record_call_with_dashboard_id_entity_type(self, tmp_path):
        """Entity type should be 'dashboard' when dashboard_id param is used."""
        mgr = MemoryManager(str(tmp_path))
        mgr.record_call(
            "publish_dashboard", {"dashboard_id": "d-abc"}, 80.0, True,
        )
        entity = mgr.knowledge.get_entity("dashboard", "d-abc")
        assert entity is not None

    def test_record_call_with_no_resource_id(self, tmp_path):
        """record_call with no resource ID params should not error."""
        mgr = MemoryManager(str(tmp_path))
        # No dataset_id, analysis_id, or dashboard_id
        mgr.record_call("get_learning_insights", {}, 10.0, True)
        # Should have recorded in usage and call_log, but no entity
        assert mgr.call_log.total_calls == 1
        assert mgr.usage.get_insights()["total_calls"] == 1

    def test_knowledge_graph_persistence(self, tmp_path):
        """KnowledgeGraph entities and relationships must survive restart."""
        store = MemoryStore(str(tmp_path / "kg.json"))
        kg = KnowledgeGraph(store)
        kg.add_entity("dataset", "ds-1", {"name": "WBR"})
        kg.add_relationship("error_on", "auth_expired", "ds-1")
        store.flush()

        # Reload from disk
        store2 = MemoryStore(str(tmp_path / "kg.json"))
        kg2 = KnowledgeGraph(store2)
        entity = kg2.get_entity("dataset", "ds-1")
        assert entity["name"] == "WBR"
        rel = kg2.get_relationship("error_on", "auth_expired", "ds-1")
        assert rel["count"] == 1
