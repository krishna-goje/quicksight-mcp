"""MemoryManager — single entry point for the full context memory system.

Wires together seven sub-components:
- UsageTracker:     tool call recording, workflow detection, timing stats
- AnalysisMemory:   analysis structures (sheets, visuals, calc fields per analysis)
- ErrorMemory:      failures per resource + recovery suggestions
- PreferenceMemory: user preferences (format defaults, common patterns)
- ToolCallLog:      append-only log of individual calls (for retrospective analysis)
- LatencyTracker:   time-series latency samples per tool (for degradation detection)
- KnowledgeGraph:   entity-relationship store (resources, errors, recoveries)
"""

from __future__ import annotations

import atexit
import logging
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

from quicksight_mcp.memory.store import MemoryStore

logger = logging.getLogger(__name__)


# =========================================================================
# Original sub-components (v1.0)
# =========================================================================


class UsageTracker:
    """Records tool calls, detects workflow patterns, tracks timing."""

    def __init__(self, store: MemoryStore):
        self._store = store
        self._sequence_buffer: List[str] = []
        self._call_count = 0

        # Initialize counters if missing
        if not self._store.get("tool_counts"):
            self._store.set("tool_counts", {})
        if not self._store.get("sequences"):
            self._store.set("sequences", {})
        if not self._store.get("avg_durations"):
            self._store.set("avg_durations", {})

    def record_call(
        self, tool_name: str, params: dict, duration_ms: float,
        success: bool, error: str = None,
    ) -> None:
        """Record a tool call for pattern analysis."""
        self._call_count += 1

        # Update counts
        counts = self._store.get("tool_counts", {})
        counts[tool_name] = counts.get(tool_name, 0) + 1
        self._store.set("tool_counts", counts)

        # Update average durations
        durations = self._store.get("avg_durations", {})
        if tool_name in durations:
            old = durations[tool_name]
            new_count = old["count"] + 1
            durations[tool_name] = {
                "avg": (old["avg"] * old["count"] + duration_ms) / new_count,
                "count": new_count,
            }
        else:
            durations[tool_name] = {"avg": duration_ms, "count": 1}
        self._store.set("avg_durations", durations)

        # Track sequences
        self._sequence_buffer.append(tool_name)
        if len(self._sequence_buffer) > 5:
            self._sequence_buffer.pop(0)
        if len(self._sequence_buffer) >= 2:
            seq_key = f"{self._sequence_buffer[-2]} -> {self._sequence_buffer[-1]}"
            sequences = self._store.get("sequences", {})
            sequences[seq_key] = sequences.get(seq_key, 0) + 1
            self._store.set("sequences", sequences)

    def get_insights(self) -> dict:
        """Get usage insights and suggestions."""
        counts = self._store.get("tool_counts", {})
        total_calls = sum(counts.values())
        top_tools = sorted(counts.items(), key=lambda x: -x[1])[:10]

        sequences = self._store.get("sequences", {})
        top_sequences = sorted(sequences.items(), key=lambda x: -x[1])[:5]

        durations = self._store.get("avg_durations", {})
        slowest = sorted(
            durations.items(),
            key=lambda x: -x[1].get("avg", 0),
        )[:5]

        suggestions = []
        for seq, count in sequences.items():
            if count > 5:
                suggestions.append(
                    f"Frequent workflow ({count}x): {seq}. "
                    f"Consider a compound operation."
                )

        return {
            "total_calls": total_calls,
            "most_used_tools": [
                {"tool": t, "count": c} for t, c in top_tools
            ],
            "slowest_tools": [
                {"tool": t, "avg_ms": d["avg"], "count": d["count"]}
                for t, d in slowest
            ],
            "common_workflows": [
                {"sequence": s, "count": c} for s, c in top_sequences
            ],
            "suggestions": suggestions,
        }


class AnalysisMemory:
    """Remembers analysis structures to avoid redundant API calls."""

    def __init__(self, store: MemoryStore):
        self._store = store

    def remember_structure(
        self, analysis_id: str, sheets: List[Dict], calc_fields: List[str],
    ) -> None:
        """Cache the structure of an analysis."""
        self._store.set(
            f"analysis:{analysis_id}",
            {
                "sheets": sheets,
                "calc_fields": calc_fields,
                "remembered_at": time.time(),
            },
        )

    def get_structure(self, analysis_id: str) -> Optional[Dict]:
        """Get remembered structure, or None."""
        return self._store.get(f"analysis:{analysis_id}")

    def forget(self, analysis_id: str) -> None:
        """Forget an analysis structure (after modification)."""
        self._store.delete(f"analysis:{analysis_id}")


class ErrorMemory:
    """Tracks errors per resource and provides recovery suggestions."""

    def __init__(self, store: MemoryStore):
        self._store = store

    def record_error(
        self, resource_id: str, error_type: str, error_msg: str,
        recovery_used: str = "", recovery_worked: bool = False,
    ) -> None:
        """Record an error occurrence."""
        key = f"error:{resource_id}:{error_type}"
        existing = self._store.get(key, {})
        self._store.set(key, {
            "count": existing.get("count", 0) + 1,
            "last_seen": time.time(),
            "first_seen": existing.get("first_seen", time.time()),
            "sample_error": error_msg[:500],
            "recovery_used": recovery_used or existing.get("recovery_used", ""),
            "recovery_worked": recovery_worked or existing.get("recovery_worked", False),
        })

    def record_recovery(
        self, resource_id: str, error_type: str, recovery: str, worked: bool,
    ) -> None:
        """Record a recovery attempt and whether it worked."""
        key = f"error:{resource_id}:{error_type}"
        existing = self._store.get(key, {})
        existing["recovery_used"] = recovery
        existing["recovery_worked"] = worked
        self._store.set(key, existing)

    def get_recovery_suggestions(
        self, resource_id: str, error_type: str,
    ) -> List[str]:
        """Get past recovery suggestions for this resource + error type."""
        suggestions = []
        # Check exact match
        key = f"error:{resource_id}:{error_type}"
        exact = self._store.get(key)
        if exact and exact.get("recovery_used") and exact.get("recovery_worked"):
            suggestions.append(
                f"Past recovery (worked): {exact['recovery_used']}"
            )

        # Check same error type across all resources
        for k in self._store.keys():
            if k.startswith("error:") and k.endswith(f":{error_type}"):
                entry = self._store.get(k)
                if entry and entry.get("recovery_worked") and entry.get("recovery_used"):
                    r = entry["recovery_used"]
                    if r not in [s.split(": ", 1)[-1] for s in suggestions]:
                        suggestions.append(f"Past recovery (similar): {r}")

        return suggestions[:3]

    def get_patterns(self) -> dict:
        """Get all error patterns for the get_error_patterns tool."""
        patterns = {}
        total = 0
        for key in self._store.keys():
            if key.startswith("error:"):
                entry = self._store.get(key)
                if entry:
                    patterns[key] = entry
                    total += entry.get("count", 0)
        return {"patterns": patterns, "total_errors": total}


class PreferenceMemory:
    """Tracks user preferences and common patterns."""

    def __init__(self, store: MemoryStore):
        self._store = store

    def set_preference(self, key: str, value: Any) -> None:
        self._store.set(f"pref:{key}", value)

    def get_preference(self, key: str, default: Any = None) -> Any:
        return self._store.get(f"pref:{key}", default)


# =========================================================================
# New brain sub-components (v1.1)
# =========================================================================


class ToolCallLog:
    """Append-only log of individual tool calls for retrospective analysis.

    Stores raw call records (tool_name, params summary, duration, success,
    timestamp) in a bounded ring buffer. Persisted via MemoryStore.

    Max entries controlled by ``max_entries`` (default 2000).
    """

    def __init__(self, store: MemoryStore, max_entries: int = 2000):
        self._store = store
        self._max = max_entries
        # In-memory ring buffer, synced to store on flush
        raw = self._store.get("call_log", [])
        self._log: deque = deque(raw if isinstance(raw, list) else [], maxlen=max_entries)

    def append(
        self,
        tool_name: str,
        params: dict,
        duration_ms: float,
        success: bool,
        error: str = "",
    ) -> None:
        """Append a call record."""
        self._log.append({
            "tool": tool_name,
            "ts": time.time(),
            "duration_ms": round(duration_ms, 1),
            "success": success,
            "params_summary": self._summarize_params(params),
            "error": error[:200] if error else "",
        })

    def get_recent(self, n: int = 50) -> List[Dict]:
        """Get the N most recent call records."""
        return list(self._log)[-n:]

    def get_all(self) -> List[Dict]:
        """Get all records."""
        return list(self._log)

    @property
    def total_calls(self) -> int:
        return len(self._log)

    def flush_to_store(self) -> None:
        """Persist to the backing MemoryStore."""
        self._store.set("call_log", list(self._log))

    @staticmethod
    def _summarize_params(params: dict) -> dict:
        """Keep only ID-like params for the log (privacy/size)."""
        summary = {}
        for k, v in params.items():
            if k.endswith("_id") or k == "name":
                summary[k] = str(v)[:100]
        return summary


class LatencyTracker:
    """Time-series latency samples per tool for degradation detection.

    Stores per-tool latency history (last N samples) so the BrainAnalyzer
    can compare recent vs historical and flag degradation.
    """

    def __init__(self, store: MemoryStore, max_samples: int = 100):
        self._store = store
        self._max = max_samples

    def record(self, tool_name: str, duration_ms: float) -> None:
        """Record a latency sample."""
        key = f"latency:{tool_name}"
        samples = self._store.get(key, [])
        if not isinstance(samples, list):
            samples = []
        samples.append({
            "ts": time.time(),
            "ms": round(duration_ms, 1),
        })
        # Keep only last N
        if len(samples) > self._max:
            samples = samples[-self._max:]
        self._store.set(key, samples)

    def get_samples(self, tool_name: str) -> List[Dict]:
        """Get latency samples for a tool."""
        samples = self._store.get(f"latency:{tool_name}", [])
        return samples if isinstance(samples, list) else []

    def get_all_tools(self) -> List[str]:
        """Get all tools with latency data."""
        return [
            k.split(":", 1)[1]
            for k in self._store.keys()
            if k.startswith("latency:")
        ]

    def get_stats(self, tool_name: str) -> Dict[str, Any]:
        """Get latency statistics for a tool."""
        samples = self.get_samples(tool_name)
        if not samples:
            return {"count": 0}
        durations = [s["ms"] for s in samples]
        n = len(durations)
        avg = sum(durations) / n
        sorted_d = sorted(durations)
        p50 = sorted_d[n // 2]
        p95 = sorted_d[int(n * 0.95)] if n >= 20 else sorted_d[-1]
        return {
            "count": n,
            "avg_ms": round(avg, 1),
            "p50_ms": round(p50, 1),
            "p95_ms": round(p95, 1),
            "min_ms": round(sorted_d[0], 1),
            "max_ms": round(sorted_d[-1], 1),
        }


class KnowledgeGraph:
    """Entity-relationship store for resources, errors, and recoveries.

    Stores facts like:
    - ``entity:dataset:ds-123`` → ``{name: "WBR", last_error: "auth_expired"}``
    - ``rel:error_recovery:auth_expired:saml2aws`` → ``{success_rate: 0.85}``

    Used by BrainAnalyzer to provide contextual insights.
    """

    def __init__(self, store: MemoryStore, max_entities: int = 5000):
        self._store = store
        self._max = max_entities

    def add_entity(
        self,
        entity_type: str,
        entity_id: str,
        properties: Dict[str, Any],
    ) -> None:
        """Add or update an entity."""
        key = f"entity:{entity_type}:{entity_id}"
        existing = self._store.get(key, {})
        if not isinstance(existing, dict):
            existing = {}
        existing.update(properties)
        existing["last_updated"] = time.time()
        self._store.set(key, existing)

    def get_entity(self, entity_type: str, entity_id: str) -> Optional[Dict]:
        """Get entity properties."""
        return self._store.get(f"entity:{entity_type}:{entity_id}")

    def add_relationship(
        self,
        rel_type: str,
        source: str,
        target: str,
        properties: Optional[Dict] = None,
    ) -> None:
        """Add a relationship between two entities."""
        key = f"rel:{rel_type}:{source}:{target}"
        existing = self._store.get(key, {})
        if not isinstance(existing, dict):
            existing = {}
        if properties:
            existing.update(properties)
        existing["last_updated"] = time.time()
        existing["count"] = existing.get("count", 0) + 1
        self._store.set(key, existing)

    def get_relationship(
        self, rel_type: str, source: str, target: str,
    ) -> Optional[Dict]:
        """Get relationship properties."""
        return self._store.get(f"rel:{rel_type}:{source}:{target}")

    def find_relationships(
        self, rel_type: str, source: str = "", target: str = "",
    ) -> List[Dict]:
        """Find relationships matching a pattern."""
        results = []
        prefix = f"rel:{rel_type}:"
        for key in self._store.keys():
            if not key.startswith(prefix):
                continue
            parts = key[len(prefix):].split(":", 1)
            if len(parts) != 2:
                continue
            s, t = parts
            if source and s != source:
                continue
            if target and t != target:
                continue
            val = self._store.get(key)
            if val:
                results.append({"source": s, "target": t, **val})
        return results

    def get_entities_by_type(self, entity_type: str) -> List[Dict]:
        """Get all entities of a given type."""
        prefix = f"entity:{entity_type}:"
        results = []
        for key in self._store.keys():
            if key.startswith(prefix):
                entity_id = key[len(prefix):]
                val = self._store.get(key)
                if val:
                    results.append({"id": entity_id, **val})
        return results


# =========================================================================
# MemoryManager (orchestrator)
# =========================================================================


class MemoryManager:
    """Single entry point for the full context memory system.

    Wires together all sub-components and handles persistence lifecycle.

    Args:
        storage_dir: Directory for memory JSON files.
        enabled: Whether memory is active.
        max_entries: Max entries per store.
        max_file_bytes: Max file size per store.
        flush_interval: Flush every N tool calls.
        max_call_log: Max entries in the ToolCallLog.
        max_knowledge: Max entries in the KnowledgeGraph.
    """

    def __init__(
        self,
        storage_dir: str,
        enabled: bool = True,
        max_entries: int = 1000,
        max_file_bytes: int = 5 * 1024 * 1024,
        flush_interval: int = 10,
        max_call_log: int = 2000,
        max_knowledge: int = 5000,
    ):
        self.enabled = enabled
        self._flush_interval = flush_interval
        self._call_count = 0

        if not enabled:
            self.usage = None
            self.analyses = None
            self.errors = None
            self.preferences = None
            self.call_log = None
            self.latency = None
            self.knowledge = None
            return

        storage = Path(storage_dir)
        storage.mkdir(parents=True, exist_ok=True)

        # Create stores
        self._usage_store = MemoryStore(
            str(storage / "usage.json"), max_entries, max_file_bytes,
        )
        self._analysis_store = MemoryStore(
            str(storage / "analyses.json"), max_entries, max_file_bytes,
        )
        self._error_store = MemoryStore(
            str(storage / "errors.json"), max_entries, max_file_bytes,
        )
        self._pref_store = MemoryStore(
            str(storage / "preferences.json"), max_entries, max_file_bytes,
        )
        self._call_log_store = MemoryStore(
            str(storage / "call_log.json"), max_call_log, max_file_bytes,
        )
        self._latency_store = MemoryStore(
            str(storage / "latency.json"), max_entries, max_file_bytes,
        )
        self._knowledge_store = MemoryStore(
            str(storage / "knowledge.json"), max_knowledge, max_file_bytes,
        )

        # Create sub-components (original)
        self.usage = UsageTracker(self._usage_store)
        self.analyses = AnalysisMemory(self._analysis_store)
        self.errors = ErrorMemory(self._error_store)
        self.preferences = PreferenceMemory(self._pref_store)

        # Create sub-components (brain v1.1)
        self.call_log = ToolCallLog(self._call_log_store, max_call_log)
        self.latency = LatencyTracker(self._latency_store)
        self.knowledge = KnowledgeGraph(self._knowledge_store, max_knowledge)

        # Register shutdown handler
        atexit.register(self.flush)

    def record_call(
        self, tool_name: str, params: dict, duration_ms: float,
        success: bool, error: str = None,
    ) -> None:
        """Record a tool call (delegates to all relevant sub-components)."""
        if not self.enabled:
            return

        # Original tracking
        self.usage.record_call(tool_name, params, duration_ms, success, error)

        # Brain tracking
        self.call_log.append(tool_name, params, duration_ms, success, error or "")
        self.latency.record(tool_name, duration_ms)

        # Knowledge graph: track resource context
        resource_id = params.get(
            "dataset_id",
            params.get("analysis_id", params.get("dashboard_id", "")),
        )
        if resource_id:
            props: Dict[str, Any] = {
                "last_tool": tool_name,
                "last_access": time.time(),
            }
            if not success and error:
                props["last_error"] = error[:200]
                props["last_error_type"] = self._classify_error(error)
            # Determine entity type from param name
            if params.get("dataset_id"):
                entity_type = "dataset"
            elif params.get("analysis_id"):
                entity_type = "analysis"
            else:
                entity_type = "dashboard"
            self.knowledge.add_entity(entity_type, resource_id, props)

        if not success and error:
            error_type = self._classify_error(error)
            if resource_id:
                self.errors.record_error(resource_id, error_type, error)
                # Track error→resource relationship
                self.knowledge.add_relationship(
                    "error_on", error_type, resource_id,
                )

        # Periodic flush
        self._call_count += 1
        if self._call_count % self._flush_interval == 0:
            self.flush()

    def get_recovery_suggestions(
        self, resource_id: str, error_type: str,
    ) -> List[str]:
        """Get past recovery suggestions."""
        if not self.enabled:
            return []
        return self.errors.get_recovery_suggestions(resource_id, error_type)

    def flush(self) -> None:
        """Persist all stores to disk."""
        if not self.enabled:
            return

        # Flush call log ring buffer to its store first
        try:
            self.call_log.flush_to_store()
        except Exception as e:
            logger.warning("Failed to flush call log: %s", e)

        for store in (
            self._usage_store, self._analysis_store,
            self._error_store, self._pref_store,
            self._call_log_store, self._latency_store,
            self._knowledge_store,
        ):
            try:
                store.flush()
            except Exception as e:
                logger.warning("Failed to flush memory store: %s", e)

    @staticmethod
    def _classify_error(error: str) -> str:
        """Classify error string into a category."""
        lower = error.lower()
        if "expired" in lower or "credential" in lower:
            return "auth_expired"
        if "not found" in lower or "404" in lower:
            return "not_found"
        if "concurrent" in lower or "conflict" in lower:
            return "concurrent_modification"
        if "throttl" in lower or "rate" in lower:
            return "rate_limited"
        if "permission" in lower or "access denied" in lower:
            return "permission_denied"
        return "unknown"
