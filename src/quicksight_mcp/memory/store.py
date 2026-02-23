"""JSON-backed memory store with TTL eviction and size limits.

Provides the persistence layer for all memory components.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class MemoryStore:
    """JSON file-backed key-value store with auto-eviction.

    Args:
        file_path: Path to the JSON file.
        max_entries: Maximum entries before eviction.
        max_file_bytes: Maximum file size in bytes.
    """

    def __init__(
        self,
        file_path: str,
        max_entries: int = 1000,
        max_file_bytes: int = 5 * 1024 * 1024,
    ):
        self._path = Path(file_path)
        self._max_entries = max_entries
        self._max_file_bytes = max_file_bytes
        self._data: Dict[str, Any] = {}
        self._dirty = False
        self._load()

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value by key."""
        entry = self._data.get(key)
        if entry is None:
            return default
        return entry.get("value", default)

    def set(self, key: str, value: Any) -> None:
        """Set a value with timestamp."""
        self._data[key] = {
            "value": value,
            "ts": time.time(),
            "access_count": self._data.get(key, {}).get("access_count", 0) + 1,
        }
        self._dirty = True
        self._evict_if_needed()

    def delete(self, key: str) -> None:
        """Remove a key."""
        self._data.pop(key, None)
        self._dirty = True

    def keys(self) -> List[str]:
        """List all keys."""
        return list(self._data.keys())

    def values(self) -> List[Any]:
        """List all values (unwrapped)."""
        return [e.get("value") for e in self._data.values()]

    def items(self) -> List[tuple]:
        """List all (key, value) pairs (unwrapped)."""
        return [(k, e.get("value")) for k, e in self._data.items()]

    @property
    def size(self) -> int:
        return len(self._data)

    def flush(self) -> None:
        """Persist to disk (atomic write)."""
        if not self._dirty:
            return
        self._save()
        self._dirty = False

    def clear(self) -> None:
        """Remove all entries."""
        self._data.clear()
        self._dirty = True

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load from disk if file exists."""
        if self._path.exists():
            try:
                with open(self._path) as f:
                    raw = json.load(f)
                # Handle both old format (flat dict) and new format (with metadata)
                if isinstance(raw, dict):
                    # Check if it's already in our format
                    if raw and all(
                        isinstance(v, dict) and "value" in v
                        for v in list(raw.values())[:1]
                    ):
                        self._data = raw
                    else:
                        # Old format: wrap values
                        self._data = {
                            k: {"value": v, "ts": time.time(), "access_count": 0}
                            for k, v in raw.items()
                        }
            except (json.JSONDecodeError, IOError) as e:
                logger.warning("Failed to load memory from %s: %s", self._path, e)
                self._data = {}

    def _save(self) -> None:
        """Atomic write to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=str(self._path.parent),
                prefix=".mem_",
                suffix=".tmp",
            )
            with os.fdopen(fd, "w") as f:
                json.dump(self._data, f, indent=2, default=str)
            os.rename(tmp_path, str(self._path))
        except Exception as e:
            logger.warning("Failed to save memory to %s: %s", self._path, e)
            # Clean up temp file if rename failed
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def _evict_if_needed(self) -> None:
        """Evict oldest 20% of entries when at capacity."""
        if len(self._data) <= self._max_entries:
            return

        # Sort by timestamp, evict oldest 20%
        n_evict = max(1, len(self._data) // 5)
        sorted_keys = sorted(
            self._data.keys(),
            key=lambda k: self._data[k].get("ts", 0),
        )
        for key in sorted_keys[:n_evict]:
            del self._data[key]
        logger.debug("Evicted %d memory entries", n_evict)
