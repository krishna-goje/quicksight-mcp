"""Instance-level TTL cache replacing the module-level global dicts.

Provides ``TTLCache`` — a simple key→value cache with time-based expiration.
Each service gets its own cache instance, eliminating shared mutable globals.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional


class TTLCache:
    """Simple TTL-based cache (not thread-safe, not needed for sync MCP).

    Args:
        ttl: Time-to-live in seconds for cache entries (default 300 = 5 min).
        max_entries: Maximum number of cached keys.  When exceeded, the oldest
            entry is evicted regardless of TTL.
    """

    def __init__(self, ttl: int = 300, max_entries: int = 500):
        self._ttl = ttl
        self._max_entries = max_entries
        self._store: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[Any]:
        """Return cached value or ``None`` if missing / expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if time.time() - entry["ts"] > self._ttl:
            del self._store[key]
            return None
        return entry["val"]

    def set(self, key: str, value: Any) -> None:
        """Store *value* under *key* with the current timestamp."""
        if len(self._store) >= self._max_entries:
            self._evict_oldest()
        self._store[key] = {"val": value, "ts": time.time()}

    def invalidate(self, key: str) -> None:
        """Remove a specific key from the cache."""
        self._store.pop(key, None)

    def clear(self) -> None:
        """Remove all entries."""
        self._store.clear()

    def has(self, key: str) -> bool:
        """Return ``True`` if *key* exists and has not expired."""
        return self.get(key) is not None

    @property
    def size(self) -> int:
        """Number of entries (including possibly-expired ones)."""
        return len(self._store)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _evict_oldest(self) -> None:
        """Remove the entry with the smallest timestamp."""
        if not self._store:
            return
        oldest_key = min(self._store, key=lambda k: self._store[k]["ts"])
        del self._store[oldest_key]
