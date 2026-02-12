"""Local JSON-based storage for learned patterns and cache hints."""

import json
import os
from pathlib import Path
from typing import Any, Dict


class KnowledgeStore:
    """Local JSON-based storage for learned patterns."""

    def __init__(self, storage_dir: str = None):
        self.storage_dir = Path(storage_dir or os.environ.get(
            'QUICKSIGHT_MCP_LEARNING_DIR',
            os.path.expanduser('~/.quicksight-mcp')
        ))
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the knowledge store."""
        data = self._load('knowledge.json')
        return data.get(key, default)

    def set(self, key: str, value: Any):
        """Set a value in the knowledge store."""
        data = self._load('knowledge.json')
        data[key] = value
        self._save('knowledge.json', data)

    def get_cache_hints(self) -> Dict:
        """Get cache optimization hints."""
        return self._load('cache_hints.json')

    def update_cache_hint(self, resource_type: str, resource_id: str, access_count: int):
        """Update cache hint for a resource."""
        hints = self._load('cache_hints.json')
        key = f"{resource_type}:{resource_id}"
        hints[key] = {
            'access_count': access_count,
            'resource_type': resource_type,
            'resource_id': resource_id,
        }
        self._save('cache_hints.json', hints)

    def _load(self, filename: str) -> dict:
        path = self.storage_dir / filename
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return {}

    def _save(self, filename: str, data: dict):
        path = self.storage_dir / filename
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
