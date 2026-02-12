"""Usage tracker that logs tool calls and detects patterns for self-learning."""

import json
import time
import logging
from pathlib import Path
from typing import Dict, List
import os

logger = logging.getLogger(__name__)


class UsageTracker:
    """Tracks MCP tool usage patterns for self-learning."""

    def __init__(self, storage_dir: str = None):
        self.storage_dir = Path(storage_dir or os.environ.get(
            'QUICKSIGHT_MCP_LEARNING_DIR',
            os.path.expanduser('~/.quicksight-mcp')
        ))
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.enabled = os.environ.get('QUICKSIGHT_MCP_LEARNING', 'true').lower() == 'true'

        self._call_log: List[Dict] = []
        self._error_log: List[Dict] = []
        self._sequence_buffer: List[str] = []

        # Load persisted patterns
        self._patterns = self._load_json('patterns.json', default={
            'tool_counts': {},
            'sequences': {},
            'avg_durations': {},
        })
        self._error_patterns = self._load_json('error_recovery.json', default={})

    def record_call(self, tool_name: str, params: dict, duration_ms: float,
                    success: bool, error: str = None):
        """Record a tool call for pattern analysis."""
        if not self.enabled:
            return

        entry = {
            'tool': tool_name,
            'params_keys': list(params.keys()),
            'duration_ms': duration_ms,
            'success': success,
            'error': error,
            'timestamp': time.time(),
        }
        self._call_log.append(entry)

        # Track sequences (last 5 tools called)
        self._sequence_buffer.append(tool_name)
        if len(self._sequence_buffer) > 5:
            self._sequence_buffer.pop(0)

        # Update counts
        counts = self._patterns.setdefault('tool_counts', {})
        counts[tool_name] = counts.get(tool_name, 0) + 1

        # Update average durations
        durations = self._patterns.setdefault('avg_durations', {})
        if tool_name in durations:
            old_avg = durations[tool_name]['avg']
            old_count = durations[tool_name]['count']
            new_count = old_count + 1
            durations[tool_name] = {
                'avg': (old_avg * old_count + duration_ms) / new_count,
                'count': new_count
            }
        else:
            durations[tool_name] = {'avg': duration_ms, 'count': 1}

        # Track sequences (pairs)
        if len(self._sequence_buffer) >= 2:
            seq_key = f"{self._sequence_buffer[-2]} -> {self._sequence_buffer[-1]}"
            sequences = self._patterns.setdefault('sequences', {})
            sequences[seq_key] = sequences.get(seq_key, 0) + 1

        # Log errors for recovery patterns
        if not success and error:
            self._record_error(tool_name, params, error)

        # Persist periodically (every 10 calls)
        if len(self._call_log) % 10 == 0:
            self._persist()

    def _record_error(self, tool_name: str, params: dict, error: str):
        """Record error for pattern detection."""
        error_key = f"{tool_name}:{self._classify_error(error)}"
        if error_key not in self._error_patterns:
            self._error_patterns[error_key] = {
                'count': 0,
                'first_seen': time.time(),
                'last_seen': time.time(),
                'sample_error': error[:500],
                'tool': tool_name,
            }
        self._error_patterns[error_key]['count'] += 1
        self._error_patterns[error_key]['last_seen'] = time.time()

    def _classify_error(self, error: str) -> str:
        """Classify error into a category."""
        error_lower = error.lower()
        if 'expired' in error_lower or 'credential' in error_lower:
            return 'auth_expired'
        if 'not found' in error_lower or '404' in error_lower:
            return 'not_found'
        if 'concurrent' in error_lower or 'conflict' in error_lower:
            return 'concurrent_modification'
        if 'throttl' in error_lower or 'rate' in error_lower:
            return 'rate_limited'
        if 'permission' in error_lower or 'access denied' in error_lower:
            return 'permission_denied'
        if 'reserved' in error_lower or 'syntax' in error_lower:
            return 'sql_syntax'
        return 'unknown'

    def get_insights(self) -> dict:
        """Get usage insights and suggestions."""
        total_calls = sum(self._patterns.get('tool_counts', {}).values())

        # Top tools
        tool_counts = self._patterns.get('tool_counts', {})
        top_tools = sorted(tool_counts.items(), key=lambda x: -x[1])[:10]

        # Common sequences
        sequences = self._patterns.get('sequences', {})
        top_sequences = sorted(sequences.items(), key=lambda x: -x[1])[:5]

        # Suggestions based on patterns
        suggestions = self._generate_suggestions()

        return {
            'total_calls': total_calls,
            'most_used_tools': [{'tool': t, 'count': c} for t, c in top_tools],
            'common_workflows': [{'sequence': s, 'count': c} for s, c in top_sequences],
            'error_count': sum(e.get('count', 0) for e in self._error_patterns.values()),
            'suggestions': suggestions,
        }

    def get_error_patterns(self) -> dict:
        """Get common errors and their frequencies."""
        return {
            'patterns': self._error_patterns,
            'total_errors': sum(e.get('count', 0) for e in self._error_patterns.values()),
        }

    def _generate_suggestions(self) -> List[str]:
        """Generate optimization suggestions based on usage patterns."""
        suggestions = []
        sequences = self._patterns.get('sequences', {})

        # Suggest compound operations for common sequences
        for seq, count in sequences.items():
            if count > 5:
                suggestions.append(
                    f"Frequent workflow detected ({count}x): {seq}. "
                    f"Consider using a compound operation."
                )

        # Suggest caching for frequently accessed resources
        tool_counts = self._patterns.get('tool_counts', {})
        if tool_counts.get('get_dataset_sql', 0) > 20:
            suggestions.append(
                "Heavy dataset SQL lookups detected. The server caches "
                "definitions for 5 minutes automatically."
            )

        return suggestions

    def _load_json(self, filename: str, default=None) -> dict:
        """Load JSON from storage dir."""
        path = self.storage_dir / filename
        if path.exists():
            try:
                with open(path) as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return default or {}

    def _persist(self):
        """Persist patterns to disk."""
        try:
            with open(self.storage_dir / 'patterns.json', 'w') as f:
                json.dump(self._patterns, f, indent=2)
            with open(self.storage_dir / 'error_recovery.json', 'w') as f:
                json.dump(self._error_patterns, f, indent=2)
        except IOError as e:
            logger.warning(f"Failed to persist learning data: {e}")

    def flush(self):
        """Force persist all data."""
        self._persist()
