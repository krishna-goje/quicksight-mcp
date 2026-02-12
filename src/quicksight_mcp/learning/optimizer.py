"""Pattern analyzer that generates optimization suggestions from usage data."""

import logging
from typing import Dict, List

from .tracker import UsageTracker

logger = logging.getLogger(__name__)


class Optimizer:
    """Analyzes usage patterns and suggests optimizations."""

    def __init__(self, tracker: UsageTracker):
        self.tracker = tracker

    def get_recommendations(self) -> List[Dict]:
        """Get optimization recommendations based on usage data."""
        recommendations = []
        insights = self.tracker.get_insights()
        error_data = self.tracker.get_error_patterns()

        # Check for auth errors
        for key, pattern in error_data.get('patterns', {}).items():
            if 'auth_expired' in key and pattern.get('count', 0) > 3:
                recommendations.append({
                    'type': 'auth',
                    'priority': 'high',
                    'message': 'Frequent authentication failures detected. '
                               'Consider using longer-lived credentials or '
                               'refreshing before batch operations.',
                    'count': pattern['count'],
                })

        # Check for rate limiting
        for key, pattern in error_data.get('patterns', {}).items():
            if 'rate_limited' in key and pattern.get('count', 0) > 2:
                recommendations.append({
                    'type': 'rate_limit',
                    'priority': 'medium',
                    'message': 'Rate limiting detected. Add delays between '
                               'rapid API calls or use cached operations.',
                    'count': pattern['count'],
                })

        # Check for common SQL errors
        for key, pattern in error_data.get('patterns', {}).items():
            if 'sql_syntax' in key:
                recommendations.append({
                    'type': 'sql_hint',
                    'priority': 'low',
                    'message': 'SQL syntax errors detected. Common QuickSight SQL '
                               'gotchas: ROWS is a reserved keyword (use row_cnt), '
                               'column aliases required for expressions.',
                    'count': pattern['count'],
                })

        # Workflow optimization
        for workflow in insights.get('common_workflows', []):
            if workflow['count'] > 10:
                seq = workflow['sequence']
                if 'search_datasets' in seq and 'update_dataset_sql' in seq:
                    recommendations.append({
                        'type': 'workflow',
                        'priority': 'info',
                        'message': 'Common workflow: search -> get SQL -> update SQL. '
                                   'After updating SQL, remember to trigger a SPICE refresh.',
                    })

        return sorted(
            recommendations,
            key=lambda x: {'high': 0, 'medium': 1, 'low': 2, 'info': 3}.get(
                x['priority'], 4
            ),
        )
