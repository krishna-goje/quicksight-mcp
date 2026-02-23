"""Brain — self-improvement engine for the QuickSight MCP server.

The brain reviews its own operations and generates actionable insights:
- Error patterns with recovery scoring
- Workflow optimization suggestions
- Latency degradation detection
- Resource context for faster troubleshooting
"""

from quicksight_mcp.brain.analyzer import BrainAnalyzer

__all__ = ["BrainAnalyzer"]
