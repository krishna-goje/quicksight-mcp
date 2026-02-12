# QuickSight MCP Server

The most comprehensive AWS QuickSight MCP server -- with self-learning capability.

[![PyPI version](https://badge.fury.io/py/quicksight-mcp.svg)](https://pypi.org/project/quicksight-mcp/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Why This Server?

Other QuickSight MCP servers are either auto-generated API wrappers or limited to lineage queries. This server was built by an analytics engineer who uses QuickSight daily in production, wrapping battle-tested patterns into 27 MCP tools.

**Key Differentiators:**

- **27 purpose-built tools** covering datasets, analyses, calculated fields, dashboards, and backups
- **Self-learning engine** that tracks usage patterns and suggests optimizations
- **Production safety** with auto-backup, optimistic locking, destructive change protection, and change verification
- **Built from real workflows** -- not auto-generated from API specs

## Quick Start

### Installation

```bash
pip install quicksight-mcp
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install quicksight-mcp
```

### Configuration

Add to your MCP client config (Claude Desktop, Cursor, etc.):

```json
{
  "mcpServers": {
    "quicksight": {
      "command": "quicksight-mcp",
      "env": {
        "AWS_PROFILE": "your-profile",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

Or with uvx (no install needed):

```json
{
  "mcpServers": {
    "quicksight": {
      "command": "uvx",
      "args": ["quicksight-mcp"],
      "env": {
        "AWS_PROFILE": "your-profile"
      }
    }
  }
}
```

### Authentication

Uses the standard AWS credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Named profile (`AWS_PROFILE`)
3. IAM role (for EC2/Lambda)
4. SSO credentials

The account ID is auto-detected from STS. Override with `AWS_ACCOUNT_ID` if needed.

## Tools Reference

### Datasets (7 tools)

| Tool | Description |
|------|-------------|
| `list_datasets` | List all datasets with name, ID, and import mode |
| `search_datasets` | Search datasets by name (case-insensitive) |
| `get_dataset` | Get full metadata for a dataset (columns, tables, import mode) |
| `get_dataset_sql` | Get the SQL query powering a dataset |
| `update_dataset_sql` | Update dataset SQL with auto-backup |
| `refresh_dataset` | Trigger SPICE refresh |
| `get_refresh_status` | Check SPICE refresh progress |

### Analyses (6 tools)

| Tool | Description |
|------|-------------|
| `list_analyses` | List all analyses with status |
| `search_analyses` | Search analyses by name |
| `describe_analysis` | Get full analysis structure (sheets, visuals, fields) |
| `list_visuals` | List all visuals with types and titles |
| `list_calculated_fields` | List all calculated fields with expressions |
| `get_columns_used` | Get column usage frequency across the analysis |

### Calculated Fields (4 tools)

| Tool | Description |
|------|-------------|
| `add_calculated_field` | Add new calculated field to an analysis |
| `update_calculated_field` | Update a calculated field's expression |
| `delete_calculated_field` | Delete a calculated field |
| `get_calculated_field` | Get details of a specific calculated field |

### Dashboards (5 tools)

| Tool | Description |
|------|-------------|
| `list_dashboards` | List all dashboards |
| `search_dashboards` | Search dashboards by name |
| `get_dashboard_versions` | List version history |
| `publish_dashboard` | Publish dashboard from analysis |
| `rollback_dashboard` | Rollback to a previous version |

### Backup & Restore (3 tools)

| Tool | Description |
|------|-------------|
| `backup_analysis` | Backup analysis definition to JSON |
| `restore_analysis` | Restore analysis from backup file |
| `clone_analysis` | Clone analysis for safe testing |

### Self-Learning (2 tools)

| Tool | Description |
|------|-------------|
| `get_learning_insights` | Show usage patterns and optimization suggestions |
| `get_error_patterns` | Show common errors and their frequencies |

## Production Safety Features

### Auto-Backup

Every destructive operation (update SQL, modify calculated fields) automatically creates a backup before making changes. Backups are saved to `~/.quicksight-mcp/backups/`.

### Optimistic Locking

When modifying an analysis, the server checks that no one else has modified it since you last read it. If there is a conflict, you get a clear error instead of silently overwriting changes.

### Destructive Change Protection

Updates that would delete all sheets, most visuals, or most calculated fields are blocked by default. This prevents accidental data loss from malformed updates.

### Change Verification

After every write operation, the server verifies the change was actually applied. QuickSight's API sometimes returns success but does not apply the change -- this catches those cases.

## Self-Learning Engine

The server learns from your usage patterns and gets smarter over time.

### What It Tracks

- Tool usage frequency and sequences
- Common workflows (e.g., search -> get SQL -> update -> refresh)
- Error patterns and their categories
- Operation durations

### What It Suggests

- Workflow optimizations based on your common patterns
- Caching improvements for frequently accessed resources
- Known fixes for recurring errors
- SQL syntax gotchas specific to QuickSight

### Configuration

```bash
# Enable/disable (default: enabled)
export QUICKSIGHT_MCP_LEARNING=true

# Custom storage directory (default: ~/.quicksight-mcp/)
export QUICKSIGHT_MCP_LEARNING_DIR=/path/to/learning/data
```

All learning data is stored locally. No telemetry is sent anywhere.

## Example Workflows

### Update Dataset SQL

```
User: "Update the WBR dataset SQL to add a new column"

1. search_datasets("WBR")                    -> finds dataset ID
2. get_dataset_sql("abc-123")                -> shows current SQL
3. update_dataset_sql("abc-123", "new SQL")  -> updates with auto-backup
4. refresh_dataset("abc-123")                -> triggers SPICE reload
5. get_refresh_status("abc-123", "ing-456")  -> monitors progress
```

### Add Calculated Field

```
User: "Add a profit margin field to the T&O Sales analysis"

1. search_analyses("T&O Sales")                           -> finds analysis ID
2. describe_analysis("xyz-789")                           -> shows structure + datasets
3. add_calculated_field("xyz-789", "Profit Margin",       -> creates the field
     "({Revenue} - {Cost}) / {Revenue}", "my_dataset")
```

### Safe Testing with Clone

```
User: "I want to test changes without affecting production"

1. clone_analysis("prod-id", "Test Copy")    -> creates isolated copy
2. add_calculated_field("clone-id", ...)     -> make changes on clone
3. [verify changes look correct]
4. [apply same changes to production]
```

### Investigate Dashboard Issues

```
User: "The revenue numbers look wrong on the sales dashboard"

1. search_dashboards("sales")                -> find the dashboard
2. get_dashboard_versions("dash-id")         -> check recent publishes
3. search_analyses("sales")                  -> find source analysis
4. describe_analysis("an-id")                -> inspect structure
5. list_calculated_fields("an-id")           -> check field expressions
6. get_dataset_sql("ds-id")                  -> inspect underlying SQL
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_PROFILE` | (none) | AWS named profile |
| `AWS_REGION` | `us-east-1` | AWS region |
| `AWS_ACCOUNT_ID` | (auto-detect) | QuickSight account ID |
| `QUICKSIGHT_MCP_LEARNING` | `true` | Enable self-learning |
| `QUICKSIGHT_MCP_LEARNING_DIR` | `~/.quicksight-mcp/` | Learning data directory |
| `LOG_LEVEL` | `INFO` | Logging level |

## Architecture

```
quicksight-mcp/
  src/quicksight_mcp/
    server.py              # FastMCP entry point, lazy dependency init
    client.py              # QuickSight API wrapper (boto3)
    exceptions.py          # Structured errors (concurrent mod, verification, destructive)
    tools/
      datasets.py          # 7 dataset tools
      analyses.py          # 6 analysis tools
      calculated_fields.py # 4 calculated field tools
      dashboards.py        # 5 dashboard tools
      backup.py            # 3 backup/restore tools
      learning.py          # 2 self-learning tools
    learning/
      tracker.py           # Usage pattern recording
      optimizer.py         # Recommendation engine
      knowledge.py         # Local key-value knowledge store
```

The server uses **lazy initialization** -- the AWS client and learning engine are only created when the first tool call arrives, keeping startup instant.

Each tool module exposes a `register_*_tools(mcp, get_client, get_tracker)` function that attaches `@mcp.tool` handlers to the FastMCP server instance. This keeps the code modular and testable.

## Development

```bash
git clone https://github.com/krishna-goje/quicksight-mcp.git
cd quicksight-mcp
pip install -e ".[dev]"
pytest
```

### Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=quicksight_mcp

# Specific module
pytest tests/test_learning.py
```

### Linting

```bash
ruff check src/ tests/
ruff format src/ tests/
```

## Contributing

Contributions are welcome. Please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run the tests (`pytest`)
4. Commit your changes
5. Open a pull request

## License

Apache 2.0 -- see [LICENSE](LICENSE) for details.
