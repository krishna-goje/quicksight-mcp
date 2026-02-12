# QuickSight MCP Server

The most comprehensive AWS QuickSight MCP server -- with self-learning capability, chart builders, and built-in QA verification.

[![PyPI version](https://badge.fury.io/py/quicksight-mcp.svg)](https://pypi.org/project/quicksight-mcp/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Why This Server?

Other QuickSight MCP servers are either auto-generated API wrappers or limited to lineage queries. This server is extracted from a 4,800+ line production library, wrapping battle-tested patterns into **55 MCP tools**.

**Key Differentiators:**

- **55 purpose-built tools** covering the full developer workflow: read, build, edit, verify, publish
- **Chart builders** that create visuals from simple parameters (column + aggregation) -- no raw JSON needed
- **QA system** with snapshot/diff to compare before and after any change
- **Post-write verification** on every operation -- catches QuickSight's silent failures
- **Self-learning engine** that tracks usage patterns and suggests optimizations
- **Production safety** with auto-backup, optimistic locking, destructive change protection

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

## Tools Reference (55 tools)

### Datasets (8 tools)

| Tool | Description |
|------|-------------|
| `list_datasets` | List all datasets with name, ID, and import mode |
| `search_datasets` | Search datasets by name (case-insensitive) |
| `get_dataset` | Get full metadata for a dataset (columns, tables, import mode) |
| `get_dataset_sql` | Get the SQL query powering a dataset |
| `update_dataset_sql` | Update dataset SQL with auto-backup and verification |
| `refresh_dataset` | Trigger SPICE refresh |
| `get_refresh_status` | Check SPICE refresh progress |
| `list_recent_refreshes` | Get refresh history for a dataset |

### Analysis Inspection (12 tools)

| Tool | Description |
|------|-------------|
| `list_analyses` | List all analyses with status |
| `search_analyses` | Search analyses by name |
| `describe_analysis` | Get full structure: sheets, visuals, fields, parameters, datasets |
| `list_visuals` | List all visuals with types, titles, and sheet locations |
| `list_calculated_fields` | List all calculated fields with expressions |
| `get_columns_used` | Get column usage frequency across the analysis |
| `get_parameters` | List all parameter declarations |
| `get_filters` | List all filter groups with scope and conditions |
| `get_analysis_raw` | Get the complete raw analysis definition for inspection |
| `verify_analysis_health` | Run comprehensive health check (status, layouts, refs) |
| `snapshot_analysis` | Capture current state as baseline for QA |
| `diff_analysis` | Compare current state against a snapshot |

### Chart Builders (5 tools)

Create visuals from simple parameters -- no raw JSON needed.

| Tool | Description |
|------|-------------|
| `create_kpi` | Create a KPI from column + aggregation |
| `create_bar_chart` | Create bar chart with category + value columns |
| `create_line_chart` | Create line chart with date + value + granularity |
| `create_pivot_table` | Create pivot table with row/value columns |
| `create_table` | Create flat table with column list |

### Visual Management (5 tools)

| Tool | Description |
|------|-------------|
| `get_visual_definition` | Get the full raw definition of any visual |
| `add_visual` | Add a visual from JSON definition (for advanced use) |
| `delete_visual` | Delete a visual with layout cleanup |
| `set_visual_title` | Update a visual's display title |
| `set_visual_layout` | Set visual position and size on the grid |

### Sheet Management (5 tools)

| Tool | Description |
|------|-------------|
| `add_sheet` | Add a new sheet to an analysis |
| `delete_sheet` | Delete a sheet and its visuals |
| `rename_sheet` | Rename an existing sheet |
| `list_sheet_visuals` | List all visuals on a specific sheet |
| `replicate_sheet` | Copy entire sheet with all visuals (batch, single API call) |

### Calculated Fields (4 tools)

| Tool | Description |
|------|-------------|
| `add_calculated_field` | Add new calculated field to an analysis |
| `update_calculated_field` | Update a calculated field's expression |
| `delete_calculated_field` | Delete a calculated field |
| `get_calculated_field` | Get details of a specific calculated field |

### Parameters & Filters (4 tools)

| Tool | Description |
|------|-------------|
| `add_parameter` | Add a parameter (string, integer, date, decimal) |
| `delete_parameter` | Delete a parameter by name |
| `add_filter_group` | Add a filter group with scope configuration |
| `delete_filter_group` | Delete a filter group |

### Dashboards (5 tools)

| Tool | Description |
|------|-------------|
| `list_dashboards` | List all dashboards |
| `search_dashboards` | Search dashboards by name |
| `get_dashboard_versions` | List version history |
| `publish_dashboard` | Publish dashboard from analysis |
| `rollback_dashboard` | Rollback to a previous version |

### Backup & Restore (4 tools)

| Tool | Description |
|------|-------------|
| `backup_analysis` | Backup analysis definition to JSON |
| `backup_dataset` | Backup dataset definition to JSON |
| `restore_analysis` | Restore analysis from backup file |
| `clone_analysis` | Clone analysis for safe testing |

### Self-Learning (2 tools)

| Tool | Description |
|------|-------------|
| `get_learning_insights` | Show usage patterns and optimization suggestions |
| `get_error_patterns` | Show common errors and their frequencies |

## Developer Workflow

The server supports the full build-verify-publish cycle:

### 1. Build: Create Visuals from Simple Parameters

```
"Add a KPI showing total contracts to the SLA sheet"

create_kpi(
    analysis_id = "abc-123",
    sheet_id    = "sheet-456",
    title       = "Total Contracts",
    column      = "FLIP_TOKEN",
    aggregation = "COUNT",
    dataset_identifier = "acq_l2_flip_details"
)
→ Returns: {visual_id: "kpi_50ed988920b4", status: "UPDATE_SUCCESSFUL"}
```

```
"Add a weekly trend line chart"

create_line_chart(
    analysis_id = "abc-123",
    sheet_id    = "sheet-456",
    title       = "Weekly Contract Trend",
    date_column = "PURCHASE_AGREEMENT_COMPLETED_AT",
    value_column = "FLIP_TOKEN",
    value_aggregation = "COUNT",
    dataset_identifier = "acq_l2_flip_details",
    date_granularity = "WEEK"
)
```

```
"Add a market breakdown pivot table"

create_pivot_table(
    analysis_id = "abc-123",
    sheet_id    = "sheet-456",
    title       = "Market Breakdown",
    row_columns = "MARKET_NAME,ASSESSMENT_TYPE",
    value_columns = "FLIP_TOKEN,REVENUE",
    value_aggregations = "COUNT,SUM",
    dataset_identifier = "acq_l2_flip_details"
)
```

### 2. Verify: QA with Snapshot and Diff

```
# Before making changes -- capture baseline
snapshot_analysis("abc-123")
→ Returns: {snapshot_id: "snap_20260212_193448", visuals: 185, sheets: 20}

# Make your changes...
create_kpi(...)
create_bar_chart(...)

# After changes -- verify what changed
diff_analysis("abc-123", "snap_20260212_193448")
→ Returns: {
    visuals_added: [
      {type: "KPI", title: "Total Contracts"},
      {type: "BarChart", title: "Contracts by Market"}
    ],
    visuals_removed: [],
    visual_changes: [],
    old_visual_count: 185,
    new_visual_count: 187
  }

# Health check -- ensure nothing broke
verify_analysis_health("abc-123")
→ Returns: {
    healthy: true,
    checks: [
      {check: "analysis_status", ok: true},
      {check: "sheet_count", ok: true, count: 20},
      {check: "visual_layout_alignment", ok: true},
      {check: "calc_field_dataset_refs", ok: true}
    ]
  }
```

### 3. Publish: Push to Dashboard

```
# Publish when ready
publish_dashboard("dash-id", "abc-123", "Added KPI and bar chart for contracts")

# Rollback if something goes wrong
rollback_dashboard("dash-id", version_number=5)
```

### Other Common Workflows

**Update Dataset SQL:**
```
search_datasets("WBR")                    → find dataset ID
get_dataset_sql("ds-123")                 → view current SQL
update_dataset_sql("ds-123", "new SQL")   → update with auto-backup
refresh_dataset("ds-123")                 → trigger SPICE reload
get_refresh_status("ds-123", "ing-456")   → monitor progress
```

**Replicate an Entire Sheet:**
```
describe_analysis("abc-123")                          → find source sheet ID
replicate_sheet("abc-123", "sheet-456", "My Copy")    → copies all visuals + layouts
```

**Safe Testing with Clone:**
```
clone_analysis("prod-id", "Test Copy")    → creates isolated copy
create_kpi("clone-id", ...)               → make changes on clone
verify_analysis_health("clone-id")        → verify
diff_analysis("clone-id", snapshot_id)    → review changes
[apply same changes to production]
```

## Production Safety Features

### Post-Write Verification

Every write operation verifies its changes actually persisted:

- `add_sheet` verifies the sheet exists with the correct name
- `delete_sheet` verifies the sheet was actually removed
- `create_kpi` / `create_bar_chart` / etc. verify the visual exists
- `set_visual_title` verifies the title matches
- `add_parameter` verifies the parameter exists
- `replicate_sheet` verifies both the sheet and the visual count

This catches QuickSight's silent failures where the API returns `200 OK` but doesn't apply the change.

### Auto-Backup

Every write operation automatically creates a timestamped JSON backup before making changes. Backups are saved to `~/.quicksight-mcp/backups/`.

### Optimistic Locking

When modifying an analysis, the server checks that no one else has modified it since you last read it. Prevents silently overwriting concurrent changes.

### Destructive Change Protection

Updates that would delete all sheets, most visuals, or most calculated fields are blocked. Prevents accidental data loss from malformed definitions.

### Failed Analysis Guard

The server refuses to update an analysis that is in FAILED status, preventing cascading failures on corrupted definitions. Restore from backup first.

### QuickSight Limits

The server enforces QuickSight's 20-sheet-per-analysis limit, providing clear error messages instead of cryptic API failures.

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

All learning data is stored locally. No telemetry is sent anywhere.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_PROFILE` | (none) | AWS named profile |
| `AWS_REGION` | `us-east-1` | AWS region |
| `AWS_ACCOUNT_ID` | (auto-detect) | QuickSight account ID |
| `QUICKSIGHT_BACKUP_DIR` | `~/.quicksight-mcp/backups` | Backup directory |
| `QUICKSIGHT_MCP_LEARNING` | `true` | Enable self-learning |
| `QUICKSIGHT_MCP_LEARNING_DIR` | `~/.quicksight-mcp/` | Learning data directory |
| `LOG_LEVEL` | `INFO` | Logging level |

## Architecture

```
quicksight-mcp/
  src/quicksight_mcp/
    server.py              # FastMCP entry point, lazy dependency init
    client.py              # QuickSight API wrapper with safety features
    exceptions.py          # Structured errors
    tools/
      datasets.py          # 8 dataset tools
      analyses.py          # 12 analysis + QA tools
      visuals.py           # 10 visual + chart builder tools
      sheets.py            # 5 sheet management tools
      calculated_fields.py # 4 calculated field tools
      parameters.py        # 2 parameter tools
      filters.py           # 2 filter tools
      dashboards.py        # 5 dashboard tools
      backup.py            # 4 backup/restore tools
      learning.py          # 2 self-learning tools
    learning/
      tracker.py           # Usage pattern recording
      optimizer.py         # Recommendation engine
      knowledge.py         # Local key-value knowledge store
```

The server uses **lazy initialization** -- the AWS client and learning engine are only created when the first tool call arrives, keeping startup instant.

Each tool module exposes a `register_*_tools(mcp, get_client, get_tracker)` function that attaches `@mcp.tool` handlers to the FastMCP server instance.

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

## Author

**Krishna Goje** -- Staff Analytics Engineer | AI-Augmented Engineering

- Portfolio: [krishna-goje.github.io](https://krishna-goje.github.io)
- LinkedIn: [linkedin.com/in/krishnagoje](https://www.linkedin.com/in/krishnagoje)
- GitHub: [github.com/krishna-goje](https://github.com/krishna-goje)
- Email: krishna19.gk@gmail.com

### Background

14 years building data platforms at American Express, Grubhub, Booking.com, and Opendoor. Pioneered an AI-augmented engineering ecosystem connecting 15+ enterprise platforms with parallel agent orchestration and self-learning feedback loops.

### Why This Project

This server is extracted from a 4,800+ line production library built over months of daily QuickSight work. Every safety feature exists because of a real production incident:

- **Auto-backup** -- an update once wiped an analysis with no way to undo
- **Optimistic locking** -- two sessions editing the same analysis silently overwrote each other
- **Change verification** -- QuickSight's API returns `200 OK` but sometimes doesn't apply the change
- **Destructive change protection** -- a malformed definition update deleted all sheets from a live dashboard
- **Failed status guard** -- updating a FAILED analysis caused cascading corruption

### Open Source

- [quicksight-mcp](https://github.com/krishna-goje/quicksight-mcp) -- MCP server for AWS QuickSight management
- [slack-data-bot](https://github.com/krishna-goje/slack-data-bot) -- Autonomous data Q&A bot with Writer/Reviewer quality loop

## Contributing

Contributions are welcome. Please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run the tests (`pytest`)
4. Commit your changes
5. Open a pull request

## License

Apache 2.0 -- see [LICENSE](LICENSE) for details.
