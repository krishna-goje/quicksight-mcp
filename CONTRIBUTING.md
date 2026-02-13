# Contributing to QuickSight MCP Server

Thank you for your interest in contributing! This document outlines the development workflow and best practices.

## Branch Strategy

```
main (protected)          ← production releases only, via PR
  └── feature/xxx         ← all development work happens here
```

### Rules

- **Never push directly to `main`** -- all changes go through pull requests
- **Feature branches** are created from `main` and merged back via PR
- **Branch naming**: `feature/<description>`, `fix/<description>`, `docs/<description>`
- **Squash merge** PRs to keep `main` history clean

## Development Workflow

### 1. Set Up

```bash
git clone https://github.com/krishna-goje/quicksight-mcp.git
cd quicksight-mcp
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Create a Feature Branch

```bash
git checkout main
git pull origin main
git checkout -b feature/add-combo-chart-builder
```

### 3. Make Changes

Follow these patterns:

- **Client methods** go in `src/quicksight_mcp/client.py`
- **MCP tool wrappers** go in `src/quicksight_mcp/tools/<domain>.py`
- **New tool modules** must be registered in `src/quicksight_mcp/server.py`
- Every write method must include **post-write verification**
- Every write method must support **backup_first=True** by default

### 4. Test

```bash
# Syntax check
python -c "import ast; ast.parse(open('src/quicksight_mcp/client.py').read())"

# Run tests
pytest

# Verify tool count
python -c "
from quicksight_mcp.server import mcp
print(f'Tools: {len(mcp._tool_manager._tools)}')
"
```

### 5. Commit

```bash
git add <specific-files>
git commit -m "Add combo chart builder with field configuration

- Accepts simple params: category, bar_values, line_values
- Auto-generates visual ID and layout element
- Post-write verification included
- Tested on T&O Homes clone analysis"
```

Commit message guidelines:
- First line: imperative mood, under 72 chars
- Body: explain *what* and *why*, not *how*
- Reference issue numbers if applicable

### 6. Push and Create PR

```bash
git push origin feature/add-combo-chart-builder
gh pr create --title "Add combo chart builder" --body "## Summary
- New tool: create_combo_chart
- Accepts simple parameters for bar + line values

## Test plan
- [ ] Syntax verification passes
- [ ] Tool registers correctly (count increased)
- [ ] create_combo_chart works on test analysis
- [ ] verify_analysis_health passes after creation
- [ ] diff_analysis shows the new visual"
```

### 7. PR Review Checklist

Before merging, verify:

- [ ] All new write methods have post-write verification
- [ ] All new write methods have `backup_first=True` default
- [ ] MCP tools follow the `start-time / try-except / tracker` pattern
- [ ] No hardcoded AWS account IDs or credentials
- [ ] CHANGELOG.md updated
- [ ] README.md updated if new tools added
- [ ] Tool count in README matches actual count

## Code Patterns

### Adding a New Chart Builder

1. Add the client method to `client.py`:
```python
def create_<type>(self, analysis_id, sheet_id, title, ...):
    definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
    visual_id = f'<type>_{uuid.uuid4().hex[:12]}'
    # ... construct visual definition ...
    self._append_visual_to_sheet(definition, sheet_id, visual_def, visual_id)
    result = self.update_analysis(analysis_id, definition, backup_first=backup_first, ...)
    if self._should_verify(None):
        self._verify_visual_exists(analysis_id, visual_id)
    result['visual_id'] = visual_id
    return result
```

2. Add the MCP tool wrapper to `tools/visuals.py`:
```python
@mcp.tool
def create_<type>(analysis_id: str, sheet_id: str, title: str, ...) -> dict:
    start = time.time()
    client = get_client()
    try:
        result = client.create_<type>(...)
        get_tracker().record_call("create_<type>", {...}, ...)
        return {"status": "success", "visual_id": result.get("visual_id"), ...}
    except Exception as e:
        get_tracker().record_call("create_<type>", {...}, ..., False, str(e))
        return {"error": str(e)}
```

### Adding a New Write Operation

Every write operation MUST:

1. Call `get_analysis_definition_with_version()` (optimistic locking)
2. Call `update_analysis()` with `backup_first=True`
3. Call the appropriate `_verify_*()` method after success
4. Return a structured result dict

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` with new version section
3. Create PR: `release/v0.x.0`
4. After merge, tag: `git tag -a v0.x.0 -m "v0.x.0: <summary>"`
5. Push tag: `git push origin v0.x.0`
6. Build and publish: `python -m build && twine upload dist/*`
