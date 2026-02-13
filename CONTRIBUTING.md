# Contributing to QuickSight MCP Server

Thank you for your interest in contributing! This document outlines the development workflow and best practices.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before participating. For security issues, see [SECURITY.md](SECURITY.md).

## Reporting Issues

Before writing code, check [existing issues](https://github.com/krishna-goje/quicksight-mcp/issues) first.

- **Bugs**: Include the error message, Python version, and the tool/method called
- **Features**: Describe the use case and expected behavior
- **Questions**: Open a discussion or issue with the `question` label

## Prerequisites

- Python 3.10 or higher
- AWS credentials configured (for running the server locally)
- `gh` CLI recommended for PR workflows

## Branch Strategy

```
master (protected)        <-- production releases only, via PR
  └── feature/xxx         <-- all development work happens here
```

### Rules

- **Never push directly to `master`** -- all changes go through pull requests
- **Feature branches** are created from `master` and merged back via PR
- **Branch naming**: `feature/<description>`, `fix/<description>`, `docs/<description>`
- **Squash merge** PRs to keep `master` history clean
- **CI must pass** before merge (lint + tests run automatically on every PR)

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
git checkout master
git pull origin master
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
# Lint (must pass before PR)
ruff check src/ tests/

# Run tests
pytest

# Verify server loads without errors
python -c "from quicksight_mcp.server import mcp; print('Server OK')"
```

### 5. Commit

```bash
git add <specific-files>
git commit -m "Add combo chart builder with field configuration

- Accepts simple params: category, bar_values, line_values
- Auto-generates visual ID and layout element
- Post-write verification included
- Tested on clone analysis"
```

Commit message guidelines:
- First line: imperative mood, under 72 chars
- Body: explain *what* and *why*, not *how*
- Reference issue numbers if applicable (e.g., `Fixes #12`)

### 6. Push and Create PR

```bash
git push origin feature/add-combo-chart-builder
gh pr create --title "Add combo chart builder" --body "## Summary
- New tool: create_combo_chart
- Accepts simple parameters for bar + line values

## Test plan
- [ ] Lint passes (ruff check)
- [ ] pytest passes
- [ ] Tool registers correctly (count increased)
- [ ] create_combo_chart works on test analysis
- [ ] verify_analysis_health passes after creation
- [ ] diff_analysis shows the new visual"
```

### 7. Handling Common Issues

**Tests fail locally**: Fix before pushing. Do not open a PR with failing tests.

**PR has merge conflicts**: Rebase onto the latest master:
```bash
git fetch origin
git rebase origin/master
# Resolve conflicts, then:
git push --force-with-lease
```

**Reviewer requests changes**: Push additional commits to the same branch. The PR updates automatically. Do not force-push after review has started unless asked.

**CI fails after push**: Check the Actions tab on GitHub. Fix the issue locally and push a new commit.

### 8. PR Review Checklist

Before merging, verify:

- [ ] CI passes (lint + tests)
- [ ] All new write methods have post-write verification
- [ ] All new write methods have `backup_first=True` default
- [ ] MCP tools follow the `start-time / try-except / tracker` pattern
- [ ] No secrets: AWS keys, account IDs, session tokens, or production resource IDs
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
4. After merge, tag and push:
   ```bash
   git tag -a v0.x.0 -m "v0.x.0: <summary>"
   git push origin v0.x.0
   ```
5. GitHub Actions automatically builds and publishes to PyPI (see `.github/workflows/publish.yml`)
