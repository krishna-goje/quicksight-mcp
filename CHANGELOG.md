# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-12

### Added

- Initial release with 27 MCP tools for AWS QuickSight
- **Dataset tools** (7): list, search, get metadata, get SQL, update SQL, refresh SPICE, check refresh status
- **Analysis tools** (6): list, search, describe structure, list visuals, list calculated fields, get column usage
- **Calculated field tools** (4): add, update, delete, get details
- **Dashboard tools** (5): list, search, version history, publish, rollback
- **Backup tools** (3): backup analysis, restore analysis, clone analysis
- **Self-learning tools** (2): usage insights, error patterns
- Self-learning engine with usage tracking, workflow detection, and optimization suggestions
- Production safety features:
  - Auto-backup before destructive operations
  - Optimistic locking for concurrent modification detection
  - Destructive change protection (blocks deletion of sheets/visuals/fields)
  - Post-write change verification
- Custom exception types: `ConcurrentModificationError`, `ChangeVerificationError`, `DestructiveChangeError`
- Standard AWS credential chain authentication with auto-detected account ID
- Local learning data storage (no telemetry)
- PyPI packaging with `quicksight-mcp` CLI entry point
- Apache 2.0 license
