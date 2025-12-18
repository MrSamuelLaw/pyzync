# Agent Guidelines for Pyzync

## Build, Lint, and Test Commands

- **Install dependencies**: `uv sync`
- **Run all tests**: `python -m unittest discover tests`
- **Run a single test**: `python -m unittest tests.test_<module>.<TestClass>.<test_method>`
  - Example: `python -m unittest tests.test_retention_policy.TestRetentionPolicy.test_policy`
- **Lint**: `ruff check .` (uses ruff v0.14.0+)
- **Format**: `yapf -r -i .` (uses .style.yapf config)

## Architecture and Codebase Structure

**Main Package**: `pyzync/` - Core ZFS backup management library

**Key Modules**:
- `interfaces.py` - Core domain models: `SnapshotNode`, `SnapshotGraph`, `SnapshotChain`, `Datetime`, custom ZFS types
- `backup.py` - `BackupJob` orchestrates snapshots, retention policies, and storage adapters
- `host.py` - `HostSnapshotManager` interacts with local ZFS system
- `streaming.py` - `SnapshotStreamManager` handles snapshot data transfer
- `retention/` - Pluggable retention policies (interface in `interfaces.py`)
- `storage/` - Pluggable storage adapters (interface in `interfaces.py`)
- `logging.py` / `otel.py` - Structured logging and OpenTelemetry tracing

**Test Structure**: `tests/` contains unit tests; test utilities and fixtures follow module structure

## Code Style Guidelines

**Style**: Google style (YAPF-based), 105 character line limit

**Imports**: Standard library → Pydantic → project imports, grouped with blank lines

**Types**: Heavy use of Pydantic v2+ `BaseModel` for validation and serialization; custom types inherit from primitives (e.g., `ZfsDatasetId(str)`)

**Naming**: PascalCase for classes, snake_case for functions/variables; custom domain types are `Zfs*` prefixed

**Error Handling**: Raise domain-specific exceptions (check `errors.py`); log exceptions with context before re-raising

**Validation**: Use Pydantic validators (`@model_validator`, `@validate_call`) and custom `__new__` methods for type coercion

**Async**: Use `asyncio` for I/O-bound operations (snapshot streaming, remote adapter calls)
