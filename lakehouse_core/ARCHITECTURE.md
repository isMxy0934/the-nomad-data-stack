# Lakehouse Core Architecture

## Overview

This document defines the architecture principles and layer responsibilities for the data stack project. The architecture follows a three-layer design to ensure the core business logic (`lakehouse_core/`) remains stable and orchestrator-agnostic.

## Layer Responsibilities

### Bottom Layer (lakehouse_core/)

**Responsibility**: Stable, orchestrator-agnostic business logic

**Allowed Dependencies**:
- Python standard library
- Third-party libraries (duckdb, boto3, pandas, etc.)
- Internal lakehouse_core modules

**Prohibited Dependencies**:
- ❌ Airflow (airflow.*)
- ❌ Any orchestrator-specific libraries (prefect, dagster, etc.)
- ❌ dags.* modules

**Key Principles**:
1. **Protocol-based abstractions**: All functions accept Protocol types (e.g., `ObjectStore`, not `AirflowS3Store`)
2. **No orchestrator context**: Use `RunContext` instead of Airflow context objects
3. **No orchestrator-specific formats**: Configuration and serialization must be orchestrator-agnostic
4. **Complete serialization logic**: Both serialization and deserialization live here

**Public API** (`lakehouse_core/__init__.py`):
- Core models: `RunContext`, `RunSpec`
- Pipeline stages: `pipeline.*`
- Storage: `ObjectStore`, `Boto3S3Store`, `LocalFileStore`
- Paths: `paths.*`, `dict_to_paths`, `paths_to_dict`
- Planning: `DirectoryDWPlanner`, `DWConfig`

### Adapter Layer (dags/adapters/)

**Responsibility**: Convert orchestrator-specific types to lakehouse_core types

**Allowed Dependencies**:
- Airflow libraries
- lakehouse_core.* (public API only)
- Python standard library

**Prohibited Dependencies**:
- ❌ dags.utils.* (except constants)
- ❌ Business logic (should delegate to lakehouse_core)

**Key Patterns**:
1. **Configuration adapters**: Convert Airflow Connection → lakehouse_core config types
   - Example: `build_s3_connection_config(s3_hook) → S3ConnectionConfig`
2. **Context adapters**: Convert Airflow context → lakehouse_core context types
   - Example: `build_run_context_from_airflow(context) → RunContext`
3. **Store adapters**: Implement lakehouse_core Protocols using orchestrator hooks
   - Example: `AirflowS3Store` implements `ObjectStore` using `S3Hook`

**Files**:
- `airflow_s3_store.py`: ObjectStore implementation using S3Hook
- `airflow_config.py`: Configuration conversion functions
- `airflow_context.py`: Context conversion functions

### Top Layer (dags/)

**Responsibility**: Airflow-specific orchestration and DAG generation

**Allowed Dependencies**:
- Airflow libraries
- dags.adapters.*
- lakehouse_core.* (public API only)
- dags.utils.* (for DAG-specific utilities)

**Prohibited**:
- ❌ Direct use of lakehouse_core internals (use public API)
- ❌ Business logic (delegate to lakehouse_core)
- ❌ Type conversions (use adapters)

**Key Patterns**:
1. **DAG factories**: Generate DAGs from configuration files
2. **Task adapters**: Bridge XCom ↔ lakehouse_core functions
3. **Orchestration**: Define task dependencies and data flow

## Dependency Flow

```
┌─────────────────────────────────────┐
│   Top Layer (dags/)                 │
│   - DAG generation                  │
│   - Task orchestration              │
│   - Airflow-specific logic          │
└──────────────┬──────────────────────┘
               │ depends on
               ↓
┌─────────────────────────────────────┐
│   Adapter Layer (dags/adapters/)    │
│   - Type conversions                │
│   - Protocol implementations        │
│   - Orchestrator isolation          │
└──────────────┬──────────────────────┘
               │ depends on
               ↓
┌─────────────────────────────────────┐
│   Bottom Layer (lakehouse_core/)    │
│   - Business logic                  │
│   - Stable API                      │
│   - Orchestrator-agnostic           │
└─────────────────────────────────────┘
```

## Testing Strategy

### Bottom Layer Tests
- **Unit tests** with no Airflow dependencies
- **Mock implementations** of ObjectStore for testing
- **LocalFileStore** for filesystem-based tests
- **Test isolation**: Each test should be independent

### Adapter Layer Tests
- **Unit tests** with Airflow test fixtures
- **Mock Airflow connections** and hooks
- **Verify type conversions** are correct
- **Test error handling** for invalid configurations

### Top Layer Tests
- **Integration tests** with Airflow DAG testing framework
- **End-to-end tests** with test DAGs
- **Verify orchestration logic** and task dependencies

## Migration Guide

### For New Features

When adding new features, follow this workflow:

1. **Implement core logic in lakehouse_core/**
   - Define business logic without orchestrator dependencies
   - Use Protocol types for external dependencies
   - Add unit tests

2. **Create adapters in dags/adapters/** (if needed)
   - Convert orchestrator-specific types to lakehouse_core types
   - Implement Protocols using orchestrator hooks
   - Add adapter tests

3. **Wire up in DAGs** using adapters
   - Use adapters to convert types
   - Call lakehouse_core functions
   - Define task dependencies

### For Existing Code

When refactoring existing code:

1. **Identify orchestrator-specific logic** in lakehouse_core/
   - Look for Airflow imports
   - Look for orchestrator-specific types

2. **Move to dags/adapters/**
   - Extract conversion logic
   - Create adapter functions
   - Update imports

3. **Update tests**
   - Separate unit tests (lakehouse_core) from integration tests (dags)
   - Add adapter tests

## Code Review Checklist

Before merging code, verify:

- [ ] **No Airflow imports in lakehouse_core/**
  - Check: `grep -r "from airflow" lakehouse_core/`
  - Check: `grep -r "import airflow" lakehouse_core/`

- [ ] **All lakehouse_core functions use Protocol types**
  - Example: `store: ObjectStore` not `store: AirflowS3Store`

- [ ] **Adapters properly convert types**
  - Verify adapter functions have correct signatures
  - Verify error handling for invalid inputs

- [ ] **DAGs use public lakehouse_core API only**
  - Check imports: should be from `lakehouse_core` not `lakehouse_core.internal.*`

- [ ] **Tests follow layer-specific patterns**
  - lakehouse_core tests: no Airflow dependencies
  - Adapter tests: use Airflow test fixtures
  - DAG tests: use Airflow DAG testing framework

- [ ] **Documentation is updated**
  - Update docstrings
  - Update this ARCHITECTURE.md if needed

## Common Patterns

### Pattern 1: Adding a New Data Source

```python
# 1. Define in lakehouse_core (orchestrator-agnostic)
class InputRegistrar(Protocol):
    def register(self, spec: RunSpec, connection, store: ObjectStore, ...) -> InputRegistration:
        ...

# 2. Implement in lakehouse_core
class NewSourceRegistrar:
    def register(self, spec: RunSpec, connection, store: ObjectStore, ...) -> InputRegistration:
        # Business logic here
        ...

# 3. Use in DAGs (no adapter needed if using existing types)
registrars = [NewSourceRegistrar()]
pipeline_load(..., registrars=registrars)
```

### Pattern 2: Adding Orchestrator-Specific Configuration

```python
# 1. Define lakehouse_core type (orchestrator-agnostic)
@dataclass
class NewConfig:
    param1: str
    param2: int

# 2. Create adapter (orchestrator-specific)
# dags/adapters/airflow_config.py
def build_new_config_from_airflow(airflow_conn: Connection) -> NewConfig:
    return NewConfig(
        param1=airflow_conn.extra_dejson.get("param1"),
        param2=int(airflow_conn.extra_dejson.get("param2", 0)),
    )

# 3. Use in DAGs
config = build_new_config_from_airflow(connection)
lakehouse_core_function(config=config)
```

### Pattern 3: Adding a New Pipeline Stage

```python
# 1. Implement in lakehouse_core/pipeline/
def new_stage(
    *,
    spec: RunSpec,
    paths: PartitionPaths | NonPartitionPaths,
    store: ObjectStore,
    ...
) -> dict[str, int]:
    # Business logic here
    ...

# 2. Export in lakehouse_core/__init__.py
from lakehouse_core.pipeline import new_stage

# 3. Use in DAGs
from lakehouse_core.pipeline import new_stage

result = new_stage(spec=spec, paths=paths, store=store, ...)
```

## Enforcement

### Automated Checks

This architecture is enforced through:

1. **Pre-commit hooks** (`.pre-commit-config.yaml`)
   - Check for prohibited imports in lakehouse_core/
   - Run on every commit

2. **CI/CD pipeline**
   - Run import checks
   - Run layer-specific tests
   - Verify no cross-layer violations

3. **Code review**
   - Use checklist above
   - Verify adherence to patterns

### Manual Review

During code review, ask:

1. **Is this in the right layer?**
   - Business logic → lakehouse_core
   - Type conversion → adapters
   - Orchestration → dags

2. **Are dependencies correct?**
   - Check import statements
   - Verify no prohibited dependencies

3. **Is the abstraction appropriate?**
   - Use Protocols for external dependencies
   - Avoid leaking orchestrator types

## Benefits

Following this architecture provides:

1. **Portability**: Easy to switch orchestrators (Airflow → Prefect → Dagster)
2. **Testability**: Core logic can be tested without orchestrator
3. **Maintainability**: Clear separation of concerns
4. **Stability**: Core library changes less frequently
5. **Reusability**: Core logic can be used in scripts, notebooks, etc.

## Evolution

This architecture will evolve as the project grows:

- **Phase 1** (Current): Establish layer boundaries
- **Phase 2** (Future): Add more orchestrator adapters (Prefect, Dagster)
- **Phase 3** (Future): Extract lakehouse_core as standalone library

## Questions?

If you have questions about this architecture:

1. Check this document first
2. Look at existing code for patterns
3. Ask in code review
4. Update this document with clarifications
