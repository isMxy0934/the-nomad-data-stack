# `lakehouse_core` Public API

This project treats `lakehouse_core` as an orchestrator-agnostic core. Airflow/Prefect/scripts should integrate via the stable imports from `lakehouse_core` (package root) and avoid depending on internal modules unless necessary.

## Stable Imports (Recommended)

- Planning: `DirectoryDWPlanner`, `RunContext`, `RunSpec`
- Storage: `ObjectStore`, `Boto3S3Store`, `LocalFileStore`
- Inputs: `InputRegistrar`, `OdsCsvRegistrar`
- Pipeline stages (fixed order): `pipeline.load`, `pipeline.validate`, `pipeline.commit`, `pipeline.cleanup`
- Path planning: `lakehouse_core.api.prepare_paths` (or `pipeline.prepare` if you want a JSON/XCom-friendly payload)

## Orchestrator Pattern (Five-Stage Chain)

The core write protocol is a fixed chain:

1. `prepare` – compute canonical/tmp prefixes for a `RunSpec` and run id (`api.prepare_paths` / `pipeline.prepare`)
2. `load` – register inputs, render SQL, materialize to tmp, return `(row_count,file_count,has_data)`
3. `validate` – validate tmp output against metrics
4. `commit` – publish tmp to canonical (delete→copy) and write markers (unless `has_data=0`)
5. `cleanup` – delete tmp prefix

Airflow should keep these as separate tasks for observability and retries, but each task should call the corresponding `lakehouse_core.pipeline` function (via the stable root imports).

## Empty Data Semantics

When `has_data=0`:

- `commit` clears the canonical prefix to avoid stale data
- `commit` does **not** write `manifest.json` or `_SUCCESS` (downstream treats missing `_SUCCESS` as "no data")
