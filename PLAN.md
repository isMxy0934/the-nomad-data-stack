# PLAN

## Milestone 1: Core ingestion runner + DuckDB compactor
- [x] Add core ingestion package (partitioners/extractors/compactors/runner)
- [x] Add CLI runner for ingestion

## Milestone 2: Orchestration wrapper
- [x] Update ingestion DAG to call the core runner
- [x] Point ingestion configs to core components

## Milestone 3: Validation and documentation
- [x] Add unit tests for runner and compactor
- [x] Add validation script for local runner
- [x] Update docs to reflect the new ingestion flow
