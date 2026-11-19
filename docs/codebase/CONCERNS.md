# Concerns

## Verified implementation concerns

1. `src/main.rs` combines startup, logging, font discovery, GUI layout, worker lifecycle, search orchestration, and result-tree rendering, so UI/startup changes have a broad change surface (`src/main.rs:1-861`).
2. A large commented legacy CLI/progress implementation remains in `src/main.rs:143-286`; it is not part of the active GUI path and can obscure the current execution model.
3. Fixed algorithm/storage invariants are spread across modules: target score `447`, maximum prime `1579`, maximum depth `249`, display limit `30`, beam policy version, and 210 shards (`src/config.rs:1-8`, `src/search.rs:17-33`, `src/rocksdb_output.rs:21-39`).
4. `ShiftPathStore::create` opens 210 RocksDB instances and shares a 32 MiB cache and 128 MiB write buffer (`src/rocksdb_output.rs:129-204`), which is a significant resource footprint.
5. Existing single-DB and 256-shard layouts are rejected without migration (`src/rocksdb_output.rs:206-227`); operational changes require a backup/move procedure.
6. There is no tracked CI workflow, so the repository provides no automated evidence that format, tests, Clippy, and release builds run remotely.

## Maintenance risks

- Several module boundaries use `String` errors, which loses structured categories (`src/search.rs`, `src/rocksdb_output.rs`). No typed-error migration decision is recorded.
- Checkpoint and RocksDB compatibility depends on configuration hashes and `BEAM_POLICY_VERSION`; algorithm changes must review resume behavior and persisted data together (`src/search.rs:17-33`, `src/search.rs:1019-1065`).
- Runtime search can create large persistent stores and long-running checkpoints; changes to batching, node IDs, or ranking require the persistence tests and README storage contract to be updated together.

## Evidence

- `README.md`
- `src/main.rs:143-286`, `src/main.rs:336-861`
- `src/config.rs`
- `src/search.rs:17-33`, `src/search.rs:917-1065`
- `src/rocksdb_output.rs:21-39`, `src/rocksdb_output.rs:129-227`
