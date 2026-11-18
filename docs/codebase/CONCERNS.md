# Concerns

## Verified implementation concerns

1. **Large orchestration module:** `src/main.rs` combines CLI parsing, logging, font discovery, search orchestration, result modeling, and all GUI rendering. This creates a broad change surface for UI or startup changes (`src/main.rs:1-1004`).
2. **Legacy commented implementation:** A substantial CLI/progress-bar implementation remains commented out in `src/main.rs:143-286`. It references APIs/dependencies that are not part of the active GUI path and can obscure the current execution model.
4. **Fixed algorithm thresholds and limits:** target score `447`, maximum prime `1579`, maximum depth `249`, 210 shards, and display limit 30 are constants spread across modules (`src/search.rs:17-33`, `src/search.rs:196-250`, `src/rocksdb_output.rs:21-39`, `src/main.rs:24-25`). Changing them requires coordinated compatibility review.
5. **RocksDB resource footprint:** `ShiftPathStore::create` opens 210 RocksDB instances and uses shared cache/write-buffer resources (`src/rocksdb_output.rs:129-204`). This is intentional for the storage layout but may be expensive on constrained systems.
6. **Persistence compatibility is strict:** existing single-DB and 256-shard layouts are rejected without migration (`src/rocksdb_output.rs:206-227`), so upgrades require an operational backup/move procedure.
7. **No automated CI evidence:** no workflow configuration was found. **[TODO]** Confirm the intended CI/build matrix.

## Potential correctness/maintenance risks

- `output.rs::with_timestamp` is tested but is not used by the active `run_search()` result path (`src/output.rs:6-25`, `src/main.rs:507-648`). **[ASK USER]** Decide whether timestamped result-file output is still a supported feature or should be removed/documented as legacy.
- The README describes a wgpu renderer, while `Cargo.toml` enables eframe's `glow`, `wayland`, and `x11` features and the active code does not explicitly configure wgpu (`README.md`, `Cargo.toml`). This is an intent-vs-reality divergence.
- The README describes CLI result-file output, while the active `main()` starts the GUI and `run_search()` returns results to the UI rather than writing the legacy JSON file path (`README.md`, `src/main.rs:507-648`, `src/main.rs:929-1004`). This is an intent-vs-reality divergence.
- Production code uses `String` error values at several module boundaries, which loses structured error categories (`src/search.rs`, `src/rocksdb_output.rs`). **[TODO]** No project decision on typed error design was found.

## Evidence

- `README.md`
- `Cargo.toml`
- `src/main.rs:143-306`, `src/main.rs:507-648`, `src/main.rs:929-1004`
- `src/search.rs:17-33`, `src/search.rs:196-250`
- `src/rocksdb_output.rs:21-39`, `src/rocksdb_output.rs:129-227`
- `src/output.rs:6-47`

