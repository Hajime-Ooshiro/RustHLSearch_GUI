# Testing

## Framework and location

The project uses Rust's built-in unit-test harness. Tests are colocated with implementation modules:

- `src/bitmask.rs:76-124`: mask sizing, bit setting, AND, and popcount.
- `src/primes.rs:31-49`: sieve edge cases and inclusive limits.
- `src/rocksdb_output.rs:737-953`: sharding, append/reopen, ordering, target paths, hashes, and completion state.
- `src/search.rs:1287-1808`: beam behavior, prefix reuse, modes, checkpoints, and interruption.
- `src/main.rs:863-1120`: font/logger helpers, tree construction, CLI validation, persistence orchestration, and result serialization.

## Commands

```text
cargo test
cargo test beam_search_keeps_all_unpruned_candidates_at_each_level
cargo test search::tests::beam_search_keeps_all_unpruned_candidates_at_each_level -- --exact
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
```

`build.bat` runs format checking, all tests, Clippy, and the release build on Windows.

## Test strategy

- Low-level bitmask and prime generation tests use deterministic in-memory cases.
- Search and RocksDB tests use small configurations and temporary filesystem directories.
- Persistence tests exercise real local RocksDB stores rather than mocks.
- Checkpoint tests cover interruption/resume behavior for sequential and parallel paths.
- GUI tests target pure helpers and state transitions; no end-to-end window automation is present.

## Evidence

- `README.md`
- `build.bat`
- `src/bitmask.rs:76-124`
- `src/primes.rs:31-49`
- `src/search.rs:1287-1808`
- `src/rocksdb_output.rs:737-953`
- `src/main.rs:863-1120`
