# Testing

## Framework and location

Tests use Rust's built-in unit test harness and are colocated with implementation modules:

- `src/bitmask.rs:77-124`: mask size, popcount, and in-place AND behavior.
- `src/primes.rs:32-49`: sieve edge cases and inclusive limits.
- `src/output.rs:28-47`: timestamped path naming.
- `src/rocksdb_output.rs:738-953`: sharding, append/reopen, ordering, target paths, config hashes, completion state.
- `src/search.rs:1287-1808`: search modes, pruning, persistence, prefix reuse, checkpoints, and interruption.
- `src/main.rs:1006-1227`: fonts, logger, tree merging, CLI validation, saved-result behavior, and result serialization.

## Commands

```text
cargo test
cargo test beam_search_expands_all_candidates_at_each_level
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
```

These commands are documented in `README.md`.

## Test strategy

- Low-level behavior is tested with deterministic unit cases.
- Search tests use small prime/column configurations and temporary filesystem paths.
- RocksDB tests exercise real local databases rather than mocks.
- Checkpoint tests simulate interruption and resume for sequential and parallel modes.
- GUI tests focus on pure helpers and state transitions; no end-to-end window automation was found. **[TODO]**

## Test concerns

- Tests perform explicit temporary-directory cleanup with `remove_dir_all` (`src/search.rs`, `src/rocksdb_output.rs`).
- The repository does not show CI execution of the test suite. **[TODO]**
- No benchmark harness or property-testing dependency was found. **[TODO]**

## Evidence

- `README.md`
- `src/bitmask.rs:77-124`
- `src/primes.rs:32-49`
- `src/output.rs:28-47`
- `src/search.rs:1287-1808`
- `src/rocksdb_output.rs:738-953`
- `src/main.rs:1006-1227`

