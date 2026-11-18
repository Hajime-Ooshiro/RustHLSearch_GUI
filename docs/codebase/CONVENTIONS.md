# Conventions

## Naming and organization

- Rust modules use snake_case filenames and are declared at the top of `src/main.rs`.
- Types use PascalCase (`AppState`, `BitMask`, `ShiftPathStore`); functions and fields use snake_case.
- Search-specific constants are uppercase with descriptive suffixes (`SHIFT_PATH_BATCH_SIZE`, `BEAM_POLICY_VERSION`).
- Tests are colocated in `#[cfg(test)] mod tests` blocks in each module.

## Error handling

- Public storage/search APIs generally return `Result<_, String>` or boxed errors at orchestration boundaries (`src/search.rs:212-258`, `src/rocksdb_output.rs:166-204`).
- GUI errors are stored in `AppState.search_error` and rendered as red labels (`src/main.rs:289-438`, `src/main.rs:786-928`).
- Logging failures are reported to stderr rather than silently ignored (`src/main.rs:46-69`).
- Some tests and internal invariants use `unwrap()`/`expect()`; production code also uses `expect()` for the proven non-empty path invariant (`src/rocksdb_output.rs:364`).

## Serialization and compatibility

- Serde derives are used for checkpoint, node, and result structures.
- Checkpoint fields frequently use `#[serde(default)]` to tolerate older checkpoint shapes (`src/search.rs:61-101`).
- RocksDB metadata stores configuration hashes and completion flags keyed by depth (`src/rocksdb_output.rs:146-160`, `src/rocksdb_output.rs:717-738`).

## UI and logging

- `AppState` owns UI-facing state; the background operation returns an `OutputFile`.
- UI repaint is requested every 100 ms while searching (`src/main.rs:771-784`).
- The custom `UiLogger` writes INFO-or-higher entries to stderr, `hlsearch.log`, and a bounded in-memory deque of 500 entries (`src/main.rs:36-73`).

## Formatting and validation

- The README specifies `cargo fmt -- --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings`.
- No repository-specific formatter or lint configuration file was found. **[TODO]**

## Evidence

- `src/main.rs`
- `src/search.rs`
- `src/rocksdb_output.rs`
- `src/bitmask.rs`
- `src/primes.rs`
- `README.md`

