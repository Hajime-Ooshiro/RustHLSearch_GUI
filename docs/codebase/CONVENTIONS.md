# Conventions

## Naming and organization

- Modules are snake_case files declared at the top of `src/main.rs`.
- Types use PascalCase; functions, fields, and modules use snake_case.
- Internal algorithm/storage constants are uppercase with descriptive names.
- Unit tests are colocated in `#[cfg(test)] mod tests` blocks.

## Error handling

- Search and storage APIs commonly return `Result<_, String>` or boxed errors.
- `AppError` converts I/O, JSON, string, and thread-panic failures at the GUI boundary (`src/error.rs:1-44`).
- UI-facing failures are stored in `AppState.search_error` and rendered by the GUI (`src/main.rs:289-438`, `src/main.rs:700-782`).
- Logger file-write failures are reported to stderr (`src/main.rs:46-69`).
- Lock poisoning is converted to explicit errors in persistence code (`src/rocksdb_output.rs:238-406`).

## Serialization and compatibility

- Serde derives are used for checkpoint, node, and result structures.
- Checkpoint fields use `#[serde(default)]` where older checkpoint shapes need to remain readable (`src/search.rs:36-101`).
- Resume validates depth, mode, and configuration hash (`src/search.rs:1020-1065`).
- RocksDB metadata stores per-depth configuration hashes and completion flags (`src/rocksdb_output.rs:717-738`).

## UI and logging

- UI state is centralized in `AppState`; worker operations return `OutputFile` (`src/main.rs:76-137`, `src/main.rs:336-534`).
- The UI requests repaint every 100 ms while searching (`src/main.rs:771-782`).
- `UiLogger` writes INFO-and-higher messages to stderr, an append-only file, and a bounded 500-entry deque (`src/main.rs:36-73`).

## Formatting and validation

The repository documents `cargo fmt -- --check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings` in `README.md`; `build.bat` runs those checks before the release build. No additional formatter or lint configuration file is tracked.

## Evidence

- `README.md`
- `build.bat`
- `src/main.rs`
- `src/search.rs`
- `src/rocksdb_output.rs`
- `src/error.rs`
