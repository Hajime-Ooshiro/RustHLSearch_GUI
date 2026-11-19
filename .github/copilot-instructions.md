# Copilot instructions for RustHLSearch

## Build, test, and lint

Use the repository root for Cargo commands:

```text
cargo fmt -- --check
cargo test
cargo test beam_search_keeps_all_unpruned_candidates_at_each_level
cargo clippy --all-targets -- -D warnings
cargo build --release
```

On Windows, `build.bat` runs format checking, the full test suite, Clippy with warnings denied, and the release build in that order. The application is started with `cargo run --release`; pass GUI defaults through the CLI, for example `cargo run --release -- --mode sequential --depth 8`.

Tests are Rust unit tests colocated in `src/bitmask.rs`, `src/primes.rs`, `src/search.rs`, `src/rocksdb_output.rs`, and `src/main.rs`. Use a test-name filter for one test; use `cargo test module_name::tests::test_name -- --exact` when an exact path is needed because this package exposes a binary test target rather than a library target.

## Architecture

This is one native Rust binary. `src/main.rs` owns CLI parsing, logging, Japanese-font setup, eframe/egui UI state, worker-thread lifecycle, and conversion of search results into a drawable shared-prefix tree. Search work runs off the GUI thread and exposes progress through atomics.

`src/search.rs` is the algorithm layer. It generates complement shift masks, evaluates sequential or Rayon-parallel beam-search candidates, prunes scores below the fixed target, reuses saved prefixes, and writes JSON checkpoints. `src/bitmask.rs` supplies packed `u64` masks and in-place AND/popcount operations; `src/primes.rs` supplies the deterministic prime list.

`src/rocksdb_output.rs` is the persistence adapter. Search paths are stored as parent-linked nodes in 210 RocksDB shards with `nodes`, `node_index`, `target_paths`, and `meta` column families. The GUI reads the top stored paths and renders them; it does not write a separate result JSON file. Runtime output under `--output-dir` includes `HLSearch.rocksdb`, `checkpoint.json`/`.bak`, and the append-only `hlsearch.log`.

## Repository-specific conventions

- Keep search invariants aligned across sequential and parallel modes. Both modes must produce the same search semantics and use the same target, depth, and configuration-hash rules.
- Preserve checkpoint compatibility behavior: checkpoint fields commonly use `#[serde(default)]`; resume rejects mismatched depth, mode, or configuration hash. Checkpoint writes use a temporary file and rotate the previous file to `.bak`.
- Treat RocksDB layout changes as compatibility changes. The current implementation intentionally rejects legacy single-database and 256-shard layouts instead of migrating them.
- Persist paths through `ShiftPathStore` APIs rather than adding direct RocksDB access to the UI or search orchestration. Buffered writes must be flushed before reads and before checkpoint/completion decisions.
- Keep UI mutations on the eframe thread. Search workers communicate through `Arc`, atomics, and their returned `OutputFile`; errors are surfaced through `AppState.search_error` and the UI.
- Use the existing error boundary: low-level search/storage code commonly returns `Result<_, String>`, while `src/error.rs` converts failures for the GUI worker boundary.
- The custom logger writes INFO-and-higher entries to stderr, `hlsearch.log`, and a bounded 500-entry in-memory deque. Preserve this fan-out when changing logging.
- Japanese UI text requires an installed supported font. Startup validates both font discovery and required glyphs before opening the window.
- Keep constants in `src/config.rs` and coordinate changes with checkpoint hashes, RocksDB metadata, README behavior, and tests. Important fixed values include target score `447`, maximum prime `1579`, maximum search depth `249`, and 210 shards.

## Source of truth

`README.md` documents the supported CLI options, GUI behavior, persistence layout, and search semantics. Verify it against active source when changing behavior; do not reintroduce removed modules or legacy commented code paths.
