# Structure

## Repository layout

```text
.
├── Cargo.toml / Cargo.lock
├── README.md
├── build.bat
├── src/
│   ├── main.rs
│   ├── search.rs
│   ├── rocksdb_output.rs
│   ├── bitmask.rs
│   ├── primes.rs
│   ├── cli.rs
│   ├── config.rs
│   └── error.rs
└── docs/codebase/
```

## Entry points

- `main()` in `src/main.rs:783-861` parses `Cli`, creates the output directory and logger, installs Ctrl+C handling, validates a Japanese font, and starts eframe.
- `run_search()` in `src/main.rs:336-470` creates the search state, opens persistence, resumes or starts a search, loads top results, and removes checkpoints after successful completion.
- `load_saved_results()` in `src/main.rs:472-534` opens the store and displays saved paths without running a new search.
- `AppState::ui()` and the adjacent rendering helpers in `src/main.rs:560-782` implement the GUI interaction and result tree.

## Module responsibilities

- `main.rs`: composition root, GUI, worker lifecycle, logging, font discovery, result presentation, and top-level error conversion.
- `search.rs`: shift-table construction, sequential/parallel beam search, progress atomics, prefix reuse, checkpoint serialization/resume, and result aggregation.
- `rocksdb_output.rs`: sharded RocksDB schema, node/index writes, path reconstruction, ranking, metadata, and completion state.
- `bitmask.rs`: packed `u64` mask value object with in-place AND and popcount.
- `primes.rs`: Sieve of Eratosthenes up to `MAX_PRIME`.
- `cli.rs`: command-line options and input validation.
- `config.rs`: shared search/storage/display constants.
- `error.rs`: error enum used at the GUI worker boundary.

## Runtime artifacts

`--output-dir` receives `HLSearch.rocksdb\shard-000` through `shard-209`, `checkpoint.json`, `checkpoint.bak`, and `hlsearch.log` during normal operation (`README.md`, `src/main.rs:336-470`, `src/rocksdb_output.rs:166-204`).

## Evidence

- `Cargo.toml`
- `README.md`
- `src/main.rs:1-861`
- `src/search.rs:1-1285`
- `src/rocksdb_output.rs:1-740`
