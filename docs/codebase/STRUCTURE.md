# Structure

## Repository layout

```text
.
├── Cargo.toml              # Package metadata and dependencies
├── Cargo.lock              # Locked dependency graph
├── README.md               # User-facing behavior, build, and algorithm notes
├── build.bat               # Windows debug/release build helper
├── LICENSE                 # MIT license
├── src/
│   ├── main.rs             # GUI, CLI configuration, orchestration, UI tests
│   ├── search.rs           # Search state, beam search, checkpoints, progress
│   ├── rocksdb_output.rs   # Sharded RocksDB path persistence
│   ├── bitmask.rs          # 64-bit packed bit mask operations
│   ├── primes.rs           # Sieve-based prime generation
│   └── output.rs           # Timestamped path helper
└── docs/codebase/          # This codebase map
```

## Entry points

- `main()` in `src/main.rs:929` parses `Cli`, initializes logging/fonts, constructs `AppState`, and starts the eframe window.
- `AppState::ui()` in `src/main.rs:786` exposes depth, search/display actions, progress, logs, and the result tree.
- `run_search()` in `src/main.rs:507` creates the search state, configures RocksDB/checkpointing, runs the selected mode, and loads top results.
- `load_saved_results()` in `src/main.rs:649` reads existing RocksDB results without running a new search.

## Module responsibilities

- `main.rs`: application composition and presentation boundary. It owns GUI state, background thread lifecycle, logging, font discovery, CLI validation, and result-tree layout.
- `search.rs`: domain/search engine. It builds masks, evaluates candidates, applies the target threshold, persists candidates, reports progress, and serializes/resumes checkpoints.
- `rocksdb_output.rs`: storage adapter. It stores paths as parent-linked nodes across 210 shards and provides reconstruction, ranking, batching, metadata, and completion state.
- `bitmask.rs`: low-level value object for packed `u64` masks and popcount.
- `primes.rs`: deterministic prime input generation using the Sieve of Eratosthenes.
- `output.rs`: reusable timestamped filename helper; it is currently covered by unit tests but is not part of the active GUI result path.

## Generated/runtime artifacts

When a search runs, `--output-dir` may contain `HLSearch.rocksdb/` with `shard-000` through `shard-209`, `checkpoint.json`, `checkpoint.bak`, and `hlsearch.log` (`README.md`, `src/main.rs:507-648`).

## Evidence

- `README.md`
- `Cargo.toml`
- `src/main.rs:1-134`, `src/main.rs:289-928`
- `src/search.rs:1-205`
- `src/rocksdb_output.rs:1-40`

