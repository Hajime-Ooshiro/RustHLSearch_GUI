# Architecture

## System shape

```text
CLI + GUI input
      |
      v
AppState / eframe UI
      |
background JoinHandle
      |
run_search or load_saved_results
      |
State (search.rs)
  |             \
  v              +--> SearchProgress atomics --> UI polling
BitMask + beam search
      |
ShiftPathStore (RocksDB, 210 shards)
      |
OutputFile --> shared-prefix result tree --> GUI
```

## Search flow

1. `generate_primes(MAX_PRIME)` creates the available prime list (`src/primes.rs:5-28`).
2. `build_shift_table()` creates a complement `BitMask` for every selected prime and shift (`src/search.rs:17-33`).
3. `run_search()` configures `State`, checkpoint paths, and `ShiftPathStore` (`src/main.rs:336-390`).
4. Sequential mode evaluates candidates directly; parallel mode evaluates each frontier with Rayon (`src/search.rs:382-429`, `src/search.rs:1077-1285`).
5. Candidates below `TARGET_SCORE` are discarded; surviving paths are aggregated and persisted (`src/search.rs:431-465`, `src/search.rs:672-900`).
6. Checkpoints capture beam/frontier state and configuration hash at the configured node interval or interruption (`src/search.rs:917-1065`).
7. Completed results are ranked from RocksDB and transformed into a shared-prefix tree (`src/main.rs:138-214`, `src/main.rs:472-534`).

## Persistence model

`ShiftPathStore` opens 210 shards, each with `nodes`, `node_index`, `target_paths`, and `meta` column families. Node IDs encode depth, sequence, and shard. Parent-linked nodes allow paths to be reconstructed; `node_index` deduplicates `(parent, shift)` edges. Writes are buffered, batched, and flushed before reads (`src/rocksdb_output.rs:1-20`, `src/rocksdb_output.rs:310-406`).

## Concurrency and state

- The eframe thread owns `AppState` and all UI mutation.
- A spawned worker performs search or saved-result loading (`src/main.rs:396-438`).
- Progress is shared through atomics in `SearchProgress` (`src/search.rs:110-194`).
- Rayon is used only for candidate evaluation; persistence remains coordinated by `State`.
- Ctrl+C sets interruption/exit flags. The UI waits for checkpoint handling and joins the worker before closing (`src/main.rs:806-820`, `src/main.rs:641-658`).

## Evidence

- `src/main.rs:138-214`, `src/main.rs:336-534`, `src/main.rs:641-861`
- `src/search.rs:17-250`, `src/search.rs:382-465`, `src/search.rs:917-1285`
- `src/rocksdb_output.rs:1-20`, `src/rocksdb_output.rs:166-740`
- `README.md`
