# Architecture

## System shape

The application is a single Rust binary with a GUI orchestration layer, a search-engine layer, and a persistence layer:

```text
CLI defaults / GUI input
          |
          v
      AppState (eframe)
          |
   background JoinHandle
          |
          v
   run_search / load_saved_results
          |
          v
   State (search.rs)
      |       \
      |        +--> SearchProgress atomics --> GUI polling
      v
  BitMask + shift table + beam expansion
          |
          v
  ShiftPathStore (RocksDB, 210 shards)
          |
          v
  OutputFile --> result tree and GUI
```

## Search flow

1. `generate_primes(MAX_PRIME)` creates the available prime list (`src/primes.rs:5-28`).
2. `build_shift_table()` creates a complement `BitMask` for every selected prime and shift (`src/search.rs:17-33`).
3. `State` starts from saved prefixes when available, otherwise from the root (`src/main.rs:535-555`, `src/search.rs:334-380`).
4. Sequential mode evaluates candidates in the current thread; parallel mode evaluates frontier candidates with Rayon (`src/search.rs:382-429`, `src/search.rs:467-644`).
5. Candidates below the fixed target `447` are pruned; surviving paths are accumulated and persisted (`src/search.rs:196-250`, `src/search.rs:431-465`).
6. Checkpoints capture traversal/beam state and are written at the configured node interval or interruption (`src/search.rs:917-1065`).
7. Completed results are ranked from RocksDB and transformed into a shared-prefix tree for display (`src/main.rs:317-394`, `src/main.rs:507-648`).

## Persistence model

`ShiftPathStore` uses four column families: `nodes`, `node_index`, `target_paths`, and `meta` (`src/rocksdb_output.rs:1-19`). Nodes contain depth, parent ID, shift, and score. `node_index` deduplicates a `(parent, shift)` edge. Node IDs encode depth, sequence, and shard. Writes are buffered and flushed in batches; paths are reconstructed by walking parent IDs (`src/rocksdb_output.rs:300-407`, `src/rocksdb_output.rs:571-664`).

## Concurrency and state

- GUI state remains on the eframe thread.
- Search runs on a spawned thread (`src/main.rs:396-438`).
- Progress is shared through atomics (`src/search.rs:110-194`).
- Parallel candidate evaluation uses Rayon; persistence and aggregate result updates remain coordinated by the search state (`src/search.rs:382-465`).
- Ctrl+C sets both the search interrupt and exit-request atomics. The GUI waits for the search worker to finish checkpoint handling, joins it, and then closes the viewport (`src/main.rs:641-658`, `src/main.rs:821-831`).

## Evidence

- `src/main.rs:289-438`, `src/main.rs:507-699`, `src/main.rs:771-1004`
- `src/search.rs:17-250`, `src/search.rs:382-1066`
- `src/rocksdb_output.rs:1-19`, `src/rocksdb_output.rs:166-738`
- `README.md`

