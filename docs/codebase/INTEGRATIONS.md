# Integrations

## External libraries

- **eframe/egui:** native windowing and GUI rendering (`Cargo.toml`, `src/main.rs:771-1004`).
- **RocksDB:** local embedded persistence; no network database or remote service is configured (`Cargo.toml`, `src/rocksdb_output.rs`).
- **Rayon:** CPU parallelism for frontier evaluation (`Cargo.toml`, `src/search.rs:382-429`).
- **OS font files:** Japanese font discovery checks Windows, macOS, and Linux paths (`src/main.rs:440-505`).
- **Ctrl+C handler:** sets the active search interrupt flag and requests application exit after the worker saves and reports its checkpoint (`Cargo.toml`, `src/main.rs:821-831`, `src/main.rs:641-658`).

## Storage interface

The only application data store is the local sharded RocksDB under `HLSearch.rocksdb`. It has 210 shards and four column families per shard (`src/rocksdb_output.rs:1-19`, `src/rocksdb_output.rs:174-204`). The store rejects legacy single-DB and 256-shard layouts instead of migrating them (`src/rocksdb_output.rs:206-227`).

## Filesystem outputs

- `checkpoint.json` and `checkpoint.bak`: resumable search state.
- `hlsearch.log`: append-style text log.
- `HLSearch.rocksdb/shard-*`: path and metadata store.

These paths are rooted at `Cli.output_dir` (`src/main.rs:107-133`, `src/main.rs:507-648`).

## Network, authentication, and secrets

- Network APIs: **[TODO]** none were found in source or manifest.
- Authentication/authorization: **[TODO]** not applicable to the local desktop flow; no auth configuration was found.
- Secrets/environment configuration: **[TODO]** no `.env` or secret configuration was found. OS environment variables are used only to locate fonts (`src/main.rs:440-463`).

## Evidence

- `Cargo.toml`
- `src/main.rs:440-505`, `src/main.rs:507-699`
- `src/rocksdb_output.rs:1-19`, `src/rocksdb_output.rs:166-227`
- `README.md`

