# Integrations

## External libraries and OS services

- `eframe`/`egui` provides native rendering and controls; the application explicitly selects Glow (`Cargo.toml`, `src/main.rs:835-861`).
- RocksDB is the only application data store and is local to `--output-dir` (`Cargo.toml`, `src/rocksdb_output.rs:166-204`).
- Rayon supplies CPU parallelism for frontier evaluation (`Cargo.toml`, `src/search.rs:382-429`).
- Japanese font discovery reads OS font paths on Windows, macOS, and Linux (`src/main.rs:440-505`).
- `ctrlc` installs the process interrupt handler (`Cargo.toml`, `src/main.rs:806-820`).

## Storage interface

The store is `HLSearch.rocksdb` with 210 shard directories and four column families per shard: `nodes`, `node_index`, `target_paths`, and `meta`. The implementation rejects legacy single-DB and 256-shard layouts instead of migrating them (`src/rocksdb_output.rs:166-227`).

## Filesystem outputs

- `checkpoint.json` and `checkpoint.bak`: resumable search state.
- `hlsearch.log`: append-style text log.
- `HLSearch.rocksdb\shard-*`: parent-linked search paths and metadata.

All are rooted at `Cli.output_dir` (`src/cli.rs:8-38`, `src/main.rs:336-390`).

## Network and configuration

No network API, authentication, environment-file configuration, container configuration, or secret-management integration appears in the tracked source or manifest. The only environment variables read by application code are used to locate fonts (`src/main.rs:440-463`).

## Evidence

- `Cargo.toml`
- `src/main.rs:440-505`, `src/main.rs:806-861`
- `src/rocksdb_output.rs:1-20`, `src/rocksdb_output.rs:166-227`
- `src/cli.rs:8-57`
- `README.md`
