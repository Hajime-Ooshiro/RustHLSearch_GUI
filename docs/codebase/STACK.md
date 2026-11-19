# Stack

## Core

- Rust 2021 package, built with Cargo (`Cargo.toml`).
- One native desktop binary using `eframe`/`egui` with the Glow renderer (`Cargo.toml`, `src/main.rs:783-861`).
- Windows helper script: `build.bat` (`build.bat:1-20`).

## Runtime dependencies

| Dependency | Role | Evidence |
|---|---|---|
| `eframe` / `egui` | Native window, panels, controls, scrolling, and custom search-tree drawing | `Cargo.toml`, `src/main.rs:560-782` |
| `clap` | CLI defaults and validation before GUI startup | `Cargo.toml`, `src/cli.rs:1-57` |
| `rocksdb` | Local 210-shard path persistence | `Cargo.toml`, `src/rocksdb_output.rs:1-20`, `src/rocksdb_output.rs:166-204` |
| `rayon` | Parallel frontier evaluation | `Cargo.toml`, `src/search.rs:382-429` |
| `serde` / `serde_json` | Checkpoints, node records, and result serialization | `Cargo.toml`, `src/search.rs:36-101`, `src/rocksdb_output.rs:76-82` |
| `log` | Application logging facade | `Cargo.toml`, `src/main.rs:36-73` |
| `ctrlc` | Interrupt and graceful checkpoint shutdown handling | `Cargo.toml`, `src/main.rs:806-820` |

## Development commands

- Format check: `cargo fmt -- --check` (`README.md`, `build.bat:4-5`)
- Tests: `cargo test` (`README.md`, `build.bat:7-8`)
- Lint: `cargo clippy --all-targets -- -D warnings` (`README.md`, `build.bat:10-11`)
- Release build: `cargo build --release` (`README.md`, `build.bat:13-14`)

No CI workflow or repository-specific formatter/linter configuration is present in the tracked tree.

## Evidence

- `Cargo.toml`
- `Cargo.lock`
- `README.md`
- `build.bat`
- `src/main.rs`
