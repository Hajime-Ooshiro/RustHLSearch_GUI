# Stack

## Core

- **Language:** Rust 2021 (`Cargo.toml`)
- **Application type:** Native desktop GUI with an `eframe::App` implementation (`src/main.rs:771-928`)
- **Build system:** Cargo; `build.bat` runs debug and release builds
- **Runtime entry point:** `src/main.rs:929`

## Runtime dependencies

| Dependency | Role | Evidence |
|---|---|---|
| `eframe` / `egui` | Native GUI, panels, controls, drawing, repaint loop | `Cargo.toml`, `src/main.rs:771-928` |
| `clap` | CLI configuration parsed before GUI startup | `Cargo.toml`, `src/main.rs:107-133` |
| `rocksdb` | Sharded persistent path/tree storage | `Cargo.toml`, `src/rocksdb_output.rs:166-738` |
| `rayon` | Parallel frontier evaluation | `Cargo.toml`, `src/search.rs:382-429` |
| `serde` / `serde_json` | Checkpoint, node, and result serialization | `Cargo.toml`, `src/search.rs:36-101`, `src/rocksdb_output.rs:76-82` |
| `log` | Application logging | `Cargo.toml`, `src/main.rs:36-73` |
| `simple_logger` | Referenced by the commented legacy CLI path | `Cargo.toml`, `src/main.rs:143-286` |
| `ctrlc` | Referenced by the commented legacy CLI path | `Cargo.toml`, `src/main.rs:143-286` |
| `chrono` | Timestamped output path helper | `Cargo.toml`, `src/output.rs:1-25` |

## Tooling

- Formatting: `cargo fmt -- --check` (`README.md`)
- Tests: `cargo test` (`README.md`)
- Linting: `cargo clippy --all-targets -- -D warnings` (`README.md`)
- CI configuration: **[TODO]** no CI workflow was found in the repository root.
- Container/orchestration: **[TODO]** no Docker or orchestration configuration was found.

## Evidence

- `Cargo.toml`
- `Cargo.lock`
- `build.bat`
- `README.md`
- `src/main.rs`

