use crate::config::{
    DEFAULT_CHECKPOINT_INTERVAL, DEFAULT_COLS, DEFAULT_DEPTH, MAX_SHIFT_TABLE_BYTES,
};
use crate::error::AppError;
use crate::search::SearchMode;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "HLSearch: 素数シフト探索 GUI (Rust版)",
    long_about = "CLI引数はGUIの初期設定に使用されます。起動後、Searchボタンで探索を開始します。"
)]
pub struct Cli {
    #[arg(short, long, default_value_t = DEFAULT_DEPTH, help = "探索する階層数")]
    pub depth: usize,

    #[arg(
        short,
        long,
        value_enum,
        default_value_t = SearchMode::Parallel,
        help = "探索モード (sequential | parallel)"
    )]
    pub mode: SearchMode,

    #[arg(long, default_value_t = DEFAULT_COLS, help = "列数 (長さ)")]
    pub cols: usize,

    #[arg(short, long, default_value = ".", help = "出力ディレクトリ")]
    pub output_dir: PathBuf,

    #[arg(
        long,
        default_value_t = DEFAULT_CHECKPOINT_INTERVAL,
        help = "チェックポイント保存周期 (ノード数)"
    )]
    pub checkpoint_interval: u64,
}

impl Cli {
    pub fn validate(&self, available_primes: usize) -> Result<(), AppError> {
        if self.depth == 0 {
            return Err(AppError::InvalidInput(
                "depth must be at least 1".to_string(),
            ));
        }
        if self.cols == 0 {
            return Err(AppError::InvalidInput(
                "cols must be at least 1".to_string(),
            ));
        }
        if self.checkpoint_interval == 0 {
            return Err(AppError::InvalidInput(
                "checkpoint-interval must be at least 1".to_string(),
            ));
        }
        if self.depth > available_primes {
            return Err(AppError::InvalidInput(format!(
                "depth ({}) cannot exceed available primes ({})",
                self.depth, available_primes
            )));
        }
        Ok(())
    }

    pub fn validate_for_primes(&self, primes: &[usize]) -> Result<(), AppError> {
        self.validate(primes.len())?;

        let words = self.cols.div_ceil(64);
        let table_bytes = primes
            .iter()
            .try_fold(0usize, |total, prime| {
                total.checked_add(prime.checked_mul(words)?.checked_mul(8)?)
            })
            .ok_or_else(|| {
                AppError::InvalidInput("shift table size overflows the platform limit".to_string())
            })?;
        if table_bytes > MAX_SHIFT_TABLE_BYTES {
            return Err(AppError::InvalidInput(format!(
                "shift table requires approximately {:.1} MiB; limit is {} MiB",
                table_bytes as f64 / (1024.0 * 1024.0),
                MAX_SHIFT_TABLE_BYTES / (1024 * 1024)
            )));
        }
        Ok(())
    }
}
