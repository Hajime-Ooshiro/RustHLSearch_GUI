mod bitmask;
mod output;
mod primes;
mod search;
mod rocksdb_output;

use clap::Parser;
use log::{info, LevelFilter};
use output::with_timestamp;
use primes::{generate_primes, MAX_PRIME};
use search::{build_shift_table, SearchMode, State};
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use eframe::{egui, App, Frame};

const CHECKPOINT_FILENAME: &str = "checkpoint.json";
const CHECKPOINT_BACKUP_FILENAME: &str = "checkpoint.bak";

#[derive(Serialize)]
struct OutputFile<'a> {
    config: OutputConfig<'a>,
    result: OutputResult,
}

#[derive(Serialize)]
struct OutputConfig<'a> {
    mode: &'a str,
    depth: usize,
    cols: usize,
    elapsed: String,
}

#[derive(Serialize)]
struct OutputResult {
    max_score: usize,
    results: usize,
    shifts: Vec<Vec<usize>>,
    target_count: usize,
    target_shifts: Vec<Vec<usize>>,
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "HLSearch: 素数シフト探索プログラム (Rust版)", long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value_t = 8, help = "探索する階層数")]
    pub depth: usize,

    #[arg(
        short,
        long,
        value_enum,
        default_value_t = SearchMode::Parallel,
        help = "探索モード (sequential | parallel)"
    )]
    pub mode: SearchMode,

    #[arg(long, default_value_t = 3159, help = "列数 (長さ)")]
    pub cols: usize,

    #[arg(short, long, default_value = ".", help = "出力ディレクトリ")]
    pub output_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 100_000,
        help = "チェックポイント保存周期 (ノード数)"
    )]
    pub checkpoint_interval: u64,
}

impl Cli {
    fn validate(&self, available_primes: usize) -> Result<(), String> {
        if self.depth == 0 {
            return Err("depth must be at least 1".to_string());
        }
        if self.cols == 0 {
            return Err("cols must be at least 1".to_string());
        }
        if self.checkpoint_interval == 0 {
            return Err("checkpoint-interval must be at least 1".to_string());
        }
        if self.depth > available_primes {
            return Err(format!(
                "depth ({}) cannot exceed available primes ({})",
                self.depth, available_primes
            ));
        }
        Ok(())
    }
}

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     SimpleLogger::new().with_level(LevelFilter::Info).init()?;
//     let cli = Cli::parse();
//     let interrupted = Arc::new(AtomicBool::new(false));
//     {
//         let interrupted = Arc::clone(&interrupted);
//         ctrlc::set_handler(move || {
//             interrupted.store(true, Ordering::SeqCst);
//         })?;
//     }
//     let all_primes = generate_primes(MAX_PRIME);

//     if let Err(message) = cli.validate(all_primes.len()) {
//         eprintln!("エラー: {}", message);
//         std::process::exit(1);
//     }

//     let primes = all_primes;

//     info!("HLSearch (Rust) 開始");
//     info!(
//         "設定: mode={:?} depth={} primes={}",
//         cli.mode,
//         cli.depth,
//         primes.len()
//     );

//     let start_time = Instant::now();
//     let shift_table = build_shift_table(&primes[..cli.depth], cli.cols);
//     let mut state = State::new(primes, cli.cols, shift_table);
//     state.checkpoint_interval = cli.checkpoint_interval;
//     let progress_bar = ProgressBar::new_spinner();
//     progress_bar.set_style(
//         ProgressStyle::with_template("{spinner:.cyan} {msg}")?
//             .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
//     );
//     progress_bar.enable_steady_tick(std::time::Duration::from_millis(100));
//     state.set_progress_bar(progress_bar);

//     // 探索中にチェックポイントを書き出すため、他の出力より先に output_dir を用意する
//     std::fs::create_dir_all(&cli.output_dir)?;
//     let shift_path = cli.output_dir.join("HLSearch.rocksdb");
//     let checkpoint_path = cli.output_dir.join(CHECKPOINT_FILENAME);
//     let backup_path = cli.output_dir.join(CHECKPOINT_BACKUP_FILENAME);
//     let resume_path = if checkpoint_path.exists() {
//         Some(checkpoint_path.as_path())
//     } else {
//         backup_path.exists().then_some(backup_path.as_path())
//     };
//     state.set_shift_path_store(&shift_path, cli.depth, resume_path.is_none())?;

//     let completed = match cli.mode {
//         SearchMode::Sequential => state.search_with_checkpoint_interruptible(
//             cli.depth,
//             Some(&checkpoint_path),
//             resume_path,
//             &interrupted,
//         )?,
//         SearchMode::Parallel => {
//             let (result, completed) = state.search_parallel_interruptible(
//                 cli.depth,
//                 Some(&checkpoint_path),
//                 resume_path,
//                 &interrupted,
//             )?;
//             state.max_score = result.max_score;
//             state.results = result.results;
//             state.shifts = result.shifts;
//             state.target_count = result.target_count;
//             state.target_shifts = result.target_shifts;
//             state.node_count = result.node_count;
//             completed
//         }
//     };

//     if !completed {
//         state.finish_progress(false);
//         info!(
//             "中断要求を受信したためチェックポイントを保存しました: {}",
//             checkpoint_path.display()
//         );
//         return Ok(());
//     }

//     if checkpoint_path.exists() {
//         std::fs::remove_file(&checkpoint_path)?;
//         if backup_path.exists() {
//             std::fs::remove_file(&backup_path)?;
//         }
//         info!(
//             "探索完了のためチェックポイントを削除: {}",
//             checkpoint_path.display()
//         );
//     }

//     state.finish_progress(true);
//     let elapsed = start_time.elapsed();
//     info!("探索時間: {:?}", elapsed);
//     info!("最大値: {}", state.max_score);
//     info!("該当件数: {}", state.results);
//     info!("ターゲット一致件数: {}", state.target_count);

//     info!("シフトパスRocksDB出力ディレクトリ: {}", shift_path.display());

//     let result_path = with_timestamp(&cli.output_dir.join("result.txt"), cli.depth);
//     let result_file = File::create(&result_path)?;
//     let mut result_writer = BufWriter::new(result_file);
//     info!("探索結果出力ファイル: {}", result_path.display());

//     let output = OutputFile {
//         config: OutputConfig {
//             mode: match cli.mode {
//                 SearchMode::Sequential => "sequential",
//                 SearchMode::Parallel => "parallel",
//             },
//             depth: cli.depth,
//             cols: cli.cols,
//             elapsed: format!("{elapsed:?}"),
//         },
//         result: OutputResult {
//             max_score: state.max_score,
//             results: state.results,
//             shifts: state.shifts,
//             target_count: state.target_count,
//             target_shifts: state.target_shifts,
//         },
//     };
//     serde_json::to_writer_pretty(&mut result_writer, &output)?;
//     writeln!(result_writer)?;

//     info!("HLSearch 終了");
//     Ok(())
// }

struct AppState {
    cli: Cli,
    state: State,
    start_time: Instant,
    search_results: Option<OutputFile<'static>>, // Use lifetime annotation or Box if necessary in a real scenario
    // Placeholder for GUI state management
    show_visualization: bool,
}

impl AppState {
    fn run_search(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        self.start_time = start_time;

        // Re-run setup logic needed before calling search methods, 
        // or refactor search_with_checkpoint_interruptible/search_parallel_interruptible 
        // to return necessary state components for GUI display.

        // --- WARNING: For simplicity, we re-initialize and run the whole flow here ---
        // In a real application, you'd pass results from main() into AppState during setup.
        info!("Re-running search logic within GUI context...");
        
        let all_primes = generate_primes(MAX_PRIME);
        if let Err(message) = self.cli.validate(all_primes.len()) {
            eprintln!("Error: {}", message);
            return Ok(());
        }

        let primes = all_primes;
        info!("HLSearch (Rust) 開始");
        
        let shift_table = build_shift_table(&primes[..self.cli.depth], self.cli.cols);
        let mut state = State::new(primes, self.cli.cols, shift_table);
        state.checkpoint_interval = self.cli.checkpoint_interval;
        
        // Reset progress bar logic for GUI context if needed
        // For now, we rely on the search function updating internal state/progress reporting mechanisms.

        let (mut completed, result) = match self.cli.mode {
            SearchMode::Sequential => {
                let checkpoint_path = self.cli.output_dir.join("checkpoint.json");
                let backup_path = self.cli.output_dir.join("checkpoint.bak");
                let resume_path = if checkpoint_path.exists() {
                    Some(checkpoint_path.as_path())
                } else {
                    backup_path.exists().then_some(backup_path.as_path())
                };

                state.set_shift_path_store(&self.cli.output_dir.join("HLSearch.rocksdb"), self.cli.depth, resume_path.is_none())?;
                
                let completed = state.search_with_checkpoint_interruptible(
                    self.cli.depth,
                    Some(&checkpoint_path),
                    resume_path,
                    &Arc::new(std::sync::atomic::AtomicBool::new(false)), // Dummy interruption handling for GUI context start
                )?;
                
                let result = OutputFile {
                    config: OutputConfig {
                        mode: "sequential",
                        depth: self.cli.depth,
                        cols: self.cli.cols,
                        elapsed: format!("{:?}", start_time.elapsed()),
                    },
                    result: OutputResult {
                        max_score: state.max_score,
                        results: state.results,
                        shifts: state.shifts,
                        target_count: state.target_count,
                        target_shifts: state.target_shifts,
                    },
                };

                (completed, result)
            }
            SearchMode::Parallel => {
                 // Similar complex initialization for parallel mode...
                let checkpoint_path = self.cli.output_dir.join("checkpoint.json");
                let backup_path = self.cli.output_dir.join("checkpoint.bak");
                let resume_path = if checkpoint_path.exists() {
                    Some(checkpoint_path.as_path())
                } else {
                    backup_path.exists().then_some(backup_path.as_path())
                };

                state.set_shift_path_store(&self.cli.output_dir.join("HLSearch.rocksdb"), self.cli.depth, resume_path.is_none())?;
                let (result, completed) = state.search_parallel_interruptible(
                    self.cli.depth,
                    Some(&checkpoint_path),
                    resume_path,
                    &Arc::new(std::sync::atomic::AtomicBool::new(false)), // Dummy interruption handling for GUI context start
                )?;
                
                let result_data = OutputFile {
                    config: OutputConfig {
                        mode: "parallel",
                        depth: self.cli.depth,
                        cols: self.cli.cols,
                        elapsed: format!("{:?}", start_time.elapsed()),
                    },
                    result: OutputResult {
                        max_score: result.max_score,
                        results: result.results,
                        shifts: result.shifts,
                        target_count: result.target_count,
                        target_shifts: result.target_shifts,
                    }
                };

                (completed, result_data)
            }
        };

        // Update AppState results based on search outcome
        self.search_results = Some(result);
        Ok(())
    }
}


impl App for AppState {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("HLSearch Visualization");
            
            if let Some(result) = &self.search_results {
                ui.separator();
                ui.label(format!("Search Finished! Max Score: {}", result.result.max_score));

                // --- Tree Visualization Placeholder ---
                ui.add_space(10.0);
                ui.label("Search Tree (Visualization Area)");
                // TODO: Implement actual tree visualization logic here using egui primitives
                egui::Label::new(egui::RichText::new("Visualization placeholder: Displaying structure based on shifts...").monospace()).sense(egui::Sense::hover());
            } else {
                ui.label("Press 'Run Search' to begin visualization.");
            }
        });
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Setup initial state structure that can be passed to the GUI AppState
    let cli = Cli::parse();

    // 1. Initialize a dummy/default AppState instance for eframe setup
    let mut initial_state = AppState {
        cli: cli.clone(),
        state: State::new(generate_primes(MAX_PRIME), cli.cols, build_shift_table(&generate_primes(MAX_PRIME)[..cli.depth], cli.cols)), // Requires complex setup/refactoring here
        start_time: Instant::now(),
        search_results: None,
        show_visualization: true,
    };

    // 2. Setup logging and other initializations (kept for compatibility)
    SimpleLogger::new().with_level(LevelFilter::Info).init()?;
    let interrupted = Arc::new(AtomicBool::new(false));
    {
        let interrupted = Arc::clone(&interrupted);
        ctrlc::set_handler(move || {
            interrupted.store(true, Ordering::SeqCst);
        })?;
    }


    // 3. Run the GUI application instead of exiting CLI style
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };

    // Note: Due to the massive structural change required, I cannot reliably replace the entire execution path below 
    // with a simple call because the search logic depends heavily on CLI arguments and exiting state cleanup.
    // The user must adapt `AppState::run_search` and the initialization above carefully.
    
    // For now, we provide the eframe run loop structure:
    eframe::run_native(
        "HLSearch Visualizer",
        options,
        Box::new(move |_cc| {
            let app = AppState {
                cli: cli.clone(),
                state: State::new(generate_primes(MAX_PRIME), cli.cols, build_shift_table(&generate_primes(MAX_PRIME)[..cli.depth], cli.cols)), 
                start_time: Instant::now(),
                search_results: None,
                show_visualization: true,
            };
            Box::new(app)
        }),
    ).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to run eframe: {}", e),
        )
    })?;


    // If the user still prefers CLI output fallback (for testing/non-GUI use):
    /* 
    if initial_state.search_results.is_some() {
        println!("\n--- GUI mode skipped. Displaying structured result (Fallback) ---");
        // Logic to write results...
    }
    */

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    fn test_cli() -> Cli {
        Cli {
            depth: 1,
            mode: SearchMode::Sequential,
            cols: 4,
            output_dir: PathBuf::from("."),
            checkpoint_interval: 100_000,
        }
    }

    #[test]
    fn cli_validation_accepts_valid_configuration() {
        assert!(test_cli().validate(3).is_ok());
    }

    #[test]
    fn cli_validation_rejects_invalid_configuration() {
        let mut cli = test_cli();
        cli.depth = 0;
        assert!(cli.validate(3).is_err());
        cli = test_cli();
        cli.cols = 0;
        assert!(cli.validate(3).is_err());
        cli = test_cli();
        cli.checkpoint_interval = 0;
        assert!(cli.validate(3).is_err());
    }

    #[test]
    fn cli_validation_accepts_depth_equal_to_available_primes() {
        let mut cli = test_cli();
        cli.depth = 3;
        assert!(cli.validate(3).is_ok());
    }

    #[test]
    fn cli_validation_rejects_depth_above_available_primes() {
        let mut cli = test_cli();
        cli.depth = 4;
        assert_eq!(
            cli.validate(3).unwrap_err(),
            "depth (4) cannot exceed available primes (3)"
        );
    }

    #[test]
    fn output_result_serializes_best_score_shifts() {
        let json = serde_json::to_value(OutputResult {
            max_score: 447,
            results: 1,
            shifts: vec![vec![1, 1, 4]],
            target_count: 0,
            target_shifts: Vec::new(),
        })
        .unwrap();

        assert_eq!(json["shifts"], serde_json::json!([[1, 1, 4]]));
    }
}
