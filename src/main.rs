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
use std::collections::HashMap;
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
    search_tree: Vec<TreeNode>,
    depth_input: String,
    search_error: Option<String>,
    // Placeholder for GUI state management
    show_visualization: bool,
}

struct TreeNode {
    shift: Option<usize>,
    parent: Option<usize>,
    depth: usize,
    children: Vec<usize>,
    y: f32,
}

fn build_search_tree(paths: &[Vec<usize>]) -> Vec<TreeNode> {
    let mut nodes = vec![TreeNode {
        shift: None,
        parent: None,
        depth: 0,
        children: Vec::new(),
        y: 0.0,
    }];
    let mut child_indices = HashMap::new();

    for path in paths {
        let mut parent = 0;
        for (depth, &shift) in path.iter().enumerate() {
            let key = (parent, shift);
            let child = *child_indices.entry(key).or_insert_with(|| {
                let child = nodes.len();
                nodes.push(TreeNode {
                    shift: Some(shift),
                    parent: Some(parent),
                    depth: depth + 1,
                    children: Vec::new(),
                    y: 0.0,
                });
                nodes[parent].children.push(child);
                child
            });
            parent = child;
        }
    }

    let mut next_leaf = 0;
    assign_tree_y(&mut nodes, 0, &mut next_leaf);
    nodes
}

fn assign_tree_y(nodes: &mut [TreeNode], index: usize, next_leaf: &mut usize) -> f32 {
    if nodes[index].children.is_empty() {
        let y = 48.0 + *next_leaf as f32 * 42.0;
        *next_leaf += 1;
        nodes[index].y = y;
        return y;
    }

    let children = nodes[index].children.clone();
    let first_y = assign_tree_y(nodes, children[0], next_leaf);
    let mut last_y = first_y;
    for child in children.iter().skip(1) {
        last_y = assign_tree_y(nodes, *child, next_leaf);
    }
    nodes[index].y = (first_y + last_y) / 2.0;
    nodes[index].y
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
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, message).into());
        }

        let primes = all_primes;
        info!("HLSearch (Rust) GUI Mode Start");
        
        let shift_table = build_shift_table(&primes[..self.cli.depth], self.cli.cols);
        let mut state = State::new(primes, self.cli.cols, shift_table);
        state.checkpoint_interval = self.cli.checkpoint_interval;
        
        // Reset progress bar logic for GUI context if needed
        // For now, we rely on the search function updating internal state/progress reporting mechanisms.

        let (completed, result) = match self.cli.mode {
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

        if completed {
            for filename in ["checkpoint.json", "checkpoint.bak"] {
                let checkpoint_path = self.cli.output_dir.join(filename);
                if checkpoint_path.exists() {
                    std::fs::remove_file(&checkpoint_path)?;
                    info!("Removed completed search checkpoint: {}", checkpoint_path.display());
                }
            }
        }

        // Update AppState results based on search outcome
        self.search_tree = build_search_tree(&result.result.shifts);
        self.search_results = Some(result);
        Ok(())
    }
}

fn show_search_tree(ui: &mut egui::Ui, nodes: &[TreeNode], max_score: usize) {
    let max_depth = nodes.iter().map(|node| node.depth).max().unwrap_or(0);
    let leaf_count = nodes
        .iter()
        .filter(|node| node.children.is_empty())
        .count()
        .max(1);
    let graph_size = egui::vec2(
        240.0 + max_depth as f32 * 140.0,
        (leaf_count as f32 * 42.0 + 80.0).max(120.0),
    );

    egui::ScrollArea::both()
        .auto_shrink([false, false])
        .show(ui, |ui| {
            let (rect, _) = ui.allocate_exact_size(graph_size, egui::Sense::hover());
            let painter = ui.painter_at(rect);
            let x_for_depth = |depth: usize| rect.left() + 60.0 + depth as f32 * 140.0;

            for node in nodes.iter().skip(1) {
                if let Some(parent) = node.parent {
                    painter.line_segment(
                        [
                            egui::pos2(
                                x_for_depth(nodes[parent].depth) + 9.0,
                                rect.top() + nodes[parent].y,
                            ),
                            egui::pos2(
                                x_for_depth(node.depth) - 9.0,
                                rect.top() + node.y,
                            ),
                        ],
                        egui::Stroke::new(1.0_f32, egui::Color32::from_gray(110)),
                    );
                }
            }

            for node in nodes {
                let center = egui::pos2(x_for_depth(node.depth), rect.top() + node.y);
                painter.circle_filled(center, 8.0, egui::Color32::from_rgb(70, 145, 220));
                let label = match node.shift {
                    Some(shift) if node.children.is_empty() => {
                        format!("shift {shift} (score {max_score})")
                    }
                    Some(shift) => format!("shift {shift}"),
                    None => "Start".to_string(),
                };
                painter.text(
                    center + egui::vec2(14.0, 0.0),
                    egui::Align2::LEFT_CENTER,
                    label,
                    egui::FontId::proportional(14.0),
                    ui.visuals().text_color(),
                );
            }
        });
}


impl App for AppState {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let mut search_requested = false;
        egui::TopBottomPanel::top("top_view")
            .default_height(60.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("TopView");
                    ui.separator();
                    ui.label("Depth");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.depth_input)
                            .desired_width(80.0),
                    );
                    search_requested = ui.button("Search").clicked();
                });
                if let Some(error) = &self.search_error {
                    ui.colored_label(egui::Color32::RED, error);
                }
            });

        egui::SidePanel::left("side_view")
            .default_width(260.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("SideView");
            });

        if search_requested {
            self.search_error = match self.depth_input.parse::<usize>() {
                Ok(depth) => {
                    self.cli.depth = depth;
                    self.run_search().err().map(|error| error.to_string())
                }
                Err(_) => Some("Depth must be a positive integer".to_string()),
            };
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("MainView");
            
            if let Some(result) = &self.search_results {
                ui.separator();
                ui.label(format!("Search Finished! Max Score: {}", result.result.max_score));

                ui.add_space(10.0);
                ui.label(format!(
                    "Best-score paths: {} ({} tree nodes)",
                    result.result.shifts.len(),
                    self.search_tree.len().saturating_sub(1)
                ));
                show_search_tree(ui, &self.search_tree, result.result.max_score);
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
        search_tree: Vec::new(),
        depth_input: cli.depth.to_string(),
        search_error: None,
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
                search_tree: Vec::new(),
                depth_input: cli.depth.to_string(),
                search_error: None,
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

    #[test]
    fn search_tree_merges_shared_path_prefixes() {
        let tree = build_search_tree(&[vec![2, 1], vec![2, 0], vec![1, 1]]);

        assert_eq!(tree.len(), 6);
        assert_eq!(tree[0].children.len(), 2);
        assert_eq!(tree[1].shift, Some(2));
        assert_eq!(tree[1].children.len(), 2);
        assert_eq!(tree[1].parent, Some(0));
    }

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
