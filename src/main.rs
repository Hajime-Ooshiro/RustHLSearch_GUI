mod bitmask;
mod output;
mod primes;
mod rocksdb_output;
mod search;

use clap::Parser;
use eframe::{egui, App};
use log::{info, Level, LevelFilter, Log, Metadata, Record};
use primes::{generate_primes, MAX_PRIME};
use search::{build_shift_table, SearchMode, SearchProgress, State};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const CHECKPOINT_FILENAME: &str = "checkpoint.json";
const CHECKPOINT_BACKUP_FILENAME: &str = "checkpoint.bak";
const LOG_FILENAME: &str = "hlsearch.log";
const MAX_LOG_ENTRIES: usize = 500;

struct UiLogger {
    entries: Arc<Mutex<VecDeque<String>>>,
    writer: Mutex<BufWriter<File>>,
}

impl Log for UiLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let entry = format!("[{}] {}", record.level(), record.args());
        eprintln!("{entry}");

        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = writeln!(writer, "{entry}").and_then(|()| writer.flush()) {
            eprintln!("Failed to write log file: {error}");
        }

        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() == MAX_LOG_ENTRIES {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    fn flush(&self) {}
}

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
    path_scores: Vec<usize>,
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
    start_time: Instant,
    search_results: Option<OutputFile<'static>>, // Use lifetime annotation or Box if necessary in a real scenario
    search_tree: Vec<TreeNode>,
    depth_input: String,
    search_error: Option<String>,
    log_entries: Arc<Mutex<VecDeque<String>>>,
    search_progress: Arc<SearchProgress>,
    search_thread: Option<JoinHandle<Result<OutputFile<'static>, String>>>,
    is_searching: bool,
}

struct TreeNode {
    shift: Option<usize>,
    parent: Option<usize>,
    depth: usize,
    score: Option<usize>,
    children: Vec<usize>,
    y: f32,
}

fn build_search_tree(paths: &[Vec<usize>], path_scores: &[usize]) -> Vec<TreeNode> {
    let mut nodes = vec![TreeNode {
        shift: None,
        parent: None,
        depth: 0,
        score: None,
        children: Vec::new(),
        y: 0.0,
    }];
    let mut child_indices = HashMap::new();

    for (path_index, path) in paths.iter().enumerate() {
        let mut parent = 0;
        for (depth, &shift) in path.iter().enumerate() {
            let key = (parent, shift);
            let child = *child_indices.entry(key).or_insert_with(|| {
                let child = nodes.len();
                nodes.push(TreeNode {
                    shift: Some(shift),
                    parent: Some(parent),
                    depth: depth + 1,
                    score: None,
                    children: Vec::new(),
                    y: 0.0,
                });
                nodes[parent].children.push(child);
                child
            });
            if depth + 1 == path.len() {
                nodes[child].score = path_scores.get(path_index).copied();
            }
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
    fn start_search(&mut self) {
        let start_time = Instant::now();
        self.start_time = start_time;
        self.search_error = None;
        self.search_results = None;
        self.search_tree.clear();
        self.search_progress = Arc::new(SearchProgress::default());

        let cli = self.cli.clone();
        let progress = Arc::clone(&self.search_progress);
        self.search_thread = Some(std::thread::spawn(move || {
            run_search(cli, progress, start_time)
        }));
        self.is_searching = true;
    }

    fn finish_search(&mut self) {
        let Some(search_thread) = self.search_thread.take() else {
            return;
        };
        self.is_searching = false;
        match search_thread.join() {
            Ok(Ok(result)) => {
                self.search_tree =
                    build_search_tree(&result.result.shifts, &result.result.path_scores);
                self.search_results = Some(result);
            }
            Ok(Err(error)) => self.search_error = Some(error),
            Err(_) => self.search_error = Some("Search thread panicked".to_string()),
        }
    }
}

fn run_search(
    cli: Cli,
    progress: Arc<SearchProgress>,
    start_time: Instant,
) -> Result<OutputFile<'static>, String> {
    let all_primes = generate_primes(MAX_PRIME);
    cli.validate(all_primes.len())?;
    info!("HLSearch (Rust) GUI Mode Start");

    let shift_table = build_shift_table(&all_primes[..cli.depth], cli.cols);
    let mut state = State::new(all_primes, cli.cols, shift_table);
    state.checkpoint_interval = cli.checkpoint_interval;
    state.set_progress_reporter(progress);

    let checkpoint_path = cli.output_dir.join(CHECKPOINT_FILENAME);
    let backup_path = cli.output_dir.join(CHECKPOINT_BACKUP_FILENAME);
    let resume_path = if checkpoint_path.exists() {
        Some(checkpoint_path.as_path())
    } else {
        backup_path.exists().then_some(backup_path.as_path())
    };
    state
        .set_shift_path_store(
            &cli.output_dir.join("HLSearch.rocksdb"),
            cli.depth,
            resume_path.is_none(),
        )
        .map_err(|error| error.to_string())?;

    let stored_records = state
        .load_top_scored_paths(cli.depth, 100)
        .map_err(|error| error.to_string())?;
    if !stored_records.is_empty() {
        let (path_scores, shifts): (Vec<_>, Vec<_>) = stored_records.into_iter().unzip();
        let max_score = path_scores[0];
        info!(
            "Displaying {} saved depth {} records ordered by score descending and node ID ascending",
            shifts.len(),
            cli.depth
        );
        return Ok(OutputFile {
            config: OutputConfig {
                mode: match cli.mode {
                    SearchMode::Sequential => "sequential",
                    SearchMode::Parallel => "parallel",
                },
                depth: cli.depth,
                cols: cli.cols,
                elapsed: format!("{:?}", start_time.elapsed()),
            },
            result: OutputResult {
                max_score,
                results: shifts.len(),
                shifts,
                path_scores,
                target_count: 0,
                target_shifts: Vec::new(),
            },
        });
    }

    if cli.depth > 1
        && state
            .load_top_scored_paths(cli.depth - 1, 1)
            .map_err(|error| error.to_string())?
            .is_empty()
    {
        return Err(format!(
            "No records exist for depth {}{}",
            cli.depth,
            if cli.depth > 1 {
                format!(" or depth {}", cli.depth - 1)
            } else {
                String::new()
            }
        ));
    }
    if cli.depth > 1 {
        info!(
            "No saved depth {} records; starting beam search from saved depth {} records",
            cli.depth,
            cli.depth - 1
        );
    } else {
        info!("No saved depth 1 records; starting search from the root");
    }

    let (completed, mut result) = match cli.mode {
        SearchMode::Sequential => {
            let completed = state
                .search_with_checkpoint_interruptible(
                    cli.depth,
                    Some(&checkpoint_path),
                    resume_path,
                    &AtomicBool::new(false),
                )
                .map_err(|error| error.to_string())?;
            (
                completed,
                OutputResult {
                    max_score: state.max_score,
                    results: state.results,
                    path_scores: vec![state.max_score; state.shifts.len()],
                    shifts: state.shifts.clone(),
                    target_count: state.target_count,
                    target_shifts: state.target_shifts.clone(),
                },
            )
        }
        SearchMode::Parallel => {
            let (result, completed) = state
                .search_parallel_interruptible(
                    cli.depth,
                    Some(&checkpoint_path),
                    resume_path,
                    &AtomicBool::new(false),
                )
                .map_err(|error| error.to_string())?;
            (
                completed,
                OutputResult {
                    max_score: result.max_score,
                    results: result.results,
                    path_scores: vec![result.max_score; result.shifts.len()],
                    shifts: result.shifts,
                    target_count: result.target_count,
                    target_shifts: result.target_shifts,
                },
            )
        }
    };

    if completed {
        let stored_records = state
            .load_top_scored_paths(cli.depth, 100)
            .map_err(|error| error.to_string())?;
        if !stored_records.is_empty() {
            let (path_scores, shifts): (Vec<_>, Vec<_>) = stored_records.into_iter().unzip();
            result.max_score = path_scores[0];
            result.results = shifts.len();
            result.path_scores = path_scores;
            result.shifts = shifts;
        }

        for filename in [CHECKPOINT_FILENAME, CHECKPOINT_BACKUP_FILENAME] {
            let checkpoint_path = cli.output_dir.join(filename);
            if checkpoint_path.exists() {
                std::fs::remove_file(&checkpoint_path).map_err(|error| error.to_string())?;
                info!(
                    "Removed completed search checkpoint: {}",
                    checkpoint_path.display()
                );
            }
        }
    }

    Ok(OutputFile {
        config: OutputConfig {
            mode: match cli.mode {
                SearchMode::Sequential => "sequential",
                SearchMode::Parallel => "parallel",
            },
            depth: cli.depth,
            cols: cli.cols,
            elapsed: format!("{:?}", start_time.elapsed()),
        },
        result,
    })
}
fn show_search_tree(ui: &mut egui::Ui, nodes: &[TreeNode]) {
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
                            egui::pos2(x_for_depth(node.depth) - 9.0, rect.top() + node.y),
                        ],
                        egui::Stroke::new(1.0_f32, egui::Color32::from_gray(110)),
                    );
                }
            }

            for node in nodes {
                let center = egui::pos2(x_for_depth(node.depth), rect.top() + node.y);
                painter.circle_filled(center, 8.0, egui::Color32::from_rgb(70, 145, 220));
                let label = match node.shift {
                    Some(shift) if node.children.is_empty() => match node.score {
                        Some(score) => format!("shift {shift} (score {score})"),
                        None => format!("shift {shift}"),
                    },
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
        if self
            .search_thread
            .as_ref()
            .map(JoinHandle::is_finished)
            .unwrap_or(false)
        {
            self.finish_search();
        }
        if self.is_searching {
            ctx.request_repaint_after(Duration::from_millis(100));
        }

        let mut search_requested = false;
        egui::TopBottomPanel::top("top_view")
            .default_height(60.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("TopView");
                    ui.separator();
                    ui.label("Depth");
                    ui.add_enabled(
                        !self.is_searching,
                        egui::TextEdit::singleline(&mut self.depth_input).desired_width(80.0),
                    );
                    search_requested = ui
                        .add_enabled(!self.is_searching, egui::Button::new("Search"))
                        .clicked();
                });
                if let Some(error) = &self.search_error {
                    ui.colored_label(egui::Color32::RED, error);
                }
            });

        egui::TopBottomPanel::bottom("bottom_view")
            .default_height(120.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("BottomView");
                ui.separator();

                let entries = self
                    .log_entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                if entries.is_empty() {
                    ui.label("ログはありません");
                } else {
                    egui::ScrollArea::vertical()
                        .stick_to_bottom(true)
                        .show(ui, |ui| {
                            for entry in entries {
                                ui.monospace(entry);
                            }
                        });
                }
            });

        if search_requested {
            match self.depth_input.parse::<usize>() {
                Ok(depth) => {
                    self.cli.depth = depth;
                    self.start_search();
                }
                Err(_) => self.search_error = Some("Depth must be a positive integer".to_string()),
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("MainView");
            if self.is_searching {
                let progress = self.search_progress.snapshot();
                ui.separator();
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(format!(
                        "Searching: nodes={} best={} hits={} depth={}",
                        progress.node_count, progress.max_score, progress.results, progress.depth
                    ));
                });
                ui.label(format!("Elapsed: {:?}", self.start_time.elapsed()));
            }

            if let Some(result) = &self.search_results {
                ui.separator();
                ui.label(format!(
                    "Search Finished! Max Score: {}",
                    result.result.max_score
                ));

                ui.add_space(10.0);
                ui.label(format!(
                    "Displayed depth {} records: {} ({} tree nodes)",
                    result.config.depth,
                    result.result.shifts.len(),
                    self.search_tree.len().saturating_sub(1)
                ));
                show_search_tree(ui, &self.search_tree);
            } else if !self.is_searching {
                ui.label("Press 'Search' to begin visualization.");
            } else {
                ui.label("Search is running...");
            }
        });
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    std::fs::create_dir_all(&cli.output_dir)?;
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(cli.output_dir.join(LOG_FILENAME))?;
    let log_entries = Arc::new(Mutex::new(VecDeque::new()));
    log::set_boxed_logger(Box::new(UiLogger {
        entries: Arc::clone(&log_entries),
        writer: Mutex::new(BufWriter::new(log_file)),
    }))?;
    log::set_max_level(LevelFilter::Info);
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
                start_time: Instant::now(),
                search_results: None,
                search_tree: Vec::new(),
                depth_input: cli.depth.to_string(),
                search_error: None,
                log_entries: Arc::clone(&log_entries),
                search_progress: Arc::new(SearchProgress::default()),
                search_thread: None,
                is_searching: false,
            };
            Box::new(app)
        }),
    )
    .map_err(|e| {
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
    fn ui_logger_writes_entries_to_text_file() {
        let path = std::env::temp_dir().join(format!(
            "hlsearch-log-test-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let logger = UiLogger {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            writer: Mutex::new(BufWriter::new(File::create(&path).unwrap())),
        };
        let record = log::Record::builder()
            .args(format_args!("search started"))
            .level(Level::Info)
            .target("hlsearch")
            .build();

        logger.log(&record);
        logger.flush();
        drop(logger);

        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "[INFO] search started\n"
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn search_tree_merges_shared_path_prefixes() {
        let tree = build_search_tree(&[vec![2, 1], vec![2, 0], vec![1, 1]], &[9, 8, 7]);

        assert_eq!(tree.len(), 6);
        assert_eq!(tree[0].children.len(), 2);
        assert_eq!(tree[1].shift, Some(2));
        assert_eq!(tree[1].children.len(), 2);
        assert_eq!(tree[1].parent, Some(0));
        assert_eq!(tree[2].score, Some(9));
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
    fn search_runs_when_depth_one_has_no_saved_records() {
        let output_dir = std::env::temp_dir().join(format!(
            "hlsearch-depth-one-search-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&output_dir).unwrap();
        let mut cli = test_cli();
        cli.output_dir = output_dir.clone();

        let result = run_search(cli, Arc::new(SearchProgress::default()), Instant::now());

        assert!(result.is_ok());
        std::fs::remove_dir_all(output_dir).unwrap();
    }

    #[test]
    fn output_result_serializes_best_score_shifts() {
        let json = serde_json::to_value(OutputResult {
            max_score: 447,
            results: 1,
            shifts: vec![vec![1, 1, 4]],
            path_scores: vec![447],
            target_count: 0,
            target_shifts: Vec::new(),
        })
        .unwrap();

        assert_eq!(json["shifts"], serde_json::json!([[1, 1, 4]]));
    }
}
