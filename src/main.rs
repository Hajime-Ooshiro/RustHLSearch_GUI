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
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const CHECKPOINT_FILENAME: &str = "checkpoint.json";
const CHECKPOINT_BACKUP_FILENAME: &str = "checkpoint.bak";
const LOG_FILENAME: &str = "hlsearch.log";
const MAX_LOG_ENTRIES: usize = 500;
const MAX_DISPLAY_RESULTS: usize = 30;

fn format_total_count(total_count: u64) -> String {
    if total_count > 1_000 {
        format!("{:.2e}", total_count as f64)
    } else {
        total_count.to_string()
    }
}

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
    total_count: u64,
    shifts: Vec<Vec<usize>>,
    path_scores: Vec<usize>,
    target_count: usize,
    target_shifts: Vec<Vec<usize>>,
}

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "HLSearch: 素数シフト探索 GUI (Rust版)",
    long_about = "CLI引数はGUIの初期設定に使用されます。起動後、Searchボタンで探索を開始します。"
)]
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

struct AppState {
    cli: Cli,
    operation_started_at: Option<Instant>,
    elapsed: Option<Duration>,
    search_results: Option<OutputFile<'static>>, // Use lifetime annotation or Box if necessary in a real scenario
    search_tree: Vec<TreeNode>,
    depth_input: usize,
    search_error: Option<String>,
    log_entries: Arc<Mutex<VecDeque<String>>>,
    search_progress: Arc<SearchProgress>,
    interrupted: Arc<AtomicBool>,
    exit_requested: Arc<AtomicBool>,
    force_exit: Arc<AtomicBool>,
    search_thread: Option<JoinHandle<Result<OutputFile<'static>, String>>>,
    is_searching: bool,
    maximum_stored_depth: Result<Option<usize>, String>,
}

fn is_checkpoint_interrupt_error(error: &str) -> bool {
    error.starts_with("検索が中断されました。チェックポイントを保存しました:")
}

type AppOperation =
    fn(Cli, Arc<SearchProgress>, Arc<AtomicBool>, Instant) -> Result<OutputFile<'static>, String>;

struct TreeNode {
    shift: Option<usize>,
    parent: Option<usize>,
    depth: usize,
    score: Option<usize>,
    children: Vec<usize>,
    y: f32,
    is_max_score_path: bool,
}

fn build_search_tree(paths: &[Vec<usize>], path_scores: &[usize]) -> Vec<TreeNode> {
    let mut nodes = vec![TreeNode {
        shift: None,
        parent: None,
        depth: 0,
        score: None,
        children: Vec::new(),
        y: 0.0,
        is_max_score_path: false,
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
                    is_max_score_path: false,
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
    mark_max_score_paths(&mut nodes);
    nodes
}

fn mark_max_score_paths(nodes: &mut [TreeNode]) {
    let Some(max_score) = nodes.iter().filter_map(|node| node.score).max() else {
        return;
    };

    for leaf_index in 0..nodes.len() {
        if nodes[leaf_index].score != Some(max_score) {
            continue;
        }

        let mut node_index = Some(leaf_index);
        while let Some(index) = node_index {
            nodes[index].is_max_score_path = true;
            node_index = nodes[index].parent;
        }
    }
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
    fn request_interrupt(&mut self) {
        self.interrupted.store(true, Ordering::SeqCst);
    }

    fn start_operation(&mut self, operation: AppOperation) {
        let start_time = Instant::now();
        self.operation_started_at = Some(start_time);
        self.elapsed = None;
        self.search_error = None;
        self.search_results = None;
        self.search_tree.clear();
        self.search_progress = Arc::new(SearchProgress::default());
        self.interrupted.store(false, Ordering::SeqCst);

        let cli = self.cli.clone();
        let progress = Arc::clone(&self.search_progress);
        let interrupted = Arc::clone(&self.interrupted);
        self.search_thread = Some(std::thread::spawn(move || {
            operation(cli, progress, interrupted, start_time)
        }));
        self.is_searching = true;
    }

    fn start_search(&mut self) {
        self.start_operation(run_search);
    }

    fn start_display(&mut self) {
        self.start_operation(load_saved_results);
    }

    fn finish_search(&mut self) {
        let Some(search_thread) = self.search_thread.take() else {
            return;
        };
        self.elapsed = self.operation_started_at.map(|started| started.elapsed());
        self.operation_started_at = None;
        self.is_searching = false;
        match search_thread.join() {
            Ok(Ok(result)) => {
                self.search_tree =
                    build_search_tree(&result.result.shifts, &result.result.path_scores);
                self.search_results = Some(result);
            }
            Ok(Err(error)) => self.search_error = Some(error),
            Err(_) => self.search_error = Some("検索スレッドが異常終了しました".to_string()),
        }
    }
}

fn japanese_font_path() -> io::Result<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(windows_dir) = std::env::var_os("WINDIR") {
        let fonts_dir = PathBuf::from(windows_dir).join("Fonts");
        for font_name in [
            "NotoSansJP-VF.ttf",
            "YuGothR.ttc",
            "meiryo.ttc",
            "BIZ-UDGothicR.ttc",
        ] {
            candidates.push(fonts_dir.join(font_name));
        }
    }

    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        let fonts_dir = PathBuf::from(local_app_data).join("Microsoft/Windows/Fonts");
        candidates.push(fonts_dir.join("NotoSansJP-VF.ttf"));
    }

    candidates.extend([
        PathBuf::from("/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc"),
        PathBuf::from("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        PathBuf::from("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
        PathBuf::from("/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf"),
        PathBuf::from("/usr/share/fonts/opentype/ipafont-gothic/ipag.ttf"),
    ]);

    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "日本語フォントが見つかりません。Noto Sans JP などの日本語フォントをインストールしてください。",
            )
        })
}

fn japanese_font_definitions(font_data: egui::FontData) -> egui::FontDefinitions {
    let mut fonts = egui::FontDefinitions::default();
    fonts
        .font_data
        .insert("Japanese".to_owned(), font_data.into());
    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .insert(0, "Japanese".to_owned());
    }
    fonts
}

fn has_japanese_ui_glyphs(ctx: &egui::Context, font_definitions: &egui::FontDefinitions) -> bool {
    let mut has_glyphs = false;
    let _ = ctx.run_ui(egui::RawInput::default(), |ctx| {
        ctx.set_fonts(font_definitions.clone());
    });
    let _ = ctx.run_ui(egui::RawInput::default(), |ctx| {
        has_glyphs = ctx.fonts_mut(|fonts| {
            fonts.has_glyphs(&egui::FontId::proportional(14.0), "日本語の表示確認")
        });
    });
    has_glyphs
}

fn run_search(
    cli: Cli,
    progress: Arc<SearchProgress>,
    interrupted: Arc<AtomicBool>,
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

    if resume_path.is_none() {
        state
            .set_search_complete(false)
            .map_err(|error| error.to_string())?;
    }

    if cli.depth > 1 {
        info!(
            "Starting depth {} beam search from saved depth {} records when available, otherwise from the root",
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
                    &interrupted,
                )
                .map_err(|error| error.to_string())?;
            (
                completed,
                OutputResult {
                    max_score: state.max_score,
                    results: state.results,
                    total_count: 0,
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
                    &interrupted,
                )
                .map_err(|error| error.to_string())?;
            (
                completed,
                OutputResult {
                    max_score: result.max_score,
                    results: result.results,
                    total_count: 0,
                    path_scores: vec![result.max_score; result.shifts.len()],
                    shifts: result.shifts,
                    target_count: result.target_count,
                    target_shifts: result.target_shifts,
                },
            )
        }
    };

    if !completed {
        return Err(format!(
            "検索が中断されました。チェックポイントを保存しました: {}",
            checkpoint_path.display()
        ));
    }

    state
        .set_search_complete(true)
        .map_err(|error| error.to_string())?;
    let stored_records = state
        .load_top_scored_paths(cli.depth, MAX_DISPLAY_RESULTS)
        .map_err(|error| error.to_string())?;
    if !stored_records.is_empty() {
        let (path_scores, shifts): (Vec<_>, Vec<_>) = stored_records.into_iter().unzip();
        result.max_score = path_scores[0];
        result.results = shifts.len();
        result.path_scores = path_scores;
        result.shifts = shifts;
    }
    result.total_count = state
        .count_depth_paths(cli.depth)
        .map_err(|error| error.to_string())?;

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

fn load_saved_results(
    cli: Cli,
    _progress: Arc<SearchProgress>,
    _interrupted: Arc<AtomicBool>,
    start_time: Instant,
) -> Result<OutputFile<'static>, String> {
    let all_primes = generate_primes(MAX_PRIME);
    cli.validate(all_primes.len())?;

    let shift_table = build_shift_table(&all_primes[..cli.depth], cli.cols);
    let mut state = State::new(all_primes, cli.cols, shift_table);
    state
        .set_shift_path_store(&cli.output_dir.join("HLSearch.rocksdb"), cli.depth, false)
        .map_err(|error| error.to_string())?;

    let stored_records = state
        .load_top_scored_paths(cli.depth, MAX_DISPLAY_RESULTS)
        .map_err(|error| error.to_string())?;
    let total_count = state
        .count_depth_paths(cli.depth)
        .map_err(|error| error.to_string())?;
    let (path_scores, shifts): (Vec<_>, Vec<_>) = stored_records.into_iter().unzip();
    let max_score = path_scores.first().copied().unwrap_or(0);
    info!(
        "Displaying {} saved depth {} records ordered by score descending and node ID ascending",
        shifts.len(),
        cli.depth
    );

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
        result: OutputResult {
            max_score,
            results: shifts.len(),
            total_count,
            shifts,
            path_scores,
            target_count: 0,
            target_shifts: Vec::new(),
        },
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
                        egui::Stroke::new(
                            if node.is_max_score_path { 2.0 } else { 1.0 },
                            if node.is_max_score_path {
                                egui::Color32::RED
                            } else {
                                egui::Color32::from_gray(110)
                            },
                        ),
                    );
                }
            }

            for node in nodes {
                let center = egui::pos2(x_for_depth(node.depth), rect.top() + node.y);
                painter.circle_filled(
                    center,
                    8.0,
                    if node.is_max_score_path {
                        egui::Color32::RED
                    } else {
                        egui::Color32::from_rgb(70, 145, 220)
                    },
                );
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
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.force_exit.load(Ordering::SeqCst) {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            return;
        }

        if self.exit_requested.load(Ordering::SeqCst) {
            if self
                .search_thread
                .as_ref()
                .map(JoinHandle::is_finished)
                .unwrap_or(true)
            {
                let no_worker_was_running = self.search_thread.is_none();
                if !no_worker_was_running {
                    self.finish_search();
                }
                let can_close = no_worker_was_running
                    || self.search_results.is_some()
                    || self
                        .search_error
                        .as_deref()
                        .is_some_and(is_checkpoint_interrupt_error);
                if can_close {
                    ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                } else {
                    self.exit_requested.store(false, Ordering::SeqCst);
                    ctx.request_repaint();
                }
                return;
            }
            ctx.request_repaint_after(Duration::from_millis(100));
            return;
        }

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
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let mut explore_requested = false;
        let mut search_requested = false;
        let mut interrupt_requested = false;
        egui::Panel::top("top_view")
            .default_size(60.0)
            .resizable(true)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("探索設定");
                    ui.separator();
                    ui.label("深さ");
                    ui.add_enabled(
                        !self.is_searching,
                        egui::DragValue::new(&mut self.depth_input)
                            .range(0..=249)
                            .speed(1.0),
                    );
                    explore_requested = ui
                        .add_enabled(!self.is_searching, egui::Button::new("探索"))
                        .clicked();
                    search_requested = ui
                        .add_enabled(!self.is_searching, egui::Button::new("検索"))
                        .clicked();
                    interrupt_requested = ui
                        .add_enabled(self.is_searching, egui::Button::new("中断"))
                        .clicked();
                });
                if let Some(error) = &self.search_error {
                    ui.colored_label(egui::Color32::RED, error);
                }
                if let Some(result) = &self.search_results {
                    ui.label(format!(
                        "表示件数 / 総件数: {} / {}",
                        result.result.shifts.len(),
                        format_total_count(result.result.total_count)
                    ));
                }
            });

        egui::Panel::bottom("bottom_view")
            .default_size(120.0)
            .resizable(true)
            .show(ui, |ui| {
                ui.heading("ログ");
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

        if explore_requested || search_requested {
            self.cli.depth = self.depth_input;
            if explore_requested {
                self.start_search();
            } else {
                self.start_display();
            }
        }
        if interrupt_requested {
            self.request_interrupt();
        }

        egui::CentralPanel::default().show(ui, |ui| {
            ui.heading("探索結果");
            match &self.maximum_stored_depth {
                Ok(Some(depth)) => ui.label(format!("保存済み RocksDB の最大深さ: {depth}")),
                Ok(None) => ui.label("保存済み RocksDB: なし"),
                Err(error) => ui.colored_label(
                    egui::Color32::RED,
                    format!("RocksDB の保存済み深さを取得できません: {error}"),
                ),
            };
            if self.is_searching {
                let progress = self.search_progress.snapshot();
                ui.separator();
                let depth_fraction =
                    (progress.depth as f32 / self.cli.depth.max(1) as f32).clamp(0.0, 1.0);
                ui.add(
                    egui::ProgressBar::new(depth_fraction)
                        .text(format!("深さ {}/{}", progress.depth, self.cli.depth)),
                );
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label(format!(
                        "検索中: ノード={} 最良={} ヒット={} prefix={}/{}",
                        progress.node_count,
                        progress.max_score,
                        progress.results,
                        progress.prefix_done,
                        progress.prefix_total
                    ));
                });
            }
            if let Some(started_at) = self.operation_started_at {
                ui.label(format!("経過時間: {:?}", started_at.elapsed()));
            }

            if let Some(result) = &self.search_results {
                ui.separator();
                ui.label(format!(
                    "検索完了！ 最大スコア: {}",
                    result.result.max_score
                ));

                ui.add_space(10.0);
                ui.label(format!(
                    "深さ {} の結果: {}件 (ツリー ノード {}個)",
                    result.config.depth,
                    result.result.shifts.len(),
                    self.search_tree.len().saturating_sub(1)
                ));
                ui.label(format!("経過時間: {}", result.config.elapsed));
                show_search_tree(ui, &self.search_tree);
            } else if !self.is_searching {
                if let Some(elapsed) = self.elapsed {
                    ui.label(format!("経過時間: {:?}", elapsed));
                }
                ui.label("『検索』を押して可視化を開始してください。");
            } else {
                ui.label("検索実行中...");
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
    let exit_requested = Arc::new(AtomicBool::new(false));
    let force_exit = Arc::new(AtomicBool::new(false));
    let ctrl_c_count = Arc::new(AtomicUsize::new(0));
    {
        let interrupted = Arc::clone(&interrupted);
        let exit_requested = Arc::clone(&exit_requested);
        let force_exit = Arc::clone(&force_exit);
        let ctrl_c_count = Arc::clone(&ctrl_c_count);
        ctrlc::set_handler(move || {
            let count = ctrl_c_count.fetch_add(1, Ordering::SeqCst) + 1;
            interrupted.store(true, Ordering::SeqCst);
            exit_requested.store(true, Ordering::SeqCst);
            if count >= 2 {
                force_exit.store(true, Ordering::SeqCst);
            }
        })?;
    }

    let font_path = japanese_font_path()?;
    let japanese_fonts =
        japanese_font_definitions(egui::FontData::from_owned(std::fs::read(&font_path)?));
    let font_check_context = egui::Context::default();
    if !has_japanese_ui_glyphs(&font_check_context, &japanese_fonts) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "日本語フォントで必要な文字を表示できません: {}",
                font_path.display()
            ),
        )
        .into());
    }

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        renderer: eframe::Renderer::Glow,
        ..Default::default()
    };

    eframe::run_native(
        "HLSearch 可視化ツール",
        options,
        Box::new(move |cc| {
            cc.egui_ctx.set_fonts(japanese_fonts);
            let maximum_stored_depth =
                rocksdb_output::maximum_stored_depth(&cli.output_dir.join("HLSearch.rocksdb"));
            let app = AppState {
                cli: cli.clone(),
                operation_started_at: None,
                elapsed: None,
                search_results: None,
                search_tree: Vec::new(),
                depth_input: cli.depth,
                search_error: None,
                log_entries: Arc::clone(&log_entries),
                search_progress: Arc::new(SearchProgress::default()),
                interrupted: Arc::clone(&interrupted),
                exit_requested: Arc::clone(&exit_requested),
                force_exit: Arc::clone(&force_exit),
                search_thread: None,
                is_searching: false,
                maximum_stored_depth,
            };
            Ok(Box::new(app) as Box<dyn eframe::App>)
        }),
    )
    .map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to run eframe: {}", e),
        )
    })?;

    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_checkpoint_interrupt_errors() {
        assert!(is_checkpoint_interrupt_error(
            "検索が中断されました。チェックポイントを保存しました: checkpoint.json"
        ));
        assert!(!is_checkpoint_interrupt_error(
            "RocksDB の保存に失敗しました"
        ));
    }

    #[test]
    fn japanese_font_supports_ui_glyphs_when_available() {
        let Ok(path) = japanese_font_path() else {
            return;
        };
        let context = egui::Context::default();
        let fonts =
            japanese_font_definitions(egui::FontData::from_owned(std::fs::read(path).unwrap()));

        assert!(has_japanese_ui_glyphs(&context, &fonts));
    }

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
        assert!(tree[0].is_max_score_path);
        assert!(tree[1].is_max_score_path);
        assert!(tree[2].is_max_score_path);
        assert!(!tree[3].is_max_score_path);
        assert!(!tree[4].is_max_score_path);
        assert!(!tree[5].is_max_score_path);
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
    fn search_runs_from_root_when_previous_depth_has_no_saved_records() {
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
        cli.depth = 2;
        cli.output_dir = output_dir.clone();

        let result = run_search(
            cli.clone(),
            Arc::new(SearchProgress::default()),
            Arc::new(AtomicBool::new(false)),
            Instant::now(),
        );

        assert!(result.is_ok());
        let cached_result = run_search(
            cli,
            Arc::new(SearchProgress::default()),
            Arc::new(AtomicBool::new(false)),
            Instant::now(),
        )
        .unwrap();
        assert!(cached_result.result.shifts.is_empty());
        std::fs::remove_dir_all(output_dir).unwrap();
    }

    #[test]
    fn incomplete_saved_paths_do_not_bypass_checkpoint_resume() {
        let output_dir = std::env::temp_dir().join(format!(
            "hlsearch-resume-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&output_dir).unwrap();

        let mut cli = test_cli();
        cli.output_dir = output_dir.clone();
        let primes = generate_primes(MAX_PRIME);
        let shift_table = build_shift_table(&primes[..cli.depth], cli.cols);
        let state = State::new(primes.clone(), cli.cols, shift_table.clone());
        let store_path = output_dir.join("HLSearch.rocksdb");
        let store = crate::rocksdb_output::ShiftPathStore::create(
            &store_path,
            cli.depth,
            true,
            state.compute_config_hash(),
        )
        .unwrap();
        store.append(447, &[0]).unwrap();
        store.set_search_complete(false).unwrap();
        drop(store);

        let mut state = State::new(primes, cli.cols, shift_table);
        state
            .set_shift_path_store(&store_path, cli.depth, false)
            .unwrap();

        let checkpoint_path = output_dir.join(CHECKPOINT_FILENAME);
        let interrupted = AtomicBool::new(true);
        state
            .search_with_checkpoint_interruptible(
                cli.depth,
                Some(&checkpoint_path),
                None,
                &interrupted,
            )
            .unwrap();
        drop(state);

        let result = run_search(
            cli,
            Arc::new(SearchProgress::default()),
            Arc::new(AtomicBool::new(true)),
            Instant::now(),
        );

        let error = result.err().unwrap();
        assert!(error.contains("検索が中断されました。チェックポイントを保存しました"));
        assert!(checkpoint_path.exists());
        std::fs::remove_dir_all(output_dir).unwrap();
    }

    #[test]
    fn output_result_serializes_best_score_shifts() {
        let json = serde_json::to_value(OutputResult {
            max_score: 447,
            results: 1,
            total_count: 1,
            shifts: vec![vec![1, 1, 4]],
            path_scores: vec![447],
            target_count: 0,
            target_shifts: Vec::new(),
        })
        .unwrap();

        assert_eq!(json["shifts"], serde_json::json!([[1, 1, 4]]));
    }

    #[test]
    fn format_total_count_uses_scientific_notation_above_1000() {
        assert_eq!(format_total_count(1_000), "1000");
        assert_eq!(format_total_count(1_001), "1.00e3");
        assert_eq!(format_total_count(12_345), "1.23e4");
    }
}
