use crate::bitmask::BitMask;
use crate::rocksdb_output::ShiftPathStore;
use indicatif::{HumanCount, ProgressBar};
use log::{debug, info};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// 基底行の生成と補集合シフトテーブルの作成
pub fn build_shift_table(primes: &[usize], cols: usize) -> Vec<Vec<BitMask>> {
    let mut shift_table = Vec::with_capacity(primes.len());

    for &p in primes {
        let mut complement_shifts = Vec::with_capacity(p);
        for k in 0..p {
            let mut mask = BitMask::new_ones(cols);
            for col in (k..cols).step_by(p) {
                mask.set(col, false);
            }
            complement_shifts.push(mask);
        }
        shift_table.push(complement_shifts);
    }

    shift_table
}

#[derive(Clone, Deserialize, Serialize)]
struct Frame {
    level: usize,
    next_idx: usize,
}

fn default_checkpoint_mode() -> SearchMode {
    SearchMode::Sequential
}

#[derive(Clone, Deserialize, Serialize)]
struct ParallelInProgress {
    work_index: usize,
    stack: Vec<Frame>,
    key: Vec<usize>,
}

type ShiftPaths = Vec<Vec<usize>>;

#[derive(Deserialize, Serialize)]
struct Checkpoint {
    #[serde(default = "default_checkpoint_mode")]
    mode: SearchMode,
    depth: usize,
    stack: Vec<Frame>,
    key: Vec<usize>,
    max_score: usize,
    results: usize,
    #[serde(default)]
    target_count: usize,
    node_count: u64,
    #[serde(default)]
    split_depth: usize,
    #[serde(default)]
    completed: Vec<usize>,
    #[serde(default)]
    in_progress: Vec<ParallelInProgress>,
    #[serde(default)]
    pending_prefixes: Vec<usize>,
    #[serde(default)]
    work_prefixes: Option<ShiftPaths>,
    #[serde(default)]
    config_hash: u64,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, clap::ValueEnum, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    Sequential,
    #[default]
    Parallel,
}

#[derive(Clone, Debug, Default)]
pub struct SharedResults {
    pub max_score: usize,
    pub results: usize,
    pub shifts: Vec<Vec<usize>>,
    pub target_count: usize,
    pub target_shifts: Vec<Vec<usize>>,
    pub node_count: u64,
}

struct ParallelResults {
    max_score: AtomicUsize,
    results: Mutex<SharedResults>,
    target: usize,
    shift_path_store: Option<ShiftPathStore>,
}

impl ParallelResults {
    fn record_target(&self, depth: usize, score: usize, key: &[usize]) -> Result<(), String> {
        if depth == 249 && score == self.target {
            let mut results = self.results.lock().unwrap();
            let target_count = results.target_count + 1;
            if let Some(store) = &self.shift_path_store {
                store.append_target(target_count, key)?;
            }
            results.target_count = target_count;
            results.target_shifts.push(key.to_vec());
        }
        Ok(())
    }

    fn record_path(&self, score: usize, key: &[usize]) -> Result<(), String> {
        if score < self.target {
            return Ok(());
        }

        let mut results = self.results.lock().unwrap();
        let current = self.max_score.load(Ordering::Relaxed);
        if score < current {
            return Ok(());
        }
        if score > current {
            self.max_score.store(score, Ordering::Relaxed);
            results.results = 1;
            results.shifts.clear();
            results.shifts.push(key.to_vec());
            return Ok(());
        }

        let is_duplicate = if results.results < 20 {
            results
                .shifts
                .iter()
                .any(|existing| existing.as_slice() == key)
        } else {
            results
                .shifts
                .binary_search_by(|existing| {
                    existing.as_slice().cmp(key)
                })
                .is_ok()
        };

        if !is_duplicate {
            results.results += 1;
            results.shifts.push(key.to_vec());
            if results.results % 100 == 0 {
                results.shifts.sort();
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> SharedResults {
        let mut results = self.results.lock().unwrap().clone();
        results.max_score = self.max_score.load(Ordering::Relaxed);
        results
    }
}

#[derive(Clone)]
struct WorkItem {
    key: Vec<usize>,
    base_mask: BitMask,
}

struct ParallelProgress {
    completed: HashSet<usize>,
    in_progress: HashMap<usize, ParallelInProgress>,
}

struct ParallelSearchCtx<'a> {
    depth: usize,
    split_depth: usize,
    work_prefixes: &'a ShiftPaths,
    results: &'a ParallelResults,
    node_count: &'a AtomicU64,
    progress: &'a Mutex<ParallelProgress>,
    checkpoint_path: Option<&'a Path>,
    interrupted: &'a AtomicBool,
    checkpoint_lock: Mutex<()>,
    last_checkpoint_nodes: AtomicU64,
    progress_bar: Option<ProgressBar>,
}

struct ParallelJob {
    index: usize,
    work_item: WorkItem,
    resume: Option<(Vec<Frame>, Vec<usize>)>,
}

pub struct State {
    pub primes: Vec<usize>,
    pub key: Vec<usize>,
    pub zero_mask: BitMask,
    pub max_score: usize,
    pub target: usize,
    pub target_count: usize,
    pub target_shifts: Vec<Vec<usize>>,
    pub results: usize,
    pub shifts: Vec<Vec<usize>>,
    pub node_count: u64,
    pub checkpoint_interval: u64,
    shift_table: Vec<Vec<BitMask>>,
    shift_path_store: Option<ShiftPathStore>,
    progress_bar: Option<ProgressBar>,
}

impl State {
    pub fn new(primes: Vec<usize>, cols: usize, shift_table: Vec<Vec<BitMask>>) -> Self {
        State {
            primes,
            key: Vec::new(),
            zero_mask: BitMask::new_ones(cols),
            max_score: 0,
            target: 447,
            target_count: 0,
            target_shifts: Vec::new(),
            results: 0,
            shifts: Vec::new(),
            node_count: 0,
            checkpoint_interval: 100_000,
            shift_table,
            shift_path_store: None,
            progress_bar: None,
        }
    }

    pub fn set_progress_bar(&mut self, progress_bar: ProgressBar) {
        self.progress_bar = Some(progress_bar);
    }

    pub fn finish_progress(&self, completed: bool) {
        let Some(progress_bar) = &self.progress_bar else {
            return;
        };
        let status = if completed {
            "探索完了"
        } else {
            "探索中断"
        };
        progress_bar.finish_with_message(format!(
            "{}: nodes={} best={} hits={}",
            status,
            HumanCount(self.node_count),
            self.max_score,
            self.results
        ));
    }

    fn update_progress(
        progress_bar: Option<&ProgressBar>,
        nodes: u64,
        max_score: usize,
        results: usize,
        depth: usize,
    ) {
        let Some(progress_bar) = progress_bar else {
            return;
        };
        progress_bar.set_position(nodes);
        progress_bar.set_message(format!(
            "探索中: nodes={} best={} hits={} depth={}",
            HumanCount(nodes),
            max_score,
            results,
            depth
        ));
    }

    pub fn set_shift_path_store(
        &mut self,
        path: &Path,
        depth: usize,
        clear_existing: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config_hash = self.compute_config_hash();
        self.shift_path_store = Some(
            ShiftPathStore::create(path, depth, clear_existing, config_hash)
                .map_err(std::io::Error::other)?,
        );
        Ok(())
    }

    fn load_checkpoint_paths(
        &self,
        expected_max_score: usize,
        expected_results: usize,
        expected_target_count: usize,
    ) -> Result<(ShiftPaths, ShiftPaths), String> {
        let Some(store) = &self.shift_path_store else {
            if expected_results == 0 && expected_target_count == 0 {
                return Ok((Vec::new(), Vec::new()));
            }
            return Err(
                "チェックポイントの結果パスを復元するための RocksDB 出力がありません".to_string(),
            );
        };
        let shifts = if expected_results == 0 {
            Vec::new()
        } else {
            store.load_result_paths(expected_max_score)?
        };
        let target_shifts = store.load_target_paths()?;
        if shifts.len() < expected_results {
            log::warn!(
                "RocksDB の結果パス数が期待値より少ないため、DB から読み込める分を使用します (shifts={}/{})",
                shifts.len(),
                expected_results
            );
        }
        if target_shifts.len() < expected_target_count {
            log::warn!(
                "RocksDB のターゲットパス数が期待値より少ないため、DB から読み込める分を使用します (targets={}/{})",
                target_shifts.len(),
                expected_target_count
            );
        }
        Ok((shifts, target_shifts))
    }

    fn load_previous_depth_paths(
        &self,
        depth: usize,
    ) -> Result<Option<ShiftPaths>, Box<dyn std::error::Error>> {
        if depth == 0 {
            return Ok(None);
        }
        let Some(store) = self.shift_path_store.as_ref() else {
            return Ok(None);
        };
        let paths = store
            .load_full_depth_paths(depth - 1)
            .map_err(std::io::Error::other)?;
        Ok((!paths.is_empty()).then_some(paths))
    }

    fn aggregate_leaf_result(&mut self, score: usize, key: &[usize]) -> Result<(), String> {
        if score < self.target {
            info!(
                "target break (depth={} score={}, key={:?})",
                self.key.len(),
                score,
                key
            );
            return Ok(());
        }
        if score > self.max_score {
            self.max_score = score;
            self.results = 1;
            self.shifts.clear();
            self.shifts.push(key.to_vec());
            info!("best level={} key={:?} score={}", key.len() - 1, key, score);
        } else if score == self.max_score {
            self.results += 1;
            self.shifts.push(key.to_vec());
            debug!("best level={} key={:?} score={}", key.len() - 1, key, score);
        }
        Ok(())
    }

    fn aggregate_target_result(
        &mut self,
        depth: usize,
        score: usize,
        key: &[usize],
    ) -> Result<(), String> {
        if depth == 249 && score == self.target {
            let target_count = self.target_count + 1;
            if let Some(store) = &self.shift_path_store {
                store.append_target(target_count, key)?;
            }
            self.target_count = target_count;
            self.target_shifts.push(key.to_vec());
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn search_with_checkpoint(
        &mut self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        resume_path: Option<&Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let interrupted = AtomicBool::new(false);
        self.search_with_checkpoint_interruptible(
            depth,
            checkpoint_path,
            resume_path,
            &interrupted,
        )?;
        Ok(())
    }

    pub fn search_with_checkpoint_interruptible(
        &mut self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        resume_path: Option<&Path>,
        interrupted: &AtomicBool,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let (mut stack, mut pending_prefixes) = if let Some(path) = resume_path {
            let checkpoint = Self::load_checkpoint(path)?;
            if checkpoint.mode != SearchMode::Sequential {
                return Err(
                    "並列モードのチェックポイントは --mode parallel でのみ再開できます".into(),
                );
            }
            self.apply_checkpoint_config(&checkpoint, depth)?;
            self.key = checkpoint.key.clone();
            self.max_score = checkpoint.max_score;
            self.results = checkpoint.results;
            self.target_count = checkpoint.target_count;
            (self.shifts, self.target_shifts) = self
                .load_checkpoint_paths(
                    checkpoint.max_score,
                    checkpoint.results,
                    checkpoint.target_count,
                )
                .map_err(std::io::Error::other)?;
            self.node_count = checkpoint.node_count;
            info!(
                "チェックポイントから探索を再開しました (nodes={})",
                checkpoint.node_count
            );

            (
                self.restore_stack(&checkpoint.stack)?,
                Vec::new(),
            )
        } else if let Some(previous_paths) = self.load_previous_depth_paths(depth)? {
            info!(
                "depth={} の開始パスとして depth={} の保存済みパス {} 件を使用します",
                depth,
                depth - 1,
                previous_paths.len()
            );
            (Vec::new(), previous_paths.into_iter().rev().collect())
        } else {
            (
                vec![Frame {
                    level: 0,
                    next_idx: self.primes[0],
                }],
                Vec::new(),
            )
        };
        let mut masks = self.rebuild_masks(depth, &self.key);
        let mut checkpoint_due = false;
        Self::update_progress(
            self.progress_bar.as_ref(),
            self.node_count,
            self.max_score,
            self.results,
            self.key.len(),
        );

        loop {
            if interrupted.load(Ordering::SeqCst) {
                if let Some(path) = checkpoint_path {
                    self.write_checkpoint(path, depth, &stack)?;
                }
                return Ok(false);
            }
            if checkpoint_due {
                if let Some(path) = checkpoint_path {
                    self.write_checkpoint(path, depth, &stack)?;
                }
                checkpoint_due = false;
            }
            if stack.is_empty() {
                let Some(prefix) = pending_prefixes.pop() else {
                    break;
                };
                self.key = prefix;
                masks = self.rebuild_masks(depth, &self.key);
                stack.push(Frame {
                    level: depth - 1,
                    next_idx: self.primes[depth - 1],
                });
                continue;
            }
            let frame = stack.last_mut().expect("探索スタックが空です");
            if frame.next_idx == 0 {
                stack.pop();
                if stack.last().is_some() {
                    self.key.pop();
                }
                continue;
            }

            frame.next_idx -= 1;
            let i = frame.next_idx;
            let level = frame.level;
            self.key.push(i);
            self.node_count += 1;

            let (base_masks, node_masks) = masks.split_at_mut(level + 1);
            let count =
                node_masks[0].bitand_into_count(&base_masks[level], &self.shift_table[level][i]);

            if self.node_count.is_multiple_of(self.checkpoint_interval) {
                Self::update_progress(
                    self.progress_bar.as_ref(),
                    self.node_count,
                    self.max_score,
                    self.results,
                    self.key.len(),
                );
                info!(
                    "探索経過: nodes={} best={} hits={} depth={}",
                    self.node_count,
                    self.max_score,
                    self.results,
                    self.key.len()
                );
                checkpoint_due = true;
            }

            if count < self.target {
                info!(
                    "target break (depth={} count={}, key={:?})",
                    self.key.len(),
                    count,
                    self.key
                );
                self.key.pop();
                continue;
            }

            if let Some(store) = &self.shift_path_store {
                store
                    .append(count, &self.key)
                    .map_err(std::io::Error::other)?;
            }

            if level + 1 >= depth {
                let key = self.key.clone();
                self.aggregate_target_result(depth, count, &key)
                    .map_err(std::io::Error::other)?;
                self.aggregate_leaf_result(count, &key)
                    .map_err(std::io::Error::other)?;
                self.key.pop();
                continue;
            }

            stack.push(Frame {
                level: level + 1,
                next_idx: self.primes[level + 1],
            });
        }
        if let Some(store) = &self.shift_path_store {
            store.flush().map_err(std::io::Error::other)?;
        }
        info!("探索完了 (nodes={})", self.node_count);
        if let Some(path) = checkpoint_path {
            self.write_checkpoint(path, depth, &stack)?;
        }
        Ok(!interrupted.load(Ordering::SeqCst))
    }

    fn write_checkpoint(
        &self,
        path: &Path,
        depth: usize,
        stack: &[Frame],
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.write_checkpoint_file(
            path,
            &Checkpoint {
                mode: SearchMode::Sequential,
                depth,
                stack: stack.to_vec(),
                key: self.key.clone(),
                max_score: self.max_score,
                results: self.results,
                target_count: self.target_count,
                node_count: self.node_count,
                split_depth: 0,
                completed: Vec::new(),
                in_progress: Vec::new(),
                pending_prefixes: Vec::new(),
                work_prefixes: None,
                config_hash: self.compute_config_hash(),
            },
        )
    }

    fn write_checkpoint_file(
        &self,
        path: &Path,
        checkpoint: &Checkpoint,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let temporary_path = path.with_extension("tmp");
        let file = fs::File::create(&temporary_path)?;
        serde_json::to_writer(file, checkpoint)?;
        if path.exists() {
            let backup_path = path.with_extension("bak");
            if backup_path.exists() {
                fs::remove_file(&backup_path)?;
            }
            fs::rename(path, backup_path)?;
        }
        fs::rename(temporary_path, path)?;
        Ok(())
    }

    fn load_checkpoint(path: &Path) -> Result<Checkpoint, Box<dyn std::error::Error>> {
        Ok(serde_json::from_reader(std::fs::File::open(path)?)?)
    }

    fn apply_checkpoint_config(
        &self,
        checkpoint: &Checkpoint,
        depth: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if checkpoint.depth != depth {
            return Err(format!(
                "チェックポイントの探索深さが現在の設定と一致しません (depth={})",
                checkpoint.depth
            )
            .into());
        }
        let current_hash = self.compute_config_hash();
        if checkpoint.config_hash != 0 && checkpoint.config_hash != current_hash {
            return Err(
                format!(
                    "チェックポイントの設定が異なります。別の --output-dir を使用してください (checkpoint_hash={}, current_hash={})",
                    checkpoint.config_hash, current_hash
                )
                .into(),
            );
        }
        Ok(())
    }

    fn compute_config_hash(&self) -> u64 {
        let mut hash = 0u64;
        hash = hash.wrapping_mul(31).wrapping_add(self.primes.len() as u64);
        for &p in &self.primes {
            hash = hash.wrapping_mul(31).wrapping_add(p as u64);
        }
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(self.zero_mask.size() as u64);
        hash = hash.wrapping_mul(31).wrapping_add(self.target as u64);
        hash
    }

    fn restore_stack(
        &mut self,
        saved_stack: &[Frame],
    ) -> Result<Vec<Frame>, Box<dyn std::error::Error>> {
        let mut stack = Vec::new();

        for frame in saved_stack {
            if frame.level >= self.primes.len() {
                return Err("Invalid stack frame level".into());
            }
            stack.push(Frame { ..*frame });
        }

        Ok(stack)
    }

    fn rebuild_masks(&self, depth: usize, key: &[usize]) -> Vec<BitMask> {
        let mut masks = vec![self.zero_mask.clone(); depth + 1];
        for (level, &shift_idx) in key.iter().enumerate() {
            let (base_masks, node_masks) = masks.split_at_mut(level + 1);
            node_masks[0]
                .bitand_into_count(&base_masks[level], &self.shift_table[level][shift_idx]);
        }
        masks
    }

    #[cfg(test)]
    pub fn search_parallel(
        &self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        resume_path: Option<&Path>,
    ) -> Result<SharedResults, Box<dyn std::error::Error>> {
        let interrupted = AtomicBool::new(false);
        let (results, _) =
            self.search_parallel_interruptible(depth, checkpoint_path, resume_path, &interrupted)?;
        Ok(results)
    }

    pub fn search_parallel_interruptible(
        &self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        resume_path: Option<&Path>,
        interrupted: &AtomicBool,
    ) -> Result<(SharedResults, bool), Box<dyn std::error::Error>> {
        let results = Arc::new(ParallelResults {
            max_score: AtomicUsize::new(0),
            results: Mutex::new(SharedResults::default()),
            target: self.target,
            shift_path_store: self.shift_path_store.clone(),
        });
        let node_count = Arc::new(AtomicU64::new(0));
        let progress = Mutex::new(ParallelProgress {
            completed: HashSet::new(),
            in_progress: HashMap::new(),
        });

        let mut split_depth = self.parallel_split_depth(depth);
        let mut checkpoint_work_prefixes = None;
        if let Some(path) = resume_path {
            let checkpoint = Self::load_checkpoint(path)?;
            if checkpoint.mode != SearchMode::Parallel {
                return Err(
                    "逐次モードのチェックポイントは --mode sequential でのみ再開できます".into(),
                );
            }
            self.apply_checkpoint_config(&checkpoint, depth)?;
            if checkpoint.split_depth > depth {
                return Err("チェックポイントの split_depth が depth を超えています".into());
            }
            split_depth = checkpoint.split_depth;
            checkpoint_work_prefixes = checkpoint.work_prefixes;
            node_count.store(checkpoint.node_count, Ordering::Relaxed);
            results
                .max_score
                .store(checkpoint.max_score, Ordering::Relaxed);
            let (shifts, target_shifts) = self
                .load_checkpoint_paths(
                    checkpoint.max_score,
                    checkpoint.results,
                    checkpoint.target_count,
                )
                .map_err(std::io::Error::other)?;
            *results.results.lock().unwrap() = SharedResults {
                max_score: checkpoint.max_score,
                results: checkpoint.results,
                shifts,
                target_count: checkpoint.target_count,
                target_shifts,
                node_count: checkpoint.node_count,
            };
            {
                let mut progress = progress.lock().unwrap();
                progress.completed = checkpoint.completed.iter().copied().collect();
                progress.in_progress = checkpoint
                    .in_progress
                    .iter()
                    .cloned()
                    .map(|item| (item.work_index, item))
                    .collect();
            }
            info!(
                "チェックポイントから並列探索を再開しました (nodes={})",
                checkpoint.node_count
            );
        }

        let work_items = if let Some(work_prefixes) = checkpoint_work_prefixes {
            self.work_items_from_prefixes(depth, work_prefixes)?
        } else if resume_path.is_some() {
            self.parallel_work_items(split_depth)?
        } else if let Some(previous_paths) = self.load_previous_depth_paths(depth)? {
            if split_depth != depth - 1 {
                split_depth = depth - 1;
            }
            let work_items = self.previous_depth_work_items(depth, previous_paths)?;
            info!(
                "depth={} の開始ワーク項目として depth={} の保存済みパス {} 件を使用します",
                depth,
                depth - 1,
                work_items.len()
            );
            work_items
        } else {
            self.parallel_work_items(split_depth)?
        };
        let work_prefixes = work_items
            .iter()
            .map(|work_item| work_item.key.clone())
            .collect();
        let jobs = {
            let progress = progress.lock().unwrap();
            self.parallel_jobs(&work_items, &progress)?
        };

        let ctx = ParallelSearchCtx {
            depth,
            split_depth,
            work_prefixes: &work_prefixes,
            results: results.as_ref(),
            node_count: node_count.as_ref(),
            progress: &progress,
            checkpoint_path,
            interrupted,
            checkpoint_lock: Mutex::new(()),
            last_checkpoint_nodes: AtomicU64::new(node_count.load(Ordering::Relaxed)),
            progress_bar: self.progress_bar.clone(),
        };
        let shared = results.snapshot();
        Self::update_progress(
            ctx.progress_bar.as_ref(),
            node_count.load(Ordering::Relaxed),
            shared.max_score,
            shared.results,
            0,
        );

        jobs.into_par_iter()
            .try_for_each(|job| self.run_parallel_job(job, &ctx))?;

        if let Some(store) = &self.shift_path_store {
            store.flush().map_err(std::io::Error::other)?;
        }
        info!(
            "並列探索完了 (nodes={})",
            node_count.load(Ordering::Relaxed)
        );
        if ctx.checkpoint_path.is_some() {
            self.write_parallel_checkpoint(&ctx, true, false)?;
        }
        let mut shared = results.snapshot();
        shared.node_count = node_count.load(Ordering::Relaxed);
        Ok((shared, !interrupted.load(Ordering::SeqCst)))
    }

    fn parallel_jobs(
        &self,
        work_items: &[WorkItem],
        progress: &ParallelProgress,
    ) -> Result<Vec<ParallelJob>, Box<dyn std::error::Error>> {
        let mut jobs = Vec::new();
        for (index, item) in progress.in_progress.iter() {
            if *index >= work_items.len() {
                return Err("チェックポイントの work_index が範囲外です".into());
            }
            if progress.completed.contains(index) {
                return Err(
                    "チェックポイントで完了済みと実行中の work_index が重複しています".into(),
                );
            }
            jobs.push(ParallelJob {
                index: *index,
                work_item: work_items[*index].clone(),
                resume: Some((item.stack.clone(), item.key.clone())),
            });
        }
        for (index, work_item) in work_items.iter().enumerate() {
            if progress.completed.contains(&index) || progress.in_progress.contains_key(&index) {
                continue;
            }
            jobs.push(ParallelJob {
                index,
                work_item: work_item.clone(),
                resume: None,
            });
        }
        Ok(jobs)
    }

    fn run_parallel_job(
        &self,
        job: ParallelJob,
        ctx: &ParallelSearchCtx<'_>,
    ) -> Result<(), String> {
        let ParallelJob {
            index,
            work_item,
            resume,
        } = job;
        let depth = ctx.depth;

        if ctx.interrupted.load(Ordering::SeqCst) {
            return Ok(());
        }

        let (mut key, mut masks, mut stack) = if let Some((saved_stack, saved_key)) = resume {
            let masks = self.rebuild_masks(depth, &saved_key);
            (saved_key, masks, saved_stack)
        } else {
            let mut masks = vec![self.zero_mask.clone(); depth + 1];
            masks[ctx.split_depth] = work_item.base_mask;
            let key = work_item.key;
            if ctx.split_depth == depth {
                let count = masks[ctx.split_depth].count_ones();
                if count < self.target {
                } else {
                    if let Some(store) = &ctx.results.shift_path_store {
                        store.append(count, &key)?;
                    }
                    ctx.results.record_target(depth, count, &key)?;
                    ctx.results.record_path(count, &key)?;
                }
                self.finish_parallel_job(index, ctx)?;
                return Ok(());
            }
            let stack = vec![Frame {
                level: ctx.split_depth,
                next_idx: self.primes[ctx.split_depth],
            }];
            (key, masks, stack)
        };

        if ctx.split_depth == depth {
            self.finish_parallel_job(index, ctx)?;
            return Ok(());
        }

        if masks[key.len()].count_ones() < self.target {
            self.finish_parallel_job(index, ctx)?;
            return Ok(());
        }

        self.store_parallel_progress(index, &stack, &key, ctx);
        let mut local_nodes = 0_u64;

        while let Some(frame) = stack.last_mut() {
            if ctx.interrupted.load(Ordering::SeqCst) {
                ctx.node_count.fetch_add(local_nodes, Ordering::Relaxed);
                self.store_parallel_progress(index, &stack, &key, ctx);
                return Ok(());
            }
            if frame.next_idx == 0 {
                stack.pop();
                if stack.last().is_some() {
                    key.pop();
                }
                continue;
            }

            frame.next_idx -= 1;
            let idx = frame.next_idx;
            let level = frame.level;
            key.push(idx);
            local_nodes += 1;
            let (base_masks, node_masks) = masks.split_at_mut(level + 1);
            let c_count =
                node_masks[0].bitand_into_count(&base_masks[level], &self.shift_table[level][idx]);

            if local_nodes == self.checkpoint_interval {
                let n = ctx.node_count.fetch_add(local_nodes, Ordering::Relaxed) + local_nodes;
                local_nodes = 0;
                let shared = ctx.results.snapshot();
                Self::update_progress(
                    ctx.progress_bar.as_ref(),
                    n,
                    shared.max_score,
                    shared.results,
                    key.len(),
                );
                info!(
                    "探索経過: nodes={} hit={} depth={} key={:?}",
                    n,
                    shared.results,
                    key.len(),
                    key
                );
                self.store_parallel_progress(index, &stack, &key, ctx);
                self.write_parallel_checkpoint(ctx, false, true)?;
            }

            if c_count < self.target {
                info!(
                    "target break (depth={}, count={}, key={:?})",
                    key.len(),
                    c_count,
                    key
                );
                key.pop();
                continue;
            }

            if let Some(store) = &ctx.results.shift_path_store {
                store.append(c_count, &key)?;
            }

            if level + 1 >= depth {
                ctx.results.record_target(depth, c_count, &key)?;
                ctx.results.record_path(c_count, &key)?;
                key.pop();
                continue;
            }

            stack.push(Frame {
                level: level + 1,
                next_idx: self.primes[level + 1],
            });
        }

        ctx.node_count.fetch_add(local_nodes, Ordering::Relaxed);
        self.finish_parallel_job(index, ctx)
    }

    fn store_parallel_progress(
        &self,
        work_index: usize,
        stack: &[Frame],
        key: &[usize],
        ctx: &ParallelSearchCtx<'_>,
    ) {
        ctx.progress.lock().unwrap().in_progress.insert(
            work_index,
            ParallelInProgress {
                work_index,
                stack: stack.to_vec(),
                key: key.to_vec(),
            },
        );
    }

    fn finish_parallel_job(
        &self,
        work_index: usize,
        ctx: &ParallelSearchCtx<'_>,
    ) -> Result<(), String> {
        {
            let mut progress = ctx.progress.lock().unwrap();
            progress.in_progress.remove(&work_index);
            progress.completed.insert(work_index);
        }
        self.write_parallel_checkpoint(ctx, false, false)
    }

    fn write_parallel_checkpoint(
        &self,
        ctx: &ParallelSearchCtx<'_>,
        force_lock: bool,
        require_interval: bool,
    ) -> Result<(), String> {
        let Some(path) = ctx.checkpoint_path else {
            return Ok(());
        };
        let n = ctx.node_count.load(Ordering::Relaxed);
        let last = ctx.last_checkpoint_nodes.load(Ordering::Relaxed);
        if require_interval && n.saturating_sub(last) < self.checkpoint_interval {
            return Ok(());
        }
        let _guard = if force_lock {
            ctx.checkpoint_lock.lock().unwrap()
        } else {
            match ctx.checkpoint_lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    log::debug!("チェックポイント書き込みスキップ (別スレッドが実行中)");
                    return Ok(());
                }
            }
        };
        let n = ctx.node_count.load(Ordering::Relaxed);
        let last = ctx.last_checkpoint_nodes.load(Ordering::Relaxed);
        if require_interval && n.saturating_sub(last) < self.checkpoint_interval {
            return Ok(());
        }

        let (completed, in_progress) = {
            let progress = ctx.progress.lock().unwrap();
            let mut completed: Vec<_> = progress.completed.iter().copied().collect();
            completed.sort_unstable();
            let mut in_progress: Vec<_> = progress.in_progress.values().cloned().collect();
            in_progress.sort_by_key(|item| item.work_index);
            (completed, in_progress)
        };
        let shared = ctx.results.snapshot();
        self.write_checkpoint_file(
            path,
            &Checkpoint {
                mode: SearchMode::Parallel,
                depth: ctx.depth,
                stack: Vec::new(),
                key: Vec::new(),
                max_score: shared.max_score,
                results: shared.results,
                target_count: shared.target_count,
                node_count: n,
                split_depth: ctx.split_depth,
                completed,
                in_progress,
                pending_prefixes: Vec::new(),
                work_prefixes: Some(ctx.work_prefixes.clone()),
                config_hash: self.compute_config_hash(),
            },
        )
        .map_err(|err| err.to_string())?;
        ctx.last_checkpoint_nodes.store(n, Ordering::Relaxed);
        Ok(())
    }

    fn parallel_split_depth(&self, depth: usize) -> usize {
        let target_tasks = rayon::current_num_threads() * 4;
        let mut task_count: usize = 1;
        for level in 0..depth {
            task_count = task_count.saturating_mul(self.primes[level]);
            if task_count >= target_tasks {
                return level + 1;
            }
        }
        depth
    }

    fn parallel_work_items(&self, split_depth: usize) -> Result<Vec<WorkItem>, String> {
        let mut work_items = vec![WorkItem {
            key: Vec::with_capacity(split_depth),
            base_mask: self.zero_mask.clone(),
        }];

        for level in 0..split_depth {
            let mut next_items = Vec::with_capacity(work_items.len() * self.primes[level]);
            for item in work_items {
                for shift in (0..self.primes[level]).rev() {
                    let mut base_mask = self.zero_mask.clone();
                    let count = base_mask
                        .bitand_into_count(&item.base_mask, &self.shift_table[level][shift]);
                    let mut key = item.key.clone();
                    key.push(shift);
                    if count < self.target {
                        continue;
                    }
                    if let Some(store) = &self.shift_path_store {
                        store.append(count, &key)?;
                    }
                    next_items.push(WorkItem { key, base_mask });
                }
            }
            work_items = next_items;
        }

        Ok(work_items)
    }

    fn previous_depth_work_items(
        &self,
        depth: usize,
        paths: ShiftPaths,
    ) -> Result<Vec<WorkItem>, String> {
        self.work_items_from_prefixes(depth, paths)
    }

    fn work_items_from_prefixes(
        &self,
        depth: usize,
        prefixes: ShiftPaths,
    ) -> Result<Vec<WorkItem>, String> {
        let mut work_items = Vec::with_capacity(prefixes.len());
        for key in prefixes {
            if key.is_empty() || key.len() > depth {
                return Err(format!("チェックポイントの開始パス長が不正です: {:?}", key));
            }
            if key
                .iter()
                .enumerate()
                .any(|(level, &shift)| shift >= self.primes[level])
            {
                return Err(format!(
                    "保存済みシフトパスに範囲外の値が含まれています: {:?}",
                    key
                ));
            }
            let masks = self.rebuild_masks(depth, &key);
            let base_mask = masks[key.len()].clone();
            let count = base_mask.count_ones();
            if count < self.target {
                continue;
            }
            if let Some(store) = &self.shift_path_store {
                store.append(count, &key)?;
            }
            work_items.push(WorkItem { key, base_mask });
        }
        Ok(work_items)
    }
}

#[cfg(test)]
mod tests {
    use super::{build_shift_table, ParallelResults, State};
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    #[test]
    fn build_shift_table_creates_expected_complement_masks() {
        let table = build_shift_table(&[2], 6);
        assert_eq!(table.len(), 1);
        assert_eq!(table[0].len(), 2);
        assert_eq!(table[0][0].count_ones(), 3);
        assert_eq!(table[0][1].count_ones(), 3);
    }

    #[test]
    fn parallel_results_keep_only_paths_at_the_current_maximum() {
        let results = ParallelResults {
            max_score: AtomicUsize::new(0),
            results: Mutex::new(Default::default()),
            target: 0,
            shift_path_store: None,
        };

        results.record_path(1, &[0]).unwrap();
        results.record_path(2, &[1]).unwrap();
        results.record_path(2, &[2]).unwrap();

        let snapshot = results.snapshot();
        assert_eq!(snapshot.max_score, 2);
        assert_eq!(snapshot.results, 2);
        assert_eq!(snapshot.shifts, vec![vec![1], vec![2]]);
    }

    #[test]
    fn sequential_and_parallel_search_find_valid_results() {
        let primes = vec![2, 3];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let mut sequential = State::new(primes.clone(), cols, table.clone());
        sequential.target = 0;
        sequential.search_with_checkpoint(2, None, None).unwrap();
        let mut parallel = State::new(primes.clone(), cols, table);
        parallel.target = 0;
        let result = parallel.search_parallel(2, None, None).unwrap();

        assert!(sequential.results > 0);
        assert_eq!(result.max_score, sequential.max_score);
        assert_eq!(result.results, sequential.results);
        assert_eq!(result.shifts.len(), result.results);
        for shifts in &result.shifts {
            assert_eq!(shifts.len(), 2);
            for (level, &shift) in shifts.iter().enumerate() {
                assert!(shift < primes[level]);
            }
        }
    }

    #[test]
    fn sequential_and_parallel_search_record_all_best_leaves() {
        let primes = vec![2];
        let cols = 4;
        let table = build_shift_table(&primes, cols);

        let mut sequential = State::new(primes.clone(), cols, table.clone());
        sequential.target = 0;
        sequential.search_with_checkpoint(1, None, None).unwrap();

        let mut parallel = State::new(primes, cols, table);
        parallel.target = 0;
        let result = parallel.search_parallel(1, None, None).unwrap();

        assert_eq!(sequential.max_score, 2);
        assert_eq!(sequential.results, 2);
        assert_eq!(sequential.shifts, vec![vec![1], vec![0]]);
        assert_eq!(result.max_score, 2);
        assert_eq!(result.results, 2);
        assert_eq!(result.shifts.len(), 2);
    }

    #[test]
    fn sequential_search_writes_each_unpruned_path_to_rocksdb() {
        let primes = vec![2, 3];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let path = std::env::temp_dir().join(format!(
            "hlsearch-shift-paths-{}-{}.rocksdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut state = State::new(primes, cols, table);
        state.target = 0;
        state.set_shift_path_store(&path, 2, true).unwrap();

        state.search_with_checkpoint(2, None, None).unwrap();

        let store = state.shift_path_store.as_ref().unwrap();
        assert_eq!(store.load_nodes(1).unwrap().len(), 2);
        let nodes = store.load_nodes(2).unwrap();
        assert_eq!(nodes.len(), 6);
        assert!(nodes
            .iter()
            .all(|(_, record)| record.parent_node_id.is_some()));

        drop(state);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_uses_previous_depth_paths_as_prefixes_when_available() {
        let depth = 2;
        let primes = vec![2; depth];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let database_path = unique_checkpoint_path("previous-depth-paths").with_extension("rocksdb");
        let prefix = vec![0; depth - 1];

        let mut previous = State::new(primes.clone(), cols, table.clone());
        previous
            .set_shift_path_store(&database_path, depth - 1, true)
            .unwrap();
        previous
            .shift_path_store
            .as_ref()
            .unwrap()
            .append(2, &prefix)
            .unwrap();
        drop(previous);

        let mut sequential = State::new(primes.clone(), cols, table.clone());
        sequential.target = 0;
        sequential
            .set_shift_path_store(&database_path, depth, false)
            .unwrap();
        sequential
            .search_with_checkpoint(depth, None, None)
            .unwrap();
        assert_eq!(sequential.node_count, 2);
        assert!(sequential
            .shifts
            .iter()
            .all(|path| path[..depth - 1] == prefix));
        drop(sequential);

        let mut parallel = State::new(primes, cols, table);
        parallel.target = 0;
        parallel
            .set_shift_path_store(&database_path, depth, false)
            .unwrap();
        let checkpoint_path = unique_checkpoint_path("previous-depth-parallel");
        let result = parallel
            .search_parallel(depth, Some(&checkpoint_path), None)
            .unwrap();
        assert!(result.shifts.iter().all(|path| path[..depth - 1] == prefix));

        drop(parallel);
        let mut resumed = State::new(vec![2; depth], cols, build_shift_table(&[2; 2], cols));
        resumed.target = 0;
        resumed
            .set_shift_path_store(&database_path, depth, false)
            .unwrap();
        let resumed_result = resumed
            .search_parallel(depth, Some(&checkpoint_path), Some(&checkpoint_path))
            .unwrap();
        assert_eq!(resumed_result.shifts, result.shifts);
        assert!(resumed_result
            .shifts
            .iter()
            .all(|path| path[..depth - 1] == prefix));

        let backup_path = checkpoint_path.with_extension("bak");
        let _ = std::fs::remove_file(checkpoint_path);
        let _ = std::fs::remove_file(backup_path);
        drop(resumed);
        std::fs::remove_dir_all(database_path).unwrap();
    }

    #[test]
    fn checkpoint_interval_defaults_to_100k() {
        let table = build_shift_table(&[2], 4);
        let state = State::new(vec![2], 4, table);
        assert_eq!(state.checkpoint_interval, 100_000);
    }

    #[test]
    fn target_defaults_to_447_and_prunes_smaller_counts() {
        let primes = vec![2];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let mut sequential = State::new(primes.clone(), cols, table.clone());
        assert_eq!(sequential.target, 447);
        sequential.target = 3;

        sequential.search_with_checkpoint(1, None, None).unwrap();
        assert_eq!(sequential.results, 0);

        let mut parallel = State::new(primes, cols, table);
        parallel.target = 3;
        let result = parallel.search_parallel(1, None, None).unwrap();
        assert_eq!(result.results, 0);
    }

    #[test]
    fn target_matches_at_depth_249_are_recorded() {
        let table = build_shift_table(&[2], 4);
        let mut state = State::new(vec![2], 4, table);
        let key = vec![1];

        state.aggregate_target_result(249, 447, &key).unwrap();
        state.aggregate_target_result(248, 447, &key).unwrap();
        state.aggregate_target_result(249, 446, &key).unwrap();

        assert_eq!(state.target_count, 1);
        assert_eq!(state.target_shifts, vec![key]);
    }

    #[test]
    fn checkpoint_interval_can_be_set() {
        let table = build_shift_table(&[2], 4);
        let mut state = State::new(vec![2], 4, table);
        state.checkpoint_interval = 5_000;
        assert_eq!(state.checkpoint_interval, 5_000);
    }

    #[test]
    fn sequential_interruption_writes_a_checkpoint() {
        use std::sync::atomic::AtomicBool;

        let primes = vec![2, 3];
        let table = build_shift_table(&primes, 4);
        let mut state = State::new(primes, 4, table);
        let path = unique_checkpoint_path("interrupted-sequential");
        let interrupted = AtomicBool::new(true);

        let completed = state
            .search_with_checkpoint_interruptible(2, Some(&path), None, &interrupted)
            .unwrap();

        assert!(!completed);
        assert!(path.exists());

        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(backup_path);
    }

    #[test]
    fn parallel_interruption_writes_a_checkpoint() {
        use std::sync::atomic::AtomicBool;

        let primes = vec![2, 3];
        let table = build_shift_table(&primes, 4);
        let state = State::new(primes, 4, table);
        let path = unique_checkpoint_path("interrupted-parallel");
        let interrupted = AtomicBool::new(true);

        let (_, completed) = state
            .search_parallel_interruptible(2, Some(&path), None, &interrupted)
            .unwrap();

        assert!(!completed);
        assert!(path.exists());

        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(backup_path);
    }

    #[test]
    fn restore_stack_recovers_search_position() {
        let primes = vec![2, 3];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let mut state = State::new(primes.clone(), cols, table);

        state.key = vec![1, 0];
        state.node_count = 10;
        state.max_score = 2;
        state.results = 0;

        let saved_stack = vec![super::Frame {
            level: 1,
            next_idx: 2,
        }];

        let rebuilt_stack = state.restore_stack(&saved_stack).unwrap();

        assert_eq!(rebuilt_stack.len(), 1);
        assert_eq!(rebuilt_stack[0].level, 1);
        assert_eq!(rebuilt_stack[0].next_idx, 2);
        assert_eq!(rebuilt_stack[0].level, 1);
    }

    #[test]
    fn stack_frame_serialization_is_lightweight() {
        use serde_json;
        let frame = super::Frame {
            level: 5,
            next_idx: 42,
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"level\":5"));
        assert!(json.contains("\"next_idx\":42"));
        assert!(!json.contains("base_mask"));
    }

    #[test]
    fn checkpoint_stores_only_key_level_and_next_idx() {
        use serde_json;
        let checkpoint = super::Checkpoint {
            mode: super::SearchMode::Sequential,
            depth: 2,
            stack: vec![super::Frame {
                level: 0,
                next_idx: 1,
            }],
            key: vec![1],
            max_score: 2,
            results: 0,
            target_count: 0,
            node_count: 100,
            split_depth: 0,
            completed: vec![],
            in_progress: vec![],
            pending_prefixes: vec![],
            work_prefixes: None,
            config_hash: 0,
        };
        let json = serde_json::to_string_pretty(&checkpoint).unwrap();
        assert!(json.contains("depth"));
        assert!(json.contains("key"));
        assert!(json.contains("level"));
        assert!(json.contains("next_idx"));
        assert!(!json.contains("primes"));
        assert!(!json.contains("cols"));
        assert!(!json.contains("shifts"));
        assert!(!json.contains("target_shifts"));
        assert!(!json.contains("zero_mask"));
        assert!(!json.contains("base_mask"));
    }

    #[test]
    fn checkpoint_write_and_resume_round_trip() {
        let primes = vec![2];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let path = std::env::temp_dir().join(format!(
            "hlsearch-checkpoint-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let saved = State::new(primes.clone(), cols, table.clone());
        let stack = vec![super::Frame {
            level: 0,
            next_idx: 2,
        }];
        saved.write_checkpoint(&path, 1, &stack).unwrap();
        assert!(!std::fs::read_to_string(&path).unwrap().contains('\n'));

        let mut resumed = State::new(primes, cols, table);
        resumed
            .search_with_checkpoint(1, Some(&path), Some(&path))
            .unwrap();

        let backup_path = path.with_extension("bak");
        assert!(backup_path.exists());
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(backup_path).unwrap();
    }

    #[test]
    fn multiple_keys_rebuild_to_correct_masks() {
        let primes = vec![2, 3, 5];
        let cols = 8;
        let table = build_shift_table(&primes, cols);
        let mut state = State::new(primes.clone(), cols, table);

        state.key = vec![1, 2, 1];

        let saved_stack = vec![super::Frame {
            level: 2,
            next_idx: 3,
        }];

        let rebuilt = state.restore_stack(&saved_stack).unwrap();
        assert_eq!(rebuilt.len(), 1);
        assert_eq!(rebuilt[0].level, 2);

        let masks = state.rebuild_masks(3, &state.key);
        assert_eq!(masks[3].count_ones(), 2);
    }

    fn unique_checkpoint_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hlsearch-{label}-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn parallel_checkpoint_write_and_resume_round_trip() {
        let primes = vec![2, 3, 5];
        let cols = 8;
        let depth = 3;
        let table = build_shift_table(&primes, cols);
        let path = unique_checkpoint_path("parallel-checkpoint");
        let database_path = path.with_extension("rocksdb");

        let mut state = State::new(primes.clone(), cols, table.clone());
        state.checkpoint_interval = 1;
        state
            .set_shift_path_store(&database_path, depth, true)
            .unwrap();
        let expected = state.search_parallel(depth, Some(&path), None).unwrap();
        drop(state); // RocksDB はディレクトリロックを持つため、再オープン前に閉じる

        let mut resumed = State::new(primes, cols, table);
        resumed
            .set_shift_path_store(&database_path, depth, false)
            .unwrap();
        let result = resumed
            .search_parallel(depth, Some(&path), Some(&path))
            .unwrap();

        assert_eq!(result.max_score, expected.max_score);
        assert_eq!(result.results, expected.results);
        assert_eq!(result.shifts.len(), expected.results);
        assert_eq!(result.node_count, expected.node_count);

        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(backup_path);
        let _ = std::fs::remove_dir_all(database_path);
    }

    #[test]
    fn parallel_resume_skips_completed_work_items() {
        let primes = vec![2, 3, 5];
        let cols = 8;
        let depth = 3;
        let table = build_shift_table(&primes, cols);
        let mut state = State::new(primes.clone(), cols, table.clone());
        state.target = 0;
        let split_depth = state.parallel_split_depth(depth);
        let work_items = state.parallel_work_items(split_depth).unwrap();
        assert!(!work_items.is_empty());

        let mut expected_state = State::new(primes.clone(), cols, table.clone());
        expected_state.target = 0;
        let expected = expected_state.search_parallel(depth, None, None).unwrap();

        let path = unique_checkpoint_path("parallel-completed");
        let database_path = path.with_extension("rocksdb");
        state
            .set_shift_path_store(&database_path, depth, true)
            .unwrap();
        let store = state.shift_path_store.as_ref().unwrap();
        for shifts in &expected.shifts {
            store.append(expected.max_score, shifts).unwrap();
        }
        for (index, target_shifts) in expected.target_shifts.iter().enumerate() {
            store.append_target(index + 1, target_shifts).unwrap();
        }
        store.flush().unwrap();
        let completed: Vec<usize> = (0..work_items.len()).collect();
        state
            .write_checkpoint_file(
                &path,
                &super::Checkpoint {
                    mode: super::SearchMode::Parallel,
                    depth,
                    stack: Vec::new(),
                    key: Vec::new(),
                    max_score: expected.max_score,
                    results: expected.results,
                    target_count: expected.target_count,
                    node_count: 42,
                    split_depth,
                    completed,
                    in_progress: Vec::new(),
                    pending_prefixes: Vec::new(),
                    work_prefixes: None,
                    config_hash: state.compute_config_hash(),
                },
            )
            .unwrap();
        drop(state); // RocksDB のロック解放

        let mut resumed = State::new(primes, cols, table);
        resumed.target = 0;
        resumed
            .set_shift_path_store(&database_path, depth, false)
            .unwrap();
        let result = resumed
            .search_parallel(depth, Some(&path), Some(&path))
            .unwrap();
        assert_eq!(result.max_score, expected.max_score);
        assert_eq!(result.results, expected.results);
        assert_eq!(result.shifts, expected.shifts);

        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(backup_path);
        let _ = std::fs::remove_dir_all(database_path);
    }

    #[test]
    fn parallel_search_rejects_sequential_checkpoint() {
        let primes = vec![2];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let path = unique_checkpoint_path("sequential-for-parallel");
        let saved = State::new(primes.clone(), cols, table.clone());
        saved
            .write_checkpoint(
                &path,
                1,
                &[super::Frame {
                    level: 0,
                    next_idx: 2,
                }],
            )
            .unwrap();

        let resumed = State::new(primes, cols, table);
        let err = resumed
            .search_parallel(1, None, Some(&path))
            .unwrap_err()
            .to_string();
        assert!(err.contains("sequential"));

        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(backup_path);
    }

    #[test]
    fn sequential_search_rejects_parallel_checkpoint() {
        let primes = vec![2, 3];
        let cols = 4;
        let depth = 2;
        let table = build_shift_table(&primes, cols);
        let path = unique_checkpoint_path("parallel-for-sequential");
        let state = State::new(primes.clone(), cols, table.clone());
        state
            .write_checkpoint_file(
                &path,
                &super::Checkpoint {
                    mode: super::SearchMode::Parallel,
                    depth,
                    stack: Vec::new(),
                    key: Vec::new(),
                    max_score: 0,
                    results: 0,
                    target_count: 0,
                    node_count: 0,
                    split_depth: 1,
                    completed: Vec::new(),
                    in_progress: Vec::new(),
                    pending_prefixes: Vec::new(),
                    work_prefixes: None,
                    config_hash: 0,
                },
            )
            .unwrap();

        let mut resumed = State::new(primes, cols, table);
        let err = resumed
            .search_with_checkpoint(depth, None, Some(&path))
            .unwrap_err()
            .to_string();
        assert!(err.contains("parallel"));

        let _ = std::fs::remove_file(&path);
    }
}
