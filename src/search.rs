use crate::bitmask::BitMask;
use crate::config::{DEFAULT_CHECKPOINT_INTERVAL, MAX_SEARCH_DEPTH, TARGET_SCORE};
use crate::rocksdb_output::ShiftPathStore;
use log::{debug, info};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

const SHIFT_PATH_BATCH_SIZE: usize = 256;
const PREVIOUS_DEPTH_BATCH_SIZE: usize = 210;
const PROGRESS_UPDATE_NODE_INTERVAL: u64 = 256;
const BEAM_POLICY_VERSION: u64 = 4;

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

#[derive(Clone, Deserialize, Serialize)]
struct BeamCandidate {
    key: Vec<usize>,
    score: usize,
}

struct BeamCheckpointData<'a> {
    mode: SearchMode,
    depth: usize,
    frontier: &'a [BeamCandidate],
    level: usize,
    next_frontier: Vec<BeamCandidate>,
    next_index: usize,
    next_shift: usize,
}

#[derive(Deserialize, Serialize)]
struct Checkpoint {
    #[serde(default)]
    key: Vec<usize>,
    #[serde(default = "default_checkpoint_mode")]
    mode: SearchMode,
    depth: usize,
    stack: Vec<Frame>,
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
    #[serde(default)]
    beam_frontier: Option<Vec<BeamCandidate>>,
    #[serde(default)]
    beam_level: usize,
    #[serde(default)]
    beam_next_frontier: Vec<BeamCandidate>,
    #[serde(default)]
    beam_next_index: usize,
    #[serde(default)]
    beam_next_shift: usize,
    #[serde(default)]
    beam_saved_prefix_batches: bool,
    #[serde(default)]
    beam_next_frontier_checkpointed: bool,
    #[serde(default)]
    prefix_after_node_id: Option<u64>,
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
}

#[derive(Default)]
pub struct SearchProgress {
    node_count: AtomicU64,
    max_score: AtomicUsize,
    results: AtomicUsize,
    depth: AtomicUsize,
    prefix_done: AtomicU64,
    prefix_total: AtomicU64,
    db_insert_done: AtomicU64,
    db_insert_total: AtomicU64,
    db_insert_active: AtomicBool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchProgressSnapshot {
    pub node_count: u64,
    pub max_score: usize,
    pub results: usize,
    pub depth: usize,
    pub prefix_done: u64,
    pub prefix_total: u64,
    pub db_insert_done: u64,
    pub db_insert_total: u64,
    pub db_insert_active: bool,
}

impl SearchProgress {
    pub fn snapshot(&self) -> SearchProgressSnapshot {
        SearchProgressSnapshot {
            node_count: self.node_count.load(Ordering::Relaxed),
            max_score: self.max_score.load(Ordering::Relaxed),
            results: self.results.load(Ordering::Relaxed),
            depth: self.depth.load(Ordering::Relaxed),
            prefix_done: self.prefix_done.load(Ordering::Relaxed),
            prefix_total: self.prefix_total.load(Ordering::Relaxed),
            db_insert_done: self.db_insert_done.load(Ordering::Relaxed),
            db_insert_total: self.db_insert_total.load(Ordering::Relaxed),
            db_insert_active: self.db_insert_active.load(Ordering::Relaxed),
        }
    }

    fn update(&self, nodes: u64, max_score: usize, results: usize, depth: usize) {
        self.node_count.store(nodes, Ordering::Relaxed);
        self.max_score.store(max_score, Ordering::Relaxed);
        self.results.store(results, Ordering::Relaxed);
        self.depth.store(depth, Ordering::Relaxed);
    }

    fn add_nodes(&self, nodes: u64) {
        self.node_count.fetch_add(nodes, Ordering::Relaxed);
    }

    fn set_prefix_total(&self, total: u64) {
        self.prefix_done.store(0, Ordering::Relaxed);
        self.prefix_total.store(total, Ordering::Relaxed);
    }

    fn add_prefix_done(&self, prefixes: u64) {
        self.prefix_done.fetch_add(prefixes, Ordering::Relaxed);
    }

    fn begin_db_insert(&self, total: u64) {
        self.db_insert_done.store(0, Ordering::Relaxed);
        self.db_insert_total.store(total, Ordering::Relaxed);
        self.db_insert_active.store(true, Ordering::Relaxed);
    }

    fn complete_db_insert(&self, done: u64) {
        self.db_insert_done.store(done, Ordering::Relaxed);
        self.db_insert_active.store(false, Ordering::Relaxed);
    }

    fn cancel_db_insert(&self) {
        self.db_insert_done.store(0, Ordering::Relaxed);
        self.db_insert_active.store(false, Ordering::Relaxed);
    }
}

pub struct State {
    pub primes: Vec<usize>,
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
    progress_reporter: Option<Arc<SearchProgress>>,
}

impl State {
    pub fn new(primes: Vec<usize>, cols: usize, shift_table: Vec<Vec<BitMask>>) -> Self {
        State {
            primes,
            zero_mask: BitMask::new_ones(cols),
            max_score: 0,
            target: TARGET_SCORE,
            target_count: 0,
            target_shifts: Vec::new(),
            results: 0,
            shifts: Vec::new(),
            node_count: 0,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            shift_table,
            shift_path_store: None,
            progress_reporter: None,
        }
    }

    pub fn set_progress_reporter(&mut self, reporter: Arc<SearchProgress>) {
        self.progress_reporter = Some(reporter);
    }

    pub fn load_top_scored_paths(
        &self,
        depth: usize,
        limit: usize,
    ) -> Result<Vec<(usize, Vec<usize>)>, String> {
        match &self.shift_path_store {
            Some(store) => store.load_top_scored_paths(depth, limit),
            None => Ok(Vec::new()),
        }
    }

    pub fn count_depth_paths(&self, depth: usize) -> Result<u64, String> {
        self.shift_path_store
            .as_ref()
            .ok_or_else(|| "RocksDB output is not configured".to_string())?
            .count_depth_paths(depth)
    }

    pub fn set_search_complete(&self, complete: bool) -> Result<(), String> {
        self.shift_path_store
            .as_ref()
            .ok_or_else(|| "RocksDB output is not configured".to_string())?
            .set_search_complete(complete)
    }

    fn update_progress(
        progress_reporter: Option<&SearchProgress>,
        nodes: u64,
        max_score: usize,
        results: usize,
        depth: usize,
    ) {
        if let Some(reporter) = progress_reporter {
            reporter.update(nodes, max_score, results, depth);
        }
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

    fn submit_shift_path_batch(
        &self,
        pending: &mut Vec<(usize, Vec<usize>)>,
    ) -> Result<(), String> {
        if let Some(store) = &self.shift_path_store {
            let appends = std::mem::take(pending);
            if appends.is_empty() {
                return Ok(());
            }
            let insert_count = appends.len() as u64;
            if let Some(reporter) = &self.progress_reporter {
                reporter.begin_db_insert(insert_count);
            }
            let result = store.append_batch(appends).and_then(|()| store.flush());
            if let Some(reporter) = &self.progress_reporter {
                if result.is_ok() {
                    reporter.complete_db_insert(insert_count);
                } else {
                    reporter.cancel_db_insert();
                }
            }
            result?;
        }
        Ok(())
    }

    fn replace_shift_path_depth(
        &self,
        depth: usize,
        paths: &[(usize, Vec<usize>)],
    ) -> Result<(), String> {
        let Some(store) = &self.shift_path_store else {
            return Ok(());
        };
        if let Some(reporter) = &self.progress_reporter {
            reporter.begin_db_insert(paths.len() as u64);
        }
        let result = store.replace_depth_paths(depth, paths);
        if let Some(reporter) = &self.progress_reporter {
            if result.is_ok() {
                reporter.complete_db_insert(paths.len() as u64);
            } else {
                reporter.cancel_db_insert();
            }
        }
        result
    }

    fn has_previous_depth_paths(&self, depth: usize) -> Result<bool, String> {
        if depth == 0 {
            return Ok(false);
        }
        let Some(store) = self.shift_path_store.as_ref() else {
            return Ok(false);
        };
        Ok(!store
            .load_full_depth_paths_batch(depth - 1, None, 1)?
            .is_empty())
    }

    fn load_candidates_from_store(&self, depth: usize) -> Result<Vec<BeamCandidate>, String> {
        let store = self
            .shift_path_store
            .as_ref()
            .ok_or_else(|| "RocksDB output is not configured".to_string())?;
        let mut candidates = Vec::new();
        for key in store.load_full_depth_paths(depth)? {
            self.validate_path(&key, depth)
                .map_err(|error| error.to_string())?;
            let score = self.rebuild_masks(depth, &key)[key.len()].count_ones();
            if score >= self.target {
                candidates.push(BeamCandidate { key, score });
            }
        }
        candidates.sort_unstable_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| right.key.cmp(&left.key))
        });
        Ok(candidates)
    }

    fn persist_candidates(&self, candidates: &[BeamCandidate]) -> Result<(), String> {
        let Some(store) = &self.shift_path_store else {
            return Ok(());
        };
        for batch in candidates.chunks(SHIFT_PATH_BATCH_SIZE) {
            let mut pending = batch
                .iter()
                .map(|candidate| (candidate.score, candidate.key.clone()))
                .collect();
            self.submit_shift_path_batch(&mut pending)?;
        }
        store.flush()
    }

    fn evaluate_frontier_parallel(
        &self,
        frontier: &[BeamCandidate],
        level: usize,
        interrupted: &AtomicBool,
    ) -> Option<(u64, Vec<BeamCandidate>)> {
        let progress_reporter = self.progress_reporter.as_deref();
        let evaluated: Vec<_> = frontier
            .par_iter()
            .map(|candidate| {
                if interrupted.load(Ordering::SeqCst) {
                    return (0, Vec::new());
                }

                let base_mask = self.rebuild_masks(level, &candidate.key)[level].clone();
                let mut candidates = Vec::new();
                let mut nodes = 0;
                for shift in (0..self.primes[level]).rev() {
                    if interrupted.load(Ordering::SeqCst) {
                        break;
                    }
                    let mut mask = self.zero_mask.clone();
                    let score = mask.bitand_into_count(&base_mask, &self.shift_table[level][shift]);
                    nodes += 1;
                    if let Some(reporter) = progress_reporter {
                        reporter.add_nodes(1);
                    }
                    if score >= self.target {
                        let mut key = candidate.key.clone();
                        key.push(shift);
                        candidates.push(BeamCandidate { key, score });
                    }
                }
                (nodes, candidates)
            })
            .collect();

        if interrupted.load(Ordering::SeqCst) {
            return None;
        }

        let node_count = evaluated.iter().map(|(nodes, _)| *nodes).sum();
        let candidates = evaluated
            .into_iter()
            .flat_map(|(_, candidates)| candidates)
            .collect();
        Some((node_count, candidates))
    }

    fn aggregate_leaf_result(&mut self, score: usize, key: &[usize]) -> Result<(), String> {
        if score < self.target {
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
            self.shifts.sort_unstable_by(|left, right| right.cmp(left));
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
        if depth == MAX_SEARCH_DEPTH && score == self.target {
            let target_count = self.target_count + 1;
            if let Some(store) = &self.shift_path_store {
                store.append_target(target_count, key)?;
            }
            self.target_count = target_count;
            self.target_shifts.push(key.to_vec());
        }
        Ok(())
    }

    fn search_saved_prefix_batches(
        &mut self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        interrupted: &AtomicBool,
        mode: SearchMode,
        mut after_node_id: Option<u64>,
    ) -> Result<(SharedResults, bool), Box<dyn std::error::Error>> {
        let store = self
            .shift_path_store
            .as_ref()
            .cloned()
            .ok_or("RocksDB output is not configured")?;
        let mut pending_appends = Vec::with_capacity(SHIFT_PATH_BATCH_SIZE);

        self.max_score = 0;
        self.results = 0;
        self.shifts.clear();
        self.target_count = 0;
        self.target_shifts.clear();
        if let Some(reporter) = self.progress_reporter.as_ref() {
            let total = store
                .count_depth_paths(depth - 1)
                .map_err(std::io::Error::other)?;
            reporter.set_prefix_total(total);
        }
        Self::update_progress(
            self.progress_reporter.as_deref(),
            self.node_count,
            self.max_score,
            self.results,
            depth,
        );

        loop {
            if interrupted.load(Ordering::SeqCst) {
                self.submit_shift_path_batch(&mut pending_appends)
                    .map_err(std::io::Error::other)?;
                store.flush().map_err(std::io::Error::other)?;
                self.write_saved_prefix_batch_checkpoint(
                    checkpoint_path,
                    mode,
                    depth,
                    after_node_id,
                )?;
                return Ok((SharedResults::default(), false));
            }

            let batch = store
                .load_full_depth_paths_batch(depth - 1, after_node_id, PREVIOUS_DEPTH_BATCH_SIZE)
                .map_err(std::io::Error::other)?;
            let Some((last_node_id, _)) = batch.last() else {
                store.flush().map_err(std::io::Error::other)?;
                self.write_saved_prefix_batch_checkpoint(checkpoint_path, mode, depth, None)?;
                return Ok((
                    SharedResults {
                        max_score: self.max_score,
                        results: self.results,
                        shifts: self.shifts.clone(),
                        target_count: self.target_count,
                        target_shifts: self.target_shifts.clone(),
                    },
                    true,
                ));
            };
            after_node_id = Some(*last_node_id);
            let prefix_count = batch.len() as u64;

            let frontier: Vec<_> = batch
                .into_iter()
                .filter_map(|(_, key)| {
                    let score = self.rebuild_masks(depth, &key)[key.len()].count_ones();
                    (score >= self.target).then_some(BeamCandidate { key, score })
                })
                .collect();
            let candidates = match mode {
                SearchMode::Sequential => {
                    let mut candidates = Vec::new();
                    for candidate in &frontier {
                        let base_mask =
                            self.rebuild_masks(depth, &candidate.key)[depth - 1].clone();
                        for shift in (0..self.primes[depth - 1]).rev() {
                            if interrupted.load(Ordering::SeqCst) {
                                break;
                            }
                            let mut mask = self.zero_mask.clone();
                            let score = mask
                                .bitand_into_count(&base_mask, &self.shift_table[depth - 1][shift]);
                            self.node_count += 1;
                            if self
                                .node_count
                                .is_multiple_of(PROGRESS_UPDATE_NODE_INTERVAL)
                            {
                                Self::update_progress(
                                    self.progress_reporter.as_deref(),
                                    self.node_count,
                                    self.max_score,
                                    self.results,
                                    depth,
                                );
                            }
                            if score >= self.target {
                                let mut key = candidate.key.clone();
                                key.push(shift);
                                candidates.push(BeamCandidate { key, score });
                            }
                        }
                    }
                    candidates
                }
                SearchMode::Parallel => {
                    let Some((nodes, candidates)) =
                        self.evaluate_frontier_parallel(&frontier, depth - 1, interrupted)
                    else {
                        self.submit_shift_path_batch(&mut pending_appends)
                            .map_err(std::io::Error::other)?;
                        store.flush().map_err(std::io::Error::other)?;
                        self.write_saved_prefix_batch_checkpoint(
                            checkpoint_path,
                            mode,
                            depth,
                            after_node_id,
                        )?;
                        return Ok((SharedResults::default(), false));
                    };
                    self.node_count += nodes;
                    candidates
                }
            };
            for candidate in candidates {
                self.aggregate_target_result(depth, candidate.score, &candidate.key)
                    .map_err(std::io::Error::other)?;
                self.aggregate_leaf_result(candidate.score, &candidate.key)
                    .map_err(std::io::Error::other)?;
                pending_appends.push((candidate.score, candidate.key));
                if pending_appends.len() == SHIFT_PATH_BATCH_SIZE {
                    self.submit_shift_path_batch(&mut pending_appends)
                        .map_err(std::io::Error::other)?;
                }
            }
            self.submit_shift_path_batch(&mut pending_appends)
                .map_err(std::io::Error::other)?;
            store.flush().map_err(std::io::Error::other)?;
            if let Some(reporter) = self.progress_reporter.as_ref() {
                reporter.add_prefix_done(prefix_count);
            }
            Self::update_progress(
                self.progress_reporter.as_deref(),
                self.node_count,
                self.max_score,
                self.results,
                depth,
            );
        }
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
        let (_, completed) = self.search_beam_interruptible(
            depth,
            checkpoint_path,
            resume_path,
            interrupted,
            SearchMode::Sequential,
        )?;
        Ok(completed)
    }

    fn search_beam_interruptible(
        &mut self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        resume_path: Option<&Path>,
        interrupted: &AtomicBool,
        mode: SearchMode,
    ) -> Result<(SharedResults, bool), Box<dyn std::error::Error>> {
        if let Some(path) = resume_path {
            let checkpoint = Self::load_checkpoint(path)?;
            if checkpoint.mode != mode {
                return Err(format!(
                    "{:?} mode checkpoint can only be resumed with --mode {}",
                    checkpoint.mode,
                    match checkpoint.mode {
                        SearchMode::Sequential => "sequential",
                        SearchMode::Parallel => "parallel",
                    }
                )
                .into());
            }
            self.apply_checkpoint_config(&checkpoint, depth)?;
            if checkpoint.beam_saved_prefix_batches {
                return self.search_saved_prefix_batches(
                    depth,
                    checkpoint_path,
                    interrupted,
                    mode,
                    checkpoint.prefix_after_node_id,
                );
            }
        } else if depth > 1
            && self
                .has_previous_depth_paths(depth)
                .map_err(std::io::Error::other)?
        {
            return self.search_saved_prefix_batches(
                depth,
                checkpoint_path,
                interrupted,
                mode,
                None,
            );
        }

        let (mut frontier, mut level, next_frontier, mut next_index, mut next_shift) = if let Some(
            path,
        ) =
            resume_path
        {
            let checkpoint = Self::load_checkpoint(path)?;
            if checkpoint.mode != mode {
                return Err(format!(
                    "{:?} mode checkpoint can only be resumed with --mode {}",
                    checkpoint.mode,
                    match checkpoint.mode {
                        SearchMode::Sequential => "sequential",
                        SearchMode::Parallel => "parallel",
                    }
                )
                .into());
            }
            self.apply_checkpoint_config(&checkpoint, depth)?;
            let frontier = checkpoint.beam_frontier.ok_or(
                    "Checkpoint predates beam search and cannot be resumed; use a different --output-dir",
                )?;
            let next_frontier = if checkpoint.beam_next_frontier_checkpointed {
                if checkpoint.beam_level < depth {
                    self.replace_shift_path_depth(
                        checkpoint.beam_level + 1,
                        &checkpoint
                            .beam_next_frontier
                            .iter()
                            .map(|candidate| (candidate.score, candidate.key.clone()))
                            .collect::<Vec<_>>(),
                    )
                    .map_err(std::io::Error::other)?;
                }
                checkpoint.beam_next_frontier
            } else if self.shift_path_store.is_some() && checkpoint.beam_level < depth {
                self.load_candidates_from_store(checkpoint.beam_level + 1)
                    .map_err(std::io::Error::other)?
            } else {
                checkpoint.beam_next_frontier
            };
            self.node_count = checkpoint.node_count;
            (
                frontier,
                checkpoint.beam_level,
                next_frontier,
                checkpoint.beam_next_index,
                checkpoint.beam_next_shift,
            )
        } else {
            (
                vec![BeamCandidate {
                    key: Vec::new(),
                    score: self.zero_mask.count_ones(),
                }],
                0,
                Vec::new(),
                0,
                0,
            )
        };

        self.max_score = 0;
        self.results = 0;
        self.shifts.clear();
        self.target_count = 0;
        self.target_shifts.clear();
        let mut pending_appends = Vec::with_capacity(SHIFT_PATH_BATCH_SIZE);
        let mut candidates = next_frontier;
        let mut current_index = None;
        let mut base_mask = self.zero_mask.clone();
        let mut restore_next_shift = resume_path.is_some();
        let mut next_checkpoint = self.node_count.saturating_add(self.checkpoint_interval);
        let mut next_progress = self
            .node_count
            .saturating_add(PROGRESS_UPDATE_NODE_INTERVAL);
        Self::update_progress(
            self.progress_reporter.as_deref(),
            self.node_count,
            self.max_score,
            self.results,
            level,
        );

        loop {
            if interrupted.load(Ordering::SeqCst) {
                self.submit_shift_path_batch(&mut pending_appends)
                    .map_err(std::io::Error::other)?;
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: candidates.clone(),
                        next_index,
                        next_shift,
                    },
                )?;
                return Ok((SharedResults::default(), false));
            }

            if level == depth {
                for candidate in &frontier {
                    self.aggregate_target_result(depth, candidate.score, &candidate.key)
                        .map_err(std::io::Error::other)?;
                    self.aggregate_leaf_result(candidate.score, &candidate.key)
                        .map_err(std::io::Error::other)?;
                }
                self.submit_shift_path_batch(&mut pending_appends)
                    .map_err(std::io::Error::other)?;
                if let Some(store) = &self.shift_path_store {
                    store.flush().map_err(std::io::Error::other)?;
                }
                let results = SharedResults {
                    max_score: self.max_score,
                    results: self.results,
                    shifts: self.shifts.clone(),
                    target_count: self.target_count,
                    target_shifts: self.target_shifts.clone(),
                };
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: Vec::new(),
                        next_index: 0,
                        next_shift: 0,
                    },
                )?;
                return Ok((results, true));
            }

            if next_index == frontier.len() {
                level += 1;
                self.submit_shift_path_batch(&mut pending_appends)
                    .map_err(std::io::Error::other)?;
                self.replace_shift_path_depth(
                    level,
                    &candidates
                        .iter()
                        .map(|candidate| (candidate.score, candidate.key.clone()))
                        .collect::<Vec<_>>(),
                )
                .map_err(std::io::Error::other)?;
                frontier = if self.shift_path_store.is_some() {
                    candidates.clear();
                    self.load_candidates_from_store(level)
                        .map_err(std::io::Error::other)?
                } else {
                    std::mem::take(&mut candidates)
                };
                next_index = 0;
                next_shift = 0;
                current_index = None;
                restore_next_shift = false;
                if frontier.is_empty() {
                    self.submit_shift_path_batch(&mut pending_appends)
                        .map_err(std::io::Error::other)?;
                    let results = SharedResults::default();
                    self.write_beam_checkpoint(
                        checkpoint_path,
                        BeamCheckpointData {
                            mode,
                            depth,
                            frontier: &frontier,
                            level,
                            next_frontier: Vec::new(),
                            next_index: 0,
                            next_shift: 0,
                        },
                    )?;
                    return Ok((results, true));
                }
                continue;
            }

            if current_index != Some(next_index) {
                base_mask = self.rebuild_masks(depth, &frontier[next_index].key)[level].clone();
                if !restore_next_shift {
                    next_shift = self.primes[level];
                }
                restore_next_shift = false;
                current_index = Some(next_index);
            }
            if next_shift == 0 {
                next_index += 1;
                current_index = None;
                continue;
            }

            next_shift -= 1;
            let shift = next_shift;
            let mut mask = self.zero_mask.clone();
            let score = mask.bitand_into_count(&base_mask, &self.shift_table[level][shift]);
            self.node_count += 1;
            if score >= self.target {
                let mut key = frontier[next_index].key.clone();
                key.push(shift);
                candidates.push(BeamCandidate {
                    key: key.clone(),
                    score,
                });
                if self.shift_path_store.is_some() {
                    pending_appends.push((score, key.clone()));
                    if pending_appends.len() >= SHIFT_PATH_BATCH_SIZE {
                        self.submit_shift_path_batch(&mut pending_appends)
                            .map_err(std::io::Error::other)?;
                    }
                }
            }

            if self.node_count >= next_progress {
                Self::update_progress(
                    self.progress_reporter.as_deref(),
                    self.node_count,
                    self.max_score,
                    self.results,
                    level + 1,
                );
                next_progress = self
                    .node_count
                    .saturating_add(PROGRESS_UPDATE_NODE_INTERVAL);
            }

            if self.node_count >= next_checkpoint {
                self.submit_shift_path_batch(&mut pending_appends)
                    .map_err(std::io::Error::other)?;
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: candidates.clone(),
                        next_index,
                        next_shift,
                    },
                )?;
                next_checkpoint = self.node_count.saturating_add(self.checkpoint_interval);
            }
        }
    }

    fn write_beam_checkpoint(
        &self,
        path: Option<&Path>,
        data: BeamCheckpointData<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(path) = path else {
            return Ok(());
        };
        if let Some(store) = &self.shift_path_store {
            store.flush().map_err(std::io::Error::other)?;
        }
        self.write_checkpoint_file(
            path,
            &Checkpoint {
                mode: data.mode,
                depth: data.depth,
                stack: Vec::new(),
                key: Vec::new(),
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
                beam_frontier: Some(data.frontier.to_vec()),
                beam_level: data.level,
                beam_next_frontier: data.next_frontier,
                beam_next_index: data.next_index,
                beam_next_shift: data.next_shift,
                beam_saved_prefix_batches: false,
                beam_next_frontier_checkpointed: true,
                prefix_after_node_id: None,
            },
        )
    }

    fn write_saved_prefix_batch_checkpoint(
        &self,
        path: Option<&Path>,
        mode: SearchMode,
        depth: usize,
        prefix_after_node_id: Option<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let Some(path) = path else {
            return Ok(());
        };
        self.write_checkpoint_file(
            path,
            &Checkpoint {
                key: Vec::new(),
                mode,
                depth,
                stack: Vec::new(),
                max_score: 0,
                results: 0,
                target_count: 0,
                node_count: self.node_count,
                split_depth: 0,
                completed: Vec::new(),
                in_progress: Vec::new(),
                pending_prefixes: Vec::new(),
                work_prefixes: None,
                config_hash: self.compute_config_hash(),
                beam_frontier: Some(Vec::new()),
                beam_level: 0,
                beam_next_frontier: Vec::new(),
                beam_next_index: 0,
                beam_next_shift: 0,
                beam_saved_prefix_batches: true,
                beam_next_frontier_checkpointed: false,
                prefix_after_node_id,
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
                "Checkpoint search depth does not match current configuration (depth={})",
                checkpoint.depth
            )
            .into());
        }
        if checkpoint.beam_level > depth {
            return Err(format!(
                "Checkpoint beam level {} exceeds search depth {}",
                checkpoint.beam_level, depth
            )
            .into());
        }
        for candidate in checkpoint
            .beam_frontier
            .iter()
            .flatten()
            .chain(checkpoint.beam_next_frontier.iter())
        {
            self.validate_path(&candidate.key, depth)?;
        }
        if checkpoint.beam_next_index > checkpoint.beam_frontier.as_ref().map_or(0, Vec::len) {
            return Err("Checkpoint frontier index is out of range".into());
        }
        if checkpoint.beam_level < depth
            && checkpoint.beam_next_shift > self.primes[checkpoint.beam_level]
        {
            return Err("Checkpoint shift position is out of range".into());
        }
        let current_hash = self.compute_config_hash();
        if checkpoint.config_hash != 0 && checkpoint.config_hash != current_hash {
            return Err(
                format!(
                    "Checkpoint configuration does not match current settings. Please use a different --output-dir (checkpoint_hash={}, current_hash={})",
                    checkpoint.config_hash, current_hash
                )
                .into(),
            );
        }
        Ok(())
    }

    fn validate_path(
        &self,
        path: &[usize],
        depth: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if path.len() > depth {
            return Err(format!(
                "Checkpoint path length {} exceeds search depth {}",
                path.len(),
                depth
            )
            .into());
        }
        for (level, &shift) in path.iter().enumerate() {
            if shift >= self.primes[level] {
                return Err(
                    format!("Checkpoint shift {} is invalid at level {}", shift, level).into(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn compute_config_hash(&self) -> u64 {
        let mut hash = 0u64;
        hash = hash.wrapping_mul(31).wrapping_add(self.primes.len() as u64);
        for &p in &self.primes {
            hash = hash.wrapping_mul(31).wrapping_add(p as u64);
        }
        hash = hash
            .wrapping_mul(31)
            .wrapping_add(self.zero_mask.size() as u64);
        hash = hash.wrapping_mul(31).wrapping_add(self.target as u64);
        hash = hash.wrapping_mul(31).wrapping_add(BEAM_POLICY_VERSION);
        hash
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
        &mut self,
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
        &mut self,
        depth: usize,
        checkpoint_path: Option<&Path>,
        resume_path: Option<&Path>,
        interrupted: &AtomicBool,
    ) -> Result<(SharedResults, bool), Box<dyn std::error::Error>> {
        if let Some(path) = resume_path {
            let checkpoint = Self::load_checkpoint(path)?;
            if checkpoint.mode != SearchMode::Parallel {
                return Err(format!(
                    "{:?} mode checkpoint can only be resumed with --mode parallel",
                    checkpoint.mode
                )
                .into());
            }
            self.apply_checkpoint_config(&checkpoint, depth)?;
            if checkpoint.beam_saved_prefix_batches {
                return self.search_saved_prefix_batches(
                    depth,
                    checkpoint_path,
                    interrupted,
                    SearchMode::Parallel,
                    checkpoint.prefix_after_node_id,
                );
            }
        } else if depth > 1
            && self
                .has_previous_depth_paths(depth)
                .map_err(std::io::Error::other)?
        {
            return self.search_saved_prefix_batches(
                depth,
                checkpoint_path,
                interrupted,
                SearchMode::Parallel,
                None,
            );
        }

        let (mut frontier, mut level) = if let Some(path) = resume_path {
            let checkpoint = Self::load_checkpoint(path)?;
            if checkpoint.mode != SearchMode::Parallel {
                return Err(format!(
                    "{:?} mode checkpoint can only be resumed with --mode parallel",
                    checkpoint.mode
                )
                .into());
            }
            self.apply_checkpoint_config(&checkpoint, depth)?;
            let frontier = checkpoint.beam_frontier.ok_or(
                "Checkpoint predates beam search and cannot be resumed; use a different --output-dir",
            )?;
            if checkpoint.beam_next_frontier_checkpointed && checkpoint.beam_level > 0 {
                self.replace_shift_path_depth(
                    checkpoint.beam_level,
                    &frontier
                        .iter()
                        .map(|candidate| (candidate.score, candidate.key.clone()))
                        .collect::<Vec<_>>(),
                )
                .map_err(std::io::Error::other)?;
            }
            self.node_count = checkpoint.node_count;
            (frontier, checkpoint.beam_level)
        } else {
            (
                vec![BeamCandidate {
                    key: Vec::new(),
                    score: self.zero_mask.count_ones(),
                }],
                0,
            )
        };

        self.max_score = 0;
        self.results = 0;
        self.shifts.clear();
        self.target_count = 0;
        self.target_shifts.clear();
        let mut next_checkpoint = self.node_count.saturating_add(self.checkpoint_interval);
        Self::update_progress(
            self.progress_reporter.as_deref(),
            self.node_count,
            self.max_score,
            self.results,
            level,
        );

        loop {
            if interrupted.load(Ordering::SeqCst) {
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode: SearchMode::Parallel,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: Vec::new(),
                        next_index: 0,
                        next_shift: 0,
                    },
                )?;
                return Ok((SharedResults::default(), false));
            }

            if level == depth {
                for candidate in &frontier {
                    self.aggregate_target_result(depth, candidate.score, &candidate.key)
                        .map_err(std::io::Error::other)?;
                    self.aggregate_leaf_result(candidate.score, &candidate.key)
                        .map_err(std::io::Error::other)?;
                }
                if let Some(store) = &self.shift_path_store {
                    store.flush().map_err(std::io::Error::other)?;
                }
                let results = SharedResults {
                    max_score: self.max_score,
                    results: self.results,
                    shifts: self.shifts.clone(),
                    target_count: self.target_count,
                    target_shifts: self.target_shifts.clone(),
                };
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode: SearchMode::Parallel,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: Vec::new(),
                        next_index: 0,
                        next_shift: 0,
                    },
                )?;
                return Ok((results, true));
            }

            let Some((nodes, candidates)) =
                self.evaluate_frontier_parallel(&frontier, level, interrupted)
            else {
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode: SearchMode::Parallel,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: Vec::new(),
                        next_index: 0,
                        next_shift: 0,
                    },
                )?;
                return Ok((SharedResults::default(), false));
            };
            self.node_count += nodes;
            self.persist_candidates(&candidates)
                .map_err(std::io::Error::other)?;

            level += 1;
            frontier = if self.shift_path_store.is_some() {
                self.load_candidates_from_store(level)
                    .map_err(std::io::Error::other)?
            } else {
                candidates
            };

            Self::update_progress(
                self.progress_reporter.as_deref(),
                self.node_count,
                self.max_score,
                self.results,
                level,
            );
            if self.node_count >= next_checkpoint {
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode: SearchMode::Parallel,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: Vec::new(),
                        next_index: 0,
                        next_shift: 0,
                    },
                )?;
                next_checkpoint = self.node_count.saturating_add(self.checkpoint_interval);
            }

            if frontier.is_empty() {
                self.write_beam_checkpoint(
                    checkpoint_path,
                    BeamCheckpointData {
                        mode: SearchMode::Parallel,
                        depth,
                        frontier: &frontier,
                        level,
                        next_frontier: Vec::new(),
                        next_index: 0,
                        next_shift: 0,
                    },
                )?;
                return Ok((SharedResults::default(), true));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_shift_table, State};

    #[test]
    fn build_shift_table_creates_expected_complement_masks() {
        let table = build_shift_table(&[2], 6);
        assert_eq!(table.len(), 1);
        assert_eq!(table[0].len(), 2);
        assert_eq!(table[0][0].count_ones(), 3);
        assert_eq!(table[0][1].count_ones(), 3);
    }

    #[test]
    fn progress_reporter_exposes_latest_search_metrics() {
        let progress = super::SearchProgress::default();
        progress.update(12_345, 450, 7, 4);

        assert_eq!(
            progress.snapshot(),
            super::SearchProgressSnapshot {
                node_count: 12_345,
                max_score: 450,
                results: 7,
                depth: 4,
                prefix_done: 0,
                prefix_total: 0,
                db_insert_done: 0,
                db_insert_total: 0,
                db_insert_active: false,
            }
        );
    }

    #[test]
    fn progress_reporter_tracks_bulk_db_insert() {
        let progress = super::SearchProgress::default();
        progress.begin_db_insert(256);

        let snapshot = progress.snapshot();
        assert!(snapshot.db_insert_active);
        assert_eq!(snapshot.db_insert_done, 0);
        assert_eq!(snapshot.db_insert_total, 256);

        progress.complete_db_insert(256);
        let snapshot = progress.snapshot();
        assert!(!snapshot.db_insert_active);
        assert_eq!(snapshot.db_insert_done, 256);
        assert_eq!(snapshot.db_insert_total, 256);
    }

    #[test]
    fn progress_reporter_adds_individual_nodes_during_search() {
        let progress = super::SearchProgress::default();
        progress.update(1_000, 0, 0, 2);
        progress.add_nodes(1);

        assert_eq!(progress.snapshot().node_count, 1_001);
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
        let progress = std::sync::Arc::new(super::SearchProgress::default());
        parallel.set_progress_reporter(std::sync::Arc::clone(&progress));
        let result = parallel.search_parallel(2, None, None).unwrap();

        assert!(sequential.results > 0);
        assert_eq!(result.max_score, sequential.max_score);
        assert_eq!(result.results, sequential.results);
        assert_eq!(result.shifts.len(), result.results);
        assert_eq!(progress.snapshot().node_count, parallel.node_count);
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
    fn beam_search_keeps_all_unpruned_candidates_at_each_level() {
        let primes = vec![23, 23, 2];
        let cols = 529;
        let table = build_shift_table(&primes, cols);
        let mut sequential = State::new(primes.clone(), cols, table.clone());
        sequential.target = 0;
        sequential.search_with_checkpoint(3, None, None).unwrap();

        let mut parallel = State::new(primes, cols, table);
        parallel.target = 0;
        let parallel_result = parallel.search_parallel(3, None, None).unwrap();

        let expected_node_count = (23 + 23 * 23 + 23 * 23 * 2) as u64;
        assert_eq!(sequential.node_count, expected_node_count);
        assert_eq!(parallel.node_count, expected_node_count);
        assert_eq!(parallel_result.max_score, sequential.max_score);
        assert_eq!(parallel_result.results, sequential.results);
        assert!(sequential.shifts.iter().all(|path| path.len() == 3));
        assert!(parallel_result.shifts.iter().all(|path| path.len() == 3));
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
        let database_path =
            unique_checkpoint_path("previous-depth-paths").with_extension("rocksdb");
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
    fn beam_search_is_seeded_only_from_previous_depth_records() {
        let primes = vec![2, 3];
        let cols = 4;
        let table = build_shift_table(&primes, cols);
        let database_path =
            unique_checkpoint_path("beam-seeded-from-previous-depth").with_extension("rocksdb");

        let mut previous = State::new(primes.clone(), cols, table.clone());
        previous
            .set_shift_path_store(&database_path, 1, true)
            .unwrap();
        previous
            .shift_path_store
            .as_ref()
            .unwrap()
            .append(2, &[1])
            .unwrap();
        drop(previous);

        let mut state = State::new(primes, cols, table);
        state.target = 0;
        state
            .set_shift_path_store(&database_path, 2, false)
            .unwrap();
        let result = state.search_parallel(2, None, None).unwrap();

        assert!(result.shifts.iter().all(|path| path[0] == 1));

        drop(state);
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
        let mut state = State::new(primes, 4, table);
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
    fn beam_checkpoint_resumes_an_incomplete_level() {
        let primes = vec![2, 3];
        let cols = 4;
        let depth = 2;
        let table = build_shift_table(&primes, cols);
        let mut expected_state = State::new(primes.clone(), cols, table.clone());
        expected_state.target = 0;
        expected_state
            .search_with_checkpoint(depth, None, None)
            .unwrap();

        let path = unique_checkpoint_path("beam-incomplete-level");
        let mut state = State::new(primes.clone(), cols, table);
        state.target = 0;
        state
            .write_checkpoint_file(
                &path,
                &super::Checkpoint {
                    mode: super::SearchMode::Sequential,
                    depth,
                    stack: Vec::new(),
                    key: Vec::new(),
                    max_score: 0,
                    results: 0,
                    target_count: 0,
                    node_count: 1,
                    split_depth: 0,
                    completed: Vec::new(),
                    in_progress: Vec::new(),
                    pending_prefixes: Vec::new(),
                    work_prefixes: None,
                    config_hash: state.compute_config_hash(),
                    beam_frontier: Some(vec![super::BeamCandidate {
                        key: Vec::new(),
                        score: cols,
                    }]),
                    beam_level: 0,
                    beam_next_frontier: vec![super::BeamCandidate {
                        key: vec![1],
                        score: 2,
                    }],
                    beam_next_index: 0,
                    beam_next_shift: 1,
                    beam_saved_prefix_batches: false,
                    beam_next_frontier_checkpointed: true,
                    prefix_after_node_id: None,
                },
            )
            .unwrap();

        let mut resumed = State::new(primes, cols, build_shift_table(&[2, 3], cols));
        resumed.target = 0;
        resumed
            .search_with_checkpoint(depth, Some(&path), Some(&path))
            .unwrap();

        assert_eq!(resumed.max_score, expected_state.max_score);
        assert_eq!(resumed.results, expected_state.results);
        assert_eq!(resumed.shifts, expected_state.shifts);
        assert_eq!(resumed.node_count, expected_state.node_count);

        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(backup_path);
    }

    #[test]
    fn beam_checkpoint_replaces_inconsistent_database_candidates() {
        let primes = vec![2, 3];
        let cols = 4;
        let depth = 2;
        let path = unique_checkpoint_path("beam-db-recovery");
        let database_path = path.with_extension("rocksdb");
        let mut state = State::new(primes.clone(), cols, build_shift_table(&primes, cols));
        state.target = 0;
        state
            .set_shift_path_store(&database_path, depth, true)
            .unwrap();
        state
            .shift_path_store
            .as_ref()
            .unwrap()
            .append(1, &[0])
            .unwrap();
        state
            .write_checkpoint_file(
                &path,
                &super::Checkpoint {
                    mode: super::SearchMode::Sequential,
                    depth,
                    stack: Vec::new(),
                    key: Vec::new(),
                    max_score: 0,
                    results: 0,
                    target_count: 0,
                    node_count: 2,
                    split_depth: 0,
                    completed: Vec::new(),
                    in_progress: Vec::new(),
                    pending_prefixes: Vec::new(),
                    work_prefixes: None,
                    config_hash: state.compute_config_hash(),
                    beam_frontier: Some(vec![super::BeamCandidate {
                        key: Vec::new(),
                        score: cols,
                    }]),
                    beam_level: 0,
                    beam_next_frontier: vec![super::BeamCandidate {
                        key: vec![1],
                        score: 2,
                    }],
                    beam_next_index: 1,
                    beam_next_shift: 0,
                    beam_saved_prefix_batches: false,
                    beam_next_frontier_checkpointed: true,
                    prefix_after_node_id: None,
                },
            )
            .unwrap();

        state
            .search_with_checkpoint(depth, Some(&path), Some(&path))
            .unwrap();
        let store = state.shift_path_store.as_ref().unwrap();
        assert_eq!(store.load_full_depth_paths(1).unwrap(), vec![vec![1]]);

        drop(state);
        let backup_path = path.with_extension("bak");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(backup_path);
        std::fs::remove_dir_all(database_path).unwrap();
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
    fn parallel_beam_checkpoint_resumes_completed_search() {
        let primes = vec![2, 3, 5];
        let cols = 8;
        let depth = 3;
        let table = build_shift_table(&primes, cols);
        let mut state = State::new(primes.clone(), cols, table.clone());
        state.target = 0;
        let path = unique_checkpoint_path("parallel-completed");
        let database_path = path.with_extension("rocksdb");
        state
            .set_shift_path_store(&database_path, depth, true)
            .unwrap();
        let expected = state.search_parallel(depth, Some(&path), None).unwrap();
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
}
