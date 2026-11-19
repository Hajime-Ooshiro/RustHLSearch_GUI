pub const DEFAULT_DEPTH: usize = 8;
pub const DEFAULT_COLS: usize = 3159;
pub const DEFAULT_CHECKPOINT_INTERVAL: u64 = 100_000;
pub const MAX_PRIME: usize = 1579;
pub const MAX_SEARCH_DEPTH: usize = 249;
pub const TARGET_SCORE: usize = 447;
#[cfg(not(test))]
pub const SHARD_COUNT: usize = 210;
#[cfg(test)]
pub const SHARD_COUNT: usize = 4;
pub const MAX_DISPLAY_RESULTS: usize = 30;
