/// BitVec による高速なビットマスク操作構造体
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BitMask {
    data: Vec<u64>,
    size: usize,
}

impl BitMask {
    pub fn new_ones(size: usize) -> Self {
        let num_words = size.div_ceil(64);
        let mut data = vec![u64::MAX; num_words];
        if !size.is_multiple_of(64) {
            let remainder = size % 64;
            data[num_words - 1] = (1u64 << remainder) - 1;
        }
        BitMask { data, size }
    }

    /// `lhs & rhs` を既存の領域へ格納し、1 ビット数を返す
    ///
    /// # Panics
    /// `self`, `lhs`, `rhs` の論理サイズが一致しない場合、デバッグビルドではパニックする。
    #[inline]
    pub fn bitand_into_count(&mut self, lhs: &Self, rhs: &Self) -> usize {
        debug_assert_eq!(
            self.size, lhs.size,
            "bitand_into_count: self.size ({}) != lhs.size ({})",
            self.size, lhs.size
        );
        debug_assert_eq!(
            self.size, rhs.size,
            "bitand_into_count: self.size ({}) != rhs.size ({})",
            self.size, rhs.size
        );
        debug_assert_eq!(self.data.len(), lhs.data.len());
        debug_assert_eq!(self.data.len(), rhs.data.len());

        self.data
            .iter_mut()
            .zip(lhs.data.iter().zip(rhs.data.iter()))
            .map(|(out, (&left, &right))| {
                let value = left & right;
                *out = value;
                value.count_ones() as usize
            })
            .sum()
    }
    /// 1 (true) のビット数をカウント (popcount)
    #[inline]
    pub fn count_ones(&self) -> usize {
        self.data.iter().map(|&w| w.count_ones() as usize).sum()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// 指定したインデックスのビットをセット
    #[inline]
    pub fn set(&mut self, idx: usize, val: bool) {
        if idx >= self.size {
            return;
        }
        let word = idx / 64;
        let bit = idx % 64;
        if val {
            self.data[word] |= 1u64 << bit;
        } else {
            self.data[word] &= !(1u64 << bit);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BitMask;

    #[test]
    fn tracks_logical_size_and_popcount() {
        let mut mask = BitMask::new_ones(65);
        for idx in 0..65 {
            mask.set(idx, false);
        }
        mask.set(0, true);
        mask.set(64, true);
        mask.set(65, true);
        assert_eq!(mask.count_ones(), 2);
        mask.set(0, false);
        assert_eq!(mask.count_ones(), 1);
    }

    #[test]
    fn initializes_only_logical_bits_at_word_boundaries() {
        assert_eq!(BitMask::new_ones(0).count_ones(), 0);
        assert_eq!(BitMask::new_ones(64).count_ones(), 64);
        assert_eq!(BitMask::new_ones(65).count_ones(), 65);
    }

    #[test]
    fn bitand_into_count_retains_only_shared_set_bits() {
        let mut left = BitMask::new_ones(65);
        let mut right = BitMask::new_ones(65);
        left.set(1, false);
        left.set(64, false);
        right.set(0, false);
        right.set(64, false);

        let mut result = BitMask::new_ones(65);
        result.bitand_into_count(&left, &right);

        assert_eq!(result.size(), 65);
        assert_eq!(result.count_ones(), 62);
    }

    #[test]
    fn bitand_into_count_reuses_destination_and_counts_bits() {
        let mut left = BitMask::new_ones(65);
        let mut right = BitMask::new_ones(65);
        left.set(1, false);
        right.set(0, false);
        let mut output = BitMask::new_ones(65);

        assert_eq!(output.bitand_into_count(&left, &right), 63);
        assert_eq!(output.count_ones(), 63);
        assert_eq!(output.size(), 65);
    }
}
