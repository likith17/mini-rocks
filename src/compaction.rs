//! Size-tiered compaction policy.
//!
//! SSTables are considered in age order (newest first). A *tier* is a run
//! of tables adjacent in age whose sizes are similar: the largest is at most
//! `ratio` times the smallest. Tables smaller than `floor` count as `floor`,
//! so freshly flushed tables of slightly different sizes group together.
//! When a tier holds at least `min_merge` tables, up to `max_merge` of them
//! are merged into one. Among several eligible tiers, the one with the
//! fewest total bytes wins: it is the cheapest, and merging small tables
//! first keeps the table count (and so read amplification) down fastest.
//!
//! Only runs *adjacent in age* are ever merged. Their sequence-number ranges
//! then never interleave with any other table's, which is what lets the
//! read path order tables by `max_seq` (see DECISIONS.md §8).

use std::ops::Range;

/// Pick the run of tables to merge, as an index range into `sizes`
/// (newest first), or `None` if no tier is ready.
pub fn pick_size_tiered(sizes: &[u64], floor: u64, min_merge: usize, max_merge: usize, ratio: f64) -> Option<Range<usize>> {
    let eff = |s: u64| s.max(floor) as f64;
    let mut best: Option<(Range<usize>, u64)> = None;
    let mut start = 0;
    while start < sizes.len() {
        let (mut lo, mut hi) = (eff(sizes[start]), eff(sizes[start]));
        let mut end = start + 1;
        while end < sizes.len() {
            let s = eff(sizes[end]);
            let (nlo, nhi) = (lo.min(s), hi.max(s));
            if nhi > nlo * ratio {
                break;
            }
            (lo, hi) = (nlo, nhi);
            end += 1;
        }
        if end - start >= min_merge {
            let run = start..start + (end - start).min(max_merge);
            let total: u64 = sizes[run.clone()].iter().sum();
            if best.as_ref().is_none_or(|(_, t)| total < *t) {
                best = Some((run, total));
            }
        }
        start = end;
    }
    best.map(|(run, _)| run)
}

#[cfg(test)]
mod tests {
    use super::pick_size_tiered as pick;

    #[test]
    fn needs_min_merge_similar_tables() {
        assert_eq!(pick(&[], 1, 4, 32, 3.0), None);
        assert_eq!(pick(&[10, 10, 10], 1, 4, 32, 3.0), None);
        assert_eq!(pick(&[10, 10, 10, 10], 1, 4, 32, 3.0), Some(0..4));
        // Newest first: three fresh tables, then four big ones.
        assert_eq!(pick(&[10, 10, 10, 1000, 1000, 1000, 1000], 1, 4, 32, 3.0), Some(3..7));
    }

    #[test]
    fn floor_groups_small_tables() {
        // Without the floor, 1 vs 9 breaks a ratio of 3.
        assert_eq!(pick(&[1, 9, 2, 5], 1, 4, 32, 3.0), None);
        assert_eq!(pick(&[1, 9, 2, 5], 10, 4, 32, 3.0), Some(0..4));
    }

    #[test]
    fn prefers_cheapest_tier_and_caps_width() {
        let sizes = [10, 10, 10, 10, 100, 100, 100, 100];
        assert_eq!(pick(&sizes, 1, 4, 32, 3.0), Some(0..4));
        assert_eq!(pick(&[10; 40], 1, 4, 32, 3.0), Some(0..32));
    }

    #[test]
    fn tiers_never_mix_across_a_size_jump() {
        // Without contiguity a picker might join the 10s around the 1000.
        assert_eq!(pick(&[10, 10, 1000, 10, 10], 1, 4, 32, 3.0), None);
    }
}
