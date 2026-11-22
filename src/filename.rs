//! File naming. WALs and SSTables share one increasing number space, so a
//! higher number always means a newer file.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Wal,
    Sst,
    /// An SSTable still being written. Never referenced; deleted on open.
    TempSst,
    /// A WAL set aside by `PointInTime` recovery. Kept for inspection.
    DiscardedWal,
    Manifest,
}

/// Names the active manifest. Replaced atomically via `CURRENT.tmp`.
pub const CURRENT: &str = "CURRENT";
pub const CURRENT_TMP: &str = "CURRENT.tmp";

pub fn manifest_file_name(number: u64) -> String {
    format!("MANIFEST-{number:06}")
}

pub fn wal_file_name(number: u64) -> String {
    format!("{number:06}.wal")
}

pub fn sst_file_name(number: u64) -> String {
    format!("{number:06}.sst")
}

pub fn temp_sst_file_name(number: u64) -> String {
    format!("{number:06}.sst.tmp")
}

pub fn parse_file_name(name: &str) -> Option<(u64, FileKind)> {
    let digits_only = |d: &str| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit());
    if let Some(digits) = name.strip_prefix("MANIFEST-") {
        return digits_only(digits).then(|| digits.parse().ok()).flatten().map(|n| (n, FileKind::Manifest));
    }
    // Longest suffixes first.
    const SUFFIXES: [(&str, FileKind); 4] = [
        (".wal.discarded", FileKind::DiscardedWal),
        (".sst.tmp", FileKind::TempSst),
        (".wal", FileKind::Wal),
        (".sst", FileKind::Sst),
    ];
    for (suffix, kind) in SUFFIXES {
        if let Some(digits) = name.strip_suffix(suffix) {
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            return Some((digits.parse().ok()?, kind));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_roundtrip() {
        assert_eq!(wal_file_name(1), "000001.wal");
        assert_eq!(parse_file_name("000001.wal"), Some((1, FileKind::Wal)));
        assert_eq!(parse_file_name("1234567.sst"), Some((1234567, FileKind::Sst)));
        assert_eq!(parse_file_name("000003.sst.tmp"), Some((3, FileKind::TempSst)));
        assert_eq!(parse_file_name("000002.wal.discarded"), Some((2, FileKind::DiscardedWal)));
        assert_eq!(parse_file_name("+1.wal"), None);
        assert_eq!(parse_file_name(".sst"), None);
        assert_eq!(parse_file_name("LOCK"), None);
        assert_eq!(parse_file_name("MANIFEST-000004"), Some((4, FileKind::Manifest)));
        assert_eq!(parse_file_name(&manifest_file_name(12)), Some((12, FileKind::Manifest)));
        assert_eq!(parse_file_name("MANIFEST-"), None);
        assert_eq!(parse_file_name("MANIFEST-12x"), None);
        assert_eq!(parse_file_name(CURRENT), None);
    }
}
