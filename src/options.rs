/// When the WAL is fsynced. See the durability table in the crate docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `sync_data` after every write, before the write is acknowledged.
    /// Acknowledged writes survive power loss.
    Always,
    /// `sync_data` after every `n`-th write (`n >= 1`). On power loss, up to
    /// `n - 1` acknowledged writes (those since the last sync) may be lost.
    EveryN(u32),
    /// Never fsync explicitly (except on clean shutdown and [`crate::Db::sync`]).
    /// Writes reach the OS page cache, so they survive a process crash but
    /// not an OS crash or power loss.
    Never,
}

/// What recovery does when the WAL contains an invalid record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecoveryMode {
    /// Discard an invalid *tail* of the log: a record cut short by a crash,
    /// or garbage after the last valid record. If any valid record appears
    /// *after* the invalid one, the damage cannot be a torn tail, so `open`
    /// returns [`crate::Error::Corruption`] and modifies nothing.
    ///
    /// Recommended with [`SyncPolicy::Always`], where the only unsynced data
    /// at any moment is the one in-flight record.
    TolerateTornTail,
    /// Recover the longest valid prefix of the log and discard everything
    /// after the first invalid record, even if valid records follow it.
    /// Always opens, but can drop acknowledged writes if the log was
    /// damaged in the middle. Use this after a power loss with
    /// `EveryN`/`Never`, where the OS may have written unsynced pages out of
    /// order.
    PointInTime,
}

/// Database options.
#[derive(Debug, Clone)]
pub struct Options {
    pub sync: SyncPolicy,
    pub wal_recovery: WalRecoveryMode,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync: SyncPolicy::Always,
            wal_recovery: WalRecoveryMode::TolerateTornTail,
        }
    }
}
