//! MiniRocks: a small, durable key-value store built on a log-structured
//! merge tree.
//!
//! Done so far: the write-ahead log, the memtable, crash recovery (phase 1),
//! the SSTable format with background flushing (phase 2), the read path
//! with Bloom filters and a streaming merge iterator (phase 3), the
//! manifest that records which SSTables are live (phase 4), size-tiered
//! background compaction (phase 5), and named crash points for the
//! crash-consistency harness `src/bin/crashtest.rs` (phase 6, behind the
//! `failpoints` feature).
//!
//! # Durability contract
//!
//! A write is *acknowledged* when `put`/`delete` returns `Ok(())`. What an
//! acknowledged write survives depends on [`SyncPolicy`]:
//!
//! | Policy        | Process crash (kill -9, panic) | OS crash / power loss                         |
//! |---------------|--------------------------------|-----------------------------------------------|
//! | `Always`      | survives                       | survives (if the disk honours flush requests) |
//! | `EveryN(n)`   | survives                       | the last unsynced writes (at most n-1) may be lost |
//! | `Never`       | survives                       | any write since the OS last flushed may be lost |
//!
//! Every policy hands each record to the OS with one `write` call before
//! returning, and nothing is buffered in user space, so killing the process
//! never loses an acknowledged write. A WAL is deleted only after the
//! SSTable holding its data has been fsynced and renamed into place. See
//! `DECISIONS.md` for the details.

mod block;
mod bloom;
mod coding;
mod compaction;
mod crashpoint;
mod db;
mod error;
pub mod filename;
mod fsutil;
mod hash;
mod iter;
pub mod manifest;
pub mod memtable;
mod options;
pub mod sstable;
pub mod wal;

#[cfg(test)]
mod testutil;

pub use db::{Db, ReadStats, RecoveryReport, WriteStats, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use iter::DbIter;
pub use error::{Error, Result};
pub use options::{Options, SyncPolicy, WalRecoveryMode};
