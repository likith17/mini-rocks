//! MiniRocks: a small, durable key-value store built on a log-structured
//! merge tree.
//!
//! Phase 1 contains the write-ahead log, the in-memory memtable, and crash
//! recovery. SSTables, flushing, compaction and the manifest come in later
//! phases.
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
//! never loses an acknowledged write. See `DECISIONS.md` for the details.

mod db;
mod error;
mod fsutil;
pub mod memtable;
mod options;
pub mod wal;

pub use db::{Db, RecoveryReport, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use error::{Error, Result};
pub use options::{Options, SyncPolicy, WalRecoveryMode};
