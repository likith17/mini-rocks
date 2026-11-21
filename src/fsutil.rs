use std::io;
use std::path::Path;

/// Make directory changes (file creation, rename, delete) durable.
///
/// On Unix, a newly created file's directory entry is only durable once the
/// directory itself is fsynced. On Windows the standard library cannot open a
/// directory handle, and NTFS journals metadata changes itself, so this is a
/// no-op there (documented in DECISIONS.md as a known limitation).
#[cfg(unix)]
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
pub fn sync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}
