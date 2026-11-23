//! Named crash points for the crash-consistency harness.
//!
//! With the `failpoints` feature, `MINIROCKS_CRASHPOINT=name:n` makes the
//! process exit abruptly the `n`-th time execution reaches `point("name")`.
//! No destructors run, nothing more is written or synced, and other threads
//! stop wherever they are. For file state that is the same as `kill -9`:
//! data already handed to the OS survives, and nothing else does.
//!
//! *Torn* points (`torn("name")`) let the caller write part of a buffer
//! before dying, to produce the half-written record a power loss could
//! leave behind.
//!
//! Without the feature, every call compiles to nothing.

#[cfg(feature = "failpoints")]
mod imp {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    struct Target {
        name: String,
        remaining: AtomicU64,
    }

    static TARGET: OnceLock<Option<Target>> = OnceLock::new();

    fn target() -> Option<&'static Target> {
        TARGET
            .get_or_init(|| {
                let spec = std::env::var("MINIROCKS_CRASHPOINT").ok()?;
                let (name, n) = spec.split_once(':')?;
                Some(Target { name: name.to_string(), remaining: AtomicU64::new(n.parse().ok()?) })
            })
            .as_ref()
    }

    pub fn armed(name: &str) -> bool {
        match target() {
            // Wraps past zero to u64::MAX, so a point fires at most once.
            Some(t) if t.name == name => t.remaining.fetch_sub(1, Ordering::SeqCst) == 1,
            _ => false,
        }
    }

    pub fn die(name: &str) -> ! {
        eprintln!("CRASHPOINT {name}");
        std::process::exit(86)
    }
}

/// Exit abruptly here if this crash point is armed.
#[inline]
pub(crate) fn point(name: &'static str) {
    #[cfg(feature = "failpoints")]
    if imp::armed(name) {
        imp::die(name)
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = name;
}

/// For torn-write points: true if the caller should write part of its
/// buffer and then call [`die`].
#[inline]
pub(crate) fn torn(name: &'static str) -> bool {
    #[cfg(feature = "failpoints")]
    return imp::armed(name);
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        false
    }
}

#[cfg(feature = "failpoints")]
pub(crate) fn die(name: &str) -> ! {
    imp::die(name)
}

#[cfg(not(feature = "failpoints"))]
pub(crate) fn die(_name: &str) -> ! {
    unreachable!("crash points are compiled out")
}
