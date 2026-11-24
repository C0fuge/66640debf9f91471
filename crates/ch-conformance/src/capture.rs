//! Bound raw input and decoded JSON structure across every process in a run.
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

const MAX_BYTES: usize = 32 * 1024 * 1024;
const MAX_LINES: usize = 8192;
const MAX_SEPARATORS: usize = 250_000;

#[derive(Default)]
struct Usage {
    bytes: usize,
    lines: usize,
    separators: usize,
}

#[derive(Clone)]
pub(crate) struct CaptureBudget {
    usage: Arc<Mutex<Usage>>,
    failure: watch::Sender<Option<&'static str>>,
}

impl Default for CaptureBudget {
    fn default() -> Self {
        Self {
            usage: Arc::default(),
            failure: watch::channel(None).0,
        }
    }
}

impl CaptureBudget {
    pub fn subscribe(&self) -> watch::Receiver<Option<&'static str>> {
        self.failure.subscribe()
    }

    pub fn failure(&self) -> Option<&'static str> {
        *self.failure.borrow()
    }

    pub fn accept(&self, frame: &[u8]) -> bool {
        let mut used = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        used.bytes = used.bytes.saturating_add(frame.len());
        used.lines += 1;
        let reason = if frame.len() > ch_protocol::MAX_FRAME_BYTES {
            Some("stdout frame exceeds the protocol frame limit")
        } else if used.bytes > MAX_BYTES || used.lines > MAX_LINES {
            Some("stdout capture byte or frame limit exceeded")
        } else {
            // A short array of tiny objects can occupy much more heap than its wire bytes.
            let mut quoted = false;
            let mut escaped = false;
            for &b in frame {
                if escaped {
                    escaped = false;
                    continue;
                }
                match b {
                    b'\\' if quoted => escaped = true,
                    b'"' => quoted = !quoted,
                    b'[' | b'{' | b':' | b',' if !quoted => used.separators += 1,
                    _ => {}
                }
            }
            (used.separators > MAX_SEPARATORS)
                .then_some("decoded JSON structure exceeds the capture limit")
        };
        if let Some(reason) = reason {
            self.failure.send_replace(Some(reason));
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_streams_count_blanks_and_dense_structures() {
        let budget = CaptureBudget::default();
        for _ in 0..MAX_LINES {
            assert!(budget.accept(b"\n"));
        }
        assert!(!budget.accept(b"\n"));
        assert!(budget.failure().is_some());
        let budget = CaptureBudget::default();
        assert!(budget.accept(br#""[{,,]""#));
        assert!(!budget.accept(&vec![b','; MAX_SEPARATORS + 1]));
    }
}
