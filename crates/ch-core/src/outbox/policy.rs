//! The protocol's NETWORK error includes disconnection after dispatch, and client_id is
//! correlation rather than guaranteed deduplication. New sends reconcile such errors. Safe
//! pre-dispatch failures and idempotent operations may retry with bounded backoff. A platform
//! FLOOD_WAIT sets a durable deadline; an account that needs sign-in waits for a connected event.

use std::time::Duration;

use ch_protocol::{ErrorCode, ErrorObject};

use crate::host::HostError;

use super::OutboxKind;

/// Tunables; [`Default`] is the production policy.
#[derive(Debug, Clone)]
pub struct OutboxPolicy {
    /// First backoff after a retryable failure (doubles each time).
    pub backoff_base: Duration,
    /// Backoff cap.
    pub backoff_cap: Duration,
    /// Attempts before a retryable failure becomes permanent (counts every attempt, FLOOD_WAIT
    /// and pauses included).
    pub max_attempts: u32,
    /// How long (after the account is online) to wait for the platform's echo of a send whose
    /// outcome is unknown before giving up on it.
    pub reconcile_timeout: Duration,
    /// How often the reconciler polls the local echo row.
    pub reconcile_poll: Duration,
    /// After a pause-class error, do not retry sooner than this even if the host says the
    /// account is online (guards against a plugin that keeps answering `AUTH_REQUIRED`).
    pub pause_min: Duration,
    /// Independent conversations of one account that may send at the same time.
    pub max_parallel_per_account: usize,
}

impl Default for OutboxPolicy {
    fn default() -> Self {
        Self {
            backoff_base: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(60),
            max_attempts: 10,
            reconcile_timeout: Duration::from_secs(30),
            reconcile_poll: Duration::from_secs(1),
            pause_min: Duration::from_secs(5),
            max_parallel_per_account: 4,
        }
    }
}

impl OutboxPolicy {
    /// Backoff before attempt `attempts + 1`, given `attempts` so far.
    pub fn backoff(&self, attempts: u32) -> Duration {
        let n = attempts.saturating_sub(1).min(16);
        let d = self.backoff_base.saturating_mul(1u32 << n);
        d.min(self.backoff_cap)
    }
}

/// What to do after a failed attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Try again after this delay (item stays `queued`, echo keeps `sending`).
    RetryAfter(Duration),
    /// Keep it queued; the lane resumes when the account is online again.
    PauseUntilOnline,
    /// The platform may have taken it: reconcile through the echo.
    Unknown,
    /// Fail for good.
    Permanent,
}

/// Classify a failed attempt. `attempts` is the count including the one that just failed.
pub fn classify(
    policy: &OutboxPolicy,
    kind: OutboxKind,
    attempts: u32,
    err: &HostError,
) -> Verdict {
    let retry = |policy: &OutboxPolicy| {
        if attempts >= policy.max_attempts {
            Verdict::Permanent
        } else {
            Verdict::RetryAfter(policy.backoff(attempts))
        }
    };
    match err {
        HostError::Rpc(e) => classify_rpc(policy, kind, attempts, e),
        HostError::PluginNotRunning { .. }
        | HostError::UnknownPlugin(_)
        | HostError::UnknownAccount(_)
        | HostError::AccountBusy(..) => Verdict::PauseUntilOnline,
        HostError::Io(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => retry(policy),
        HostError::Io(_) | HostError::ShuttingDown => unknown_or_retry(policy, kind, attempts),
        HostError::Json(_)
        | HostError::InvalidMethod(..)
        | HostError::SingleAccount(_)
        | HostError::AccountAlreadyStarted(_) => Verdict::Permanent,
    }
}

fn classify_rpc(
    policy: &OutboxPolicy,
    kind: OutboxKind,
    attempts: u32,
    e: &ErrorObject,
) -> Verdict {
    let exhausted = attempts >= policy.max_attempts;
    let retry = |d: Duration| {
        if exhausted {
            Verdict::Permanent
        } else {
            Verdict::RetryAfter(d)
        }
    };
    match e.code() {
        ErrorCode::FloodWait => {
            let secs = e.data.as_ref().and_then(|d| d.seconds).unwrap_or(60);
            Verdict::RetryAfter(Duration::from_secs(secs.max(1)))
        }
        ErrorCode::Unavailable => retry(policy.backoff(attempts)),
        ErrorCode::AuthRequired | ErrorCode::UnknownAccount => Verdict::PauseUntilOnline,
        ErrorCode::Timeout | ErrorCode::Network | ErrorCode::Internal => {
            unknown_or_retry(policy, kind, attempts)
        }
        ErrorCode::Cancelled => Verdict::Permanent,
        _ => Verdict::Permanent,
    }
}

/// `TIMEOUT` / "plugin restarted": idempotent kinds retry; everything else reconciles.
fn unknown_or_retry(policy: &OutboxPolicy, kind: OutboxKind, attempts: u32) -> Verdict {
    if kind.is_idempotent() {
        if attempts >= policy.max_attempts {
            Verdict::Permanent
        } else {
            Verdict::RetryAfter(policy.backoff(attempts))
        }
    } else {
        Verdict::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rpc(code: ErrorCode) -> HostError {
        HostError::code(code, "x")
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let p = OutboxPolicy::default();
        assert_eq!(p.backoff(1), Duration::from_secs(1));
        assert_eq!(p.backoff(2), Duration::from_secs(2));
        assert_eq!(p.backoff(3), Duration::from_secs(4));
        assert_eq!(p.backoff(7), Duration::from_secs(60));
        assert_eq!(p.backoff(40), Duration::from_secs(60));
    }

    #[test]
    fn buckets() {
        let p = OutboxPolicy::default();
        let k = OutboxKind::SendText;
        assert_eq!(
            classify(&p, OutboxKind::Edit, 1, &rpc(ErrorCode::Network)),
            Verdict::RetryAfter(Duration::from_secs(1))
        );
        assert_eq!(
            classify(&p, OutboxKind::Edit, 10, &rpc(ErrorCode::Network)),
            Verdict::Permanent
        );
        assert_eq!(
            classify(&p, k, 5, &HostError::rpc(ErrorObject::flood_wait(37))),
            Verdict::RetryAfter(Duration::from_secs(37))
        );
        assert_eq!(
            classify(&p, k, 1, &HostError::rpc(ErrorObject::flood_wait(99_999))),
            Verdict::RetryAfter(Duration::from_secs(99_999))
        );
        assert_eq!(
            classify(&p, k, 1, &rpc(ErrorCode::AuthRequired)),
            Verdict::PauseUntilOnline
        );
        assert_eq!(
            classify(&p, k, 1, &rpc(ErrorCode::Timeout)),
            Verdict::Unknown
        );
        assert_eq!(
            classify(&p, OutboxKind::Edit, 1, &rpc(ErrorCode::Timeout)),
            Verdict::RetryAfter(Duration::from_secs(1))
        );
        assert_eq!(
            classify(
                &p,
                k,
                1,
                &HostError::code(ErrorCode::Internal, "plugin restarted")
            ),
            Verdict::Unknown
        );
        assert_eq!(
            classify(&p, k, 1, &rpc(ErrorCode::Internal)),
            Verdict::Unknown
        );
        assert_eq!(
            classify(&p, k, 1, &rpc(ErrorCode::Forbidden)),
            Verdict::Permanent
        );
        assert_eq!(
            classify(&p, k, 1, &rpc(ErrorCode::NotSupported)),
            Verdict::Permanent
        );
        assert_eq!(
            classify(&p, k, 1, &rpc(ErrorCode::InvalidParams)),
            Verdict::Permanent
        );
        assert_eq!(
            classify(&p, k, 1, &HostError::ShuttingDown),
            Verdict::Unknown
        );
    }
}
