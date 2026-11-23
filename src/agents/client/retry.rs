//! Retry policy of [`AgentsClient`](super::AgentsClient): which statuses are
//! retried for which methods, and how long to wait between two attempts.

use std::time::Duration;

use reqwest::{Method, StatusCode};

use super::{RETRY_STATUSES, RETRY_STATUSES_IDEMPOTENT};

pub(super) const DEFAULT_MAX_RETRIES: u32 = 3;
pub(super) const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_millis(500);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
const MAX_BACKOFF: Duration = Duration::from_secs(8);

pub(super) fn is_idempotent(method: &Method) -> bool {
    *method == Method::GET
}

pub(super) fn should_retry(method: &Method, status: StatusCode) -> bool {
    let code = status.as_u16();
    RETRY_STATUSES.contains(&code)
        || (RETRY_STATUSES_IDEMPOTENT.contains(&code) && is_idempotent(method))
}

/// How long to wait before attempt `attempt + 1`: `Retry-After` (seconds)
/// when present and sane, capped at 30 s; otherwise exponential backoff from
/// `backoff`, capped at 8 s, plus up to 250 ms of jitter so clients retrying
/// the same outage do not line up.
pub(in crate::agents) fn retry_delay(
    attempt: u32,
    backoff: Duration,
    retry_after: Option<Duration>,
) -> Duration {
    if let Some(d) = retry_after {
        return d.min(MAX_RETRY_AFTER);
    }
    let base = backoff
        .saturating_mul(2u32.saturating_pow(attempt.min(16)))
        .min(MAX_BACKOFF);
    // No `rand` dependency for 250 ms of spread: the clock's sub-millisecond
    // digits are as good as it needs to be.
    let jitter_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.subsec_micros() % 250) as u64)
        .unwrap_or(0);
    base + Duration::from_millis(jitter_ms)
}

/// `Retry-After` in seconds (decimals accepted, as the other SDKs do); an
/// HTTP date or a negative value is ignored and the backoff applies.
pub(super) fn parse_retry_after(resp: &reqwest::Response) -> Option<Duration> {
    resp.headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|secs| secs.is_finite() && *secs >= 0.0)
        .map(Duration::from_secs_f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_delay_honours_retry_after_up_to_its_cap() {
        let backoff = Duration::from_millis(500);
        assert_eq!(
            retry_delay(0, backoff, Some(Duration::from_secs(2))),
            Duration::from_secs(2)
        );
        assert_eq!(
            retry_delay(0, backoff, Some(Duration::from_secs(600))),
            MAX_RETRY_AFTER
        );
        // Exponential, capped at 8 s, jitter under 250 ms.
        let d1 = retry_delay(1, backoff, None);
        assert!(
            d1 >= Duration::from_secs(1) && d1 < Duration::from_millis(1250),
            "{d1:?}"
        );
        let d9 = retry_delay(9, backoff, None);
        assert!(d9 >= MAX_BACKOFF && d9 < MAX_BACKOFF + Duration::from_millis(250));
    }
}
