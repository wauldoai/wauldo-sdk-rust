//! History API client — Wauldo Funnel #1 audit log.
//!
//! Read-only access to a tenant's task history (every completed task is
//! persisted to a tenant-scoped DynamoDB audit log on the server side,
//! exposed via `/v1/history`). Mirrors [`MemoryClient`] shape so a caller
//! already familiar with the Memory API has zero ramp-up.
//!
//! Three formats:
//!
//! - [`HistoryClient::list`] — paginated JSON, suitable for dashboards.
//! - [`HistoryClient::export`] with `format = "csv"` — single CSV blob
//!   (compliance evidence, header + footer metadata).
//! - [`HistoryClient::export`] with `format = "jsonl"` — newline-
//!   delimited JSON for log pipelines.
//!
//! Right To Be Forgotten (GDPR Art. 17) is supported via
//! [`HistoryClient::delete_task`], which removes every audit row for a
//! specific task id within the caller's tenant.
//!
//! # Example
//! ```no_run
//! use wauldo::history::{HistoryClient, ListOptions};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let hist = HistoryClient::new("https://api.wauldo.com")
//!     .with_api_key("tig_live_...")
//!     .with_tenant("my-org");
//! let page = hist
//!     .list(ListOptions { verdict: Some("CONFLICT".into()), limit: Some(20), ..Default::default() })
//!     .await?;
//! for item in &page.items {
//!     println!("{} {}", item.task_id, item.verdict);
//! }
//! let csv: String = hist.export("csv", Default::default()).await?;
//! let deleted = hist.delete_task("a69b8612-0c47-43f3-93f2-c00c8a4ac1f8").await?;
//! println!("deleted {} rows", deleted);
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use reqwest::{header::HeaderMap, Client, Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::agents::{bounded_read, AgentsError, AgentsResult, MAX_RESPONSE_SIZE};

/// One audit log entry — same shape as the server's [`TaskHistoryEntry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHistoryEntry {
    pub task_id: String,
    pub tenant_id: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    pub verdict: String,
    pub support_score: f64,
    pub halluc_rate: f64,
    pub latency_ms: u64,
    pub cost_micro_usd: u64,
    pub claims_count: u32,
    #[serde(default)]
    pub model: Option<String>,
    pub created_at: u64,
}

/// Server response for `GET /v1/history`.
///
/// `enabled = false` signals the server hasn't wired its DynamoDB store
/// (self-host without IAM perm). `items` is empty + `enabled = true`
/// means the window simply has no events for this tenant yet.
#[derive(Debug, Clone, Deserialize)]
pub struct HistoryListResponse {
    pub items: Vec<TaskHistoryEntry>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    pub enabled: bool,
}

/// Filters for [`HistoryClient::list`] / [`HistoryClient::export`].
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub verdict: Option<String>,
    pub agent_id: Option<String>,
    pub from_ms: Option<u64>,
    pub to_ms: Option<u64>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

pub struct HistoryClient {
    base_url: String,
    api_key: Option<String>,
    tenant: Option<String>,
    client: Client,
}

impl HistoryClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: None,
            tenant: None,
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
        }
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    fn headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("Content-Type", "application/json".parse().unwrap());
        if let Some(key) = &self.api_key {
            if let Ok(val) = format!("Bearer {key}").parse() {
                h.insert("Authorization", val);
            }
        }
        if let Some(t) = &self.tenant {
            if let Ok(val) = t.parse() {
                h.insert("x-rapidapi-user", val);
            }
        }
        h
    }

    fn build_qs(opts: &ListOptions, format: Option<&str>) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(v) = &opts.verdict {
            parts.push(format!("verdict={}", urlencode(v)));
        }
        if let Some(v) = &opts.agent_id {
            parts.push(format!("agent_id={}", urlencode(v)));
        }
        if let Some(v) = opts.from_ms {
            parts.push(format!("from={v}"));
        }
        if let Some(v) = opts.to_ms {
            parts.push(format!("to={v}"));
        }
        if let Some(v) = opts.limit {
            parts.push(format!("limit={v}"));
        }
        if let Some(v) = &opts.cursor {
            parts.push(format!("cursor={}", urlencode(v)));
        }
        if let Some(v) = format {
            parts.push(format!("format={v}"));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!("?{}", parts.join("&"))
        }
    }

    /// `GET /v1/history` — paginated audit log page. Pass `cursor` from
    /// a previous response's `next_cursor` to paginate. Filters compose
    /// with AND.
    pub async fn list(&self, opts: ListOptions) -> AgentsResult<HistoryListResponse> {
        let path = format!("/v1/history{}", Self::build_qs(&opts, None));
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .request(Method::GET, &url)
            .headers(self.headers())
            .send()
            .await?;
        let status = resp.status();
        let bytes = bounded_read(resp, MAX_RESPONSE_SIZE).await?;
        if !status.is_success() {
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// `GET /v1/history?format=csv|jsonl` — single-blob export. Returns
    /// the body as a `String`. Server auto-paginates up to 10000 rows
    /// then signals truncation via the body footer (CSV `# wauldo-
    /// history-export` line / JSONL `_export` object) and the
    /// `x-wauldo-truncated` header. Rate-limited per tenant to 5 / 60s
    /// — a non-2xx response is surfaced as [`AgentsError::Status`]
    /// (HTTP 429 on cap).
    pub async fn export(&self, format: &str, opts: ListOptions) -> AgentsResult<String> {
        if !matches!(format, "csv" | "jsonl" | "json") {
            return Err(AgentsError::Status {
                status: 400,
                body: format!("unsupported format '{format}' — use csv|jsonl|json"),
            });
        }
        let path = format!("/v1/history{}", Self::build_qs(&opts, Some(format)));
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .request(Method::GET, &url)
            .headers(self.headers())
            .send()
            .await?;
        let status = resp.status();
        let bytes = bounded_read(resp, MAX_RESPONSE_SIZE).await?;
        if !status.is_success() {
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        Ok(String::from_utf8(bytes).unwrap_or_default())
    }

    /// `DELETE /v1/history/:task_id` — RTBF (GDPR Art. 17). Removes
    /// every audit row for `task_id` within the caller's tenant.
    /// Idempotent : deleting a non-existent task returns `0`.
    /// Returns the number of rows deleted.
    pub async fn delete_task(&self, task_id: &str) -> AgentsResult<u64> {
        if task_id.is_empty() {
            return Err(AgentsError::Status {
                status: 400,
                body: "task_id required".into(),
            });
        }
        let url = format!("{}/v1/history/{}", self.base_url, urlencode(task_id));
        let resp = self
            .client
            .request(Method::DELETE, &url)
            .headers(self.headers())
            .send()
            .await?;
        let status = resp.status();
        let bytes = bounded_read(resp, MAX_RESPONSE_SIZE).await?;
        if status == StatusCode::NO_CONTENT || bytes.is_empty() {
            return Ok(0);
        }
        if !status.is_success() {
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        Ok(body.get("deleted").and_then(|v| v.as_u64()).unwrap_or(0))
    }
}

/// Minimal RFC 3986 unreserved-only escaping. Sufficient for the
/// audit-log query params we accept (UUIDs, ISO-style strings, opaque
/// base64 cursors). Avoids pulling a percent-encoding crate just for
/// this one helper.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_qs_empty_when_no_filters() {
        let opts = ListOptions::default();
        assert_eq!(HistoryClient::build_qs(&opts, None), "");
    }

    #[test]
    fn build_qs_includes_format_and_filters() {
        let opts = ListOptions {
            verdict: Some("CONFLICT".into()),
            limit: Some(20),
            ..Default::default()
        };
        let qs = HistoryClient::build_qs(&opts, Some("csv"));
        assert!(qs.starts_with('?'));
        assert!(qs.contains("verdict=CONFLICT"));
        assert!(qs.contains("limit=20"));
        assert!(qs.contains("format=csv"));
    }

    #[test]
    fn urlencode_keeps_unreserved_escapes_others() {
        assert_eq!(urlencode("abc-123_._~"), "abc-123_._~");
        assert_eq!(urlencode("a b"), "a%20b");
        assert_eq!(urlencode("a+b/c"), "a%2Bb%2Fc");
    }
}
