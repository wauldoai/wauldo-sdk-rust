//! Memory API client — Wauldo Deploy long-term memory.
//!
//! Tenant-scoped key-value store with namespaces and lexical search.
//! Standalone like `AgentsClient` — no coupling to `HttpClient`.
//!
//! # Example
//! ```no_run
//! use wauldo::memory::{MemoryClient, SearchOptions};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mem = MemoryClient::new("http://localhost:3000").with_api_key("sk-...");
//! mem.set("support", "ticket-123", "Customer asked about pricing", &["urgent", "sales"], None)
//!     .await?;
//! let hits = mem.search("support", SearchOptions { query: Some("pricing".into()), tags: vec!["urgent".into()], limit: None }).await?;
//! println!("{} hits", hits.results.len());
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use reqwest::{header::HeaderMap, Client, Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::agents::{bounded_read, AgentsError, AgentsResult, MAX_RESPONSE_SIZE};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub id: String,
    pub tenant_id: String,
    pub namespace: String,
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryListResponse {
    pub entries: Vec<MemoryEntry>,
    pub pagination: MemoryPagination,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryPagination {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemorySearchResponse {
    pub results: Vec<MemorySearchResult>,
    pub total_matched: usize,
    pub mode: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemorySearchResult {
    pub entry: MemoryEntry,
    pub score: f32,
    pub matched_fields: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    pub query: Option<String>,
    pub tags: Vec<String>,
    pub limit: Option<usize>,
}

pub struct MemoryClient {
    base_url: String,
    api_key: Option<String>,
    tenant: Option<String>,
    client: Client,
}

impl MemoryClient {
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

    async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> AgentsResult<Option<T>> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.client.request(method, &url).headers(self.headers());
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let bytes = bounded_read(resp, MAX_RESPONSE_SIZE).await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body,
            });
        }
        if bytes.is_empty() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    // ── CRUD ─────────────────────────────────────────────────────

    pub async fn set(
        &self,
        namespace: &str,
        key: &str,
        value: &str,
        tags: &[&str],
        embedding: Option<Vec<f32>>,
    ) -> AgentsResult<MemoryEntry> {
        let mut body = serde_json::json!({ "key": key, "value": value });
        if !tags.is_empty() {
            body["tags"] = serde_json::json!(tags);
        }
        if let Some(emb) = embedding {
            body["embedding"] = serde_json::json!(emb);
        }
        self.request::<MemoryEntry>(
            Method::POST,
            &format!("/v1/memory/{namespace}"),
            Some(&body),
        )
        .await
        .map(|o| o.expect("server returned empty body for memory.set"))
    }

    pub async fn get(&self, namespace: &str, key: &str) -> AgentsResult<MemoryEntry> {
        self.request::<MemoryEntry>(Method::GET, &format!("/v1/memory/{namespace}/{key}"), None)
            .await
            .map(|o| o.expect("server returned empty body for memory.get"))
    }

    pub async fn delete(&self, namespace: &str, key: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/memory/{namespace}/{key}"),
                None,
            )
            .await?;
        Ok(())
    }

    pub async fn list(
        &self,
        namespace: &str,
        limit: usize,
        offset: usize,
    ) -> AgentsResult<MemoryListResponse> {
        self.request::<MemoryListResponse>(
            Method::GET,
            &format!("/v1/memory/{namespace}?limit={limit}&offset={offset}"),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for memory.list"))
    }

    pub async fn search(
        &self,
        namespace: &str,
        options: SearchOptions,
    ) -> AgentsResult<MemorySearchResponse> {
        let query = options.query.unwrap_or_default();
        if query.is_empty() && options.tags.is_empty() {
            return Err(AgentsError::InvalidInput(
                "search requires query or tags (or both)".into(),
            ));
        }
        let mut body = serde_json::json!({ "query": query });
        if !options.tags.is_empty() {
            body["tags"] = serde_json::json!(options.tags);
        }
        if let Some(limit) = options.limit {
            body["limit"] = serde_json::json!(limit);
        }
        self.request::<MemorySearchResponse>(
            Method::POST,
            &format!("/v1/memory/{namespace}/search"),
            Some(&body),
        )
        .await
        .map(|o| o.expect("server returned empty body for memory.search"))
    }

    // ── Namespace sugar ──────────────────────────────────────────
    //
    // Bound views so callers can write `client.short_term().set(...)`
    // instead of `client.set("short_term", ...)`. Pure sugar — the
    // base CRUD methods above remain unchanged.

    /// Sugar for namespace `short_term` (session/transient state).
    pub fn short_term(&self) -> NamespacedMemory<'_> {
        NamespacedMemory {
            client: self,
            namespace: "short_term",
        }
    }

    /// Sugar for namespace `long_term` (durable user/agent facts).
    pub fn long_term(&self) -> NamespacedMemory<'_> {
        NamespacedMemory {
            client: self,
            namespace: "long_term",
        }
    }

    /// Sugar for namespace `entity` (per-entity profiles/state).
    pub fn entity(&self) -> NamespacedMemory<'_> {
        NamespacedMemory {
            client: self,
            namespace: "entity",
        }
    }

    /// Sugar for namespace `contextual` (per-context attachments).
    pub fn contextual(&self) -> NamespacedMemory<'_> {
        NamespacedMemory {
            client: self,
            namespace: "contextual",
        }
    }
}

/// Namespace-bound view over a [`MemoryClient`].
///
/// Returned by [`MemoryClient::short_term`], [`MemoryClient::long_term`],
/// [`MemoryClient::entity`], [`MemoryClient::contextual`]. Every method
/// forwards to the parent client with the namespace prefilled.
pub struct NamespacedMemory<'a> {
    client: &'a MemoryClient,
    pub namespace: &'static str,
}

impl<'a> NamespacedMemory<'a> {
    pub async fn set(
        &self,
        key: &str,
        value: &str,
        tags: &[&str],
        embedding: Option<Vec<f32>>,
    ) -> AgentsResult<MemoryEntry> {
        self.client
            .set(self.namespace, key, value, tags, embedding)
            .await
    }

    pub async fn get(&self, key: &str) -> AgentsResult<MemoryEntry> {
        self.client.get(self.namespace, key).await
    }

    pub async fn delete(&self, key: &str) -> AgentsResult<()> {
        self.client.delete(self.namespace, key).await
    }

    pub async fn list(&self, limit: usize, offset: usize) -> AgentsResult<MemoryListResponse> {
        self.client.list(self.namespace, limit, offset).await
    }

    pub async fn search(&self, options: SearchOptions) -> AgentsResult<MemorySearchResponse> {
        self.client.search(self.namespace, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_entry_json() -> serde_json::Value {
        serde_json::json!({
            "id": "m1",
            "tenant_id": "t",
            "namespace": "support",
            "key": "k1",
            "value": "hello",
            "tags": [],
            "created_at": 0u64,
            "updated_at": 0u64,
        })
    }

    #[tokio::test]
    async fn test_set_basic_posts_key_and_value() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/memory/support"))
            .and(body_partial_json(serde_json::json!({
                "key": "k1",
                "value": "hello",
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_entry_json()))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri());
        let out = client
            .set("support", "k1", "hello", &[], None)
            .await
            .unwrap();
        assert_eq!(out.id, "m1");
    }

    #[tokio::test]
    async fn test_set_with_tags_and_embedding() {
        use wiremock::Request;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/memory/ns"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_entry_json()))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri());
        client
            .set("ns", "k", "v", &["urgent"], Some(vec![0.1, 0.2]))
            .await
            .unwrap();

        // Verify via inspecting the captured request — body_partial_json is
        // fussy with mixed f32/f64 literals, so we parse the body ourselves.
        let requests = server.received_requests().await.unwrap();
        let req: &Request = &requests[0];
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(body["key"], "k");
        assert_eq!(body["value"], "v");
        assert_eq!(body["tags"], serde_json::json!(["urgent"]));
        assert!(body["embedding"].is_array());
        let emb = body["embedding"].as_array().unwrap();
        assert_eq!(emb.len(), 2);
    }

    #[tokio::test]
    async fn test_get() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/memory/ns/k"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_entry_json()))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri());
        let out = client.get("ns", "k").await.unwrap();
        assert_eq!(out.value, "hello");
    }

    #[tokio::test]
    async fn test_delete_returns_unit() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/memory/ns/k"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri());
        client.delete("ns", "k").await.unwrap();
    }

    #[tokio::test]
    async fn test_list_returns_paginated_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/memory/ns"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "entries": [],
                "pagination": { "total": 0, "limit": 20, "offset": 0 },
            })))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri());
        let out = client.list("ns", 20, 0).await.unwrap();
        assert_eq!(out.pagination.limit, 20);
    }

    #[tokio::test]
    async fn test_search_query_only_sends_query() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/memory/ns/search"))
            .and(body_partial_json(serde_json::json!({"query": "hello"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "results": [],
                "total_matched": 0,
                "mode": "lexical",
            })))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri());
        client
            .search(
                "ns",
                SearchOptions {
                    query: Some("hello".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_search_rejects_empty_query_and_tags() {
        let client = MemoryClient::new("http://localhost:1");
        let err = client
            .search("ns", SearchOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, AgentsError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn test_tenant_header_injected() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/memory/ns"))
            .and(header("authorization", "Bearer k"))
            .and(header("x-rapidapi-user", "tenant-x"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_entry_json()))
            .mount(&server)
            .await;
        let client = MemoryClient::new(server.uri())
            .with_api_key("k")
            .with_tenant("tenant-x");
        client.set("ns", "k", "v", &[], None).await.unwrap();
    }
}
