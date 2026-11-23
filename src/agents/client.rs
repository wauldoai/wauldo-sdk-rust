//! `AgentsClient` — HTTP client for the `/v1/agents` and `/v1/tasks` endpoints.
//!
//! This file holds the client itself, its transport and the agent CRUD and
//! revision calls; the retry policy is in `retry`, and the run and task calls
//! continue this impl in the sibling `runs` and `tasks` modules.

use std::time::Duration;

use reqwest::{header::HeaderMap, Client, Method, StatusCode};
use serde::{Deserialize, Serialize};

use super::types::*;

// ─── Client ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AgentsClient {
    pub(super) base_url: String,
    api_key: Option<String>,
    tenant: Option<String>,
    pub(super) client: Client,
    /// For `stream_task` alone: a connect timeout but no total timeout (a
    /// stream lives up to 30 minutes); `stream_task` bounds each read instead.
    pub(super) stream_client: Client,
    /// Longest `stream_task` waits for the stream's headers, then for each
    /// read: [`super::STREAM_IDLE_TIMEOUT`] (a field so the tests can shorten it).
    pub(super) stream_idle_timeout: Duration,
    max_retries: u32,
    pub(super) retry_backoff: Duration,
}

/// Statuses retried whatever the method: the server did not take the request.
pub const RETRY_STATUSES: [u16; 2] = [429, 503];
/// Statuses retried only on a read: the server may have acted before failing,
/// a replayed `POST` would run the agent twice and a replayed cancel answers
/// 409 for a cancel that worked.
pub const RETRY_STATUSES_IDEMPOTENT: [u16; 3] = [500, 502, 504];

mod retry;

pub(super) use retry::retry_delay;
use retry::{
    is_idempotent, parse_retry_after, should_retry, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BACKOFF,
};

impl AgentsClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: None,
            tenant: None,
            client: Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .expect("reqwest client"),
            stream_client: Client::builder()
                .connect_timeout(super::stream::STREAM_CONNECT_TIMEOUT)
                .build()
                .expect("reqwest client"),
            stream_idle_timeout: super::stream::STREAM_IDLE_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_backoff: DEFAULT_RETRY_BACKOFF,
        }
    }

    /// Attempts after the first on a 429 or 503 (any method) and on a
    /// 500/502/504 or a connection failure (`GET` only — a replayed `POST`
    /// could run an agent twice, a replayed cancel answers 409 for a cancel
    /// that worked). Default 3; 0 disables.
    /// `Retry-After` is honoured; otherwise the wait doubles from the backoff
    /// ([`Self::with_retry_backoff`], 500 ms by default). Timeouts are never
    /// retried.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// First wait between two attempts (doubles per attempt, capped at 8 s).
    pub fn with_retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    pub(super) fn headers(&self, extra: Option<HeaderMap>) -> HeaderMap {
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
        if let Some(extra_map) = extra {
            for (k, v) in extra_map.iter() {
                h.insert(k.clone(), v.clone());
            }
        }
        h
    }

    pub(super) async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
        extra_headers: Option<HeaderMap>,
    ) -> AgentsResult<Option<T>> {
        let url = format!("{}{}", self.base_url, path);
        let mut attempt = 0u32;
        loop {
            let mut req = self
                .client
                .request(method.clone(), &url)
                .headers(self.headers(extra_headers.clone()));
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = match req.send().await {
                Ok(resp) => resp,
                // The request did not get a response (refused, reset): replayed
                // only when replaying is safe. A timeout is never retried — on
                // a long poll it would double the wait the caller chose.
                Err(e)
                    if attempt < self.max_retries && is_idempotent(&method) && e.is_connect() =>
                {
                    tokio::time::sleep(retry_delay(attempt, self.retry_backoff, None)).await;
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            let status = resp.status();
            if !status.is_success() {
                if attempt < self.max_retries && should_retry(&method, status) {
                    let retry_after = parse_retry_after(&resp);
                    tokio::time::sleep(retry_delay(attempt, self.retry_backoff, retry_after)).await;
                    attempt += 1;
                    continue;
                }
                let body_str = resp.text().await.unwrap_or_default();
                return Err(AgentsError::Status {
                    status: status.as_u16(),
                    body: body_str,
                });
            }
            if status == StatusCode::NO_CONTENT {
                return Ok(None);
            }
            let bytes = resp.bytes().await?;
            if bytes.is_empty() {
                return Ok(None);
            }
            return Ok(Some(serde_json::from_slice(&bytes)?));
        }
    }

    // ── CRUD ─────────────────────────────────────────────────────

    pub async fn create(&self, req: CreateAgentRequest) -> AgentsResult<DeployedAgent> {
        self.request::<DeployedAgent>(Method::POST, "/v1/agents", Some(&req), None)
            .await
            .map(|o| o.expect("server returned empty body for create"))
    }

    pub async fn list(&self, limit: usize, offset: usize) -> AgentsResult<AgentListResponse> {
        let path = format!("/v1/agents?limit={limit}&offset={offset}");
        self.request::<AgentListResponse>(Method::GET, &path, Option::<&()>::None, None)
            .await
            .map(|o| o.expect("server returned empty body for list"))
    }

    pub async fn get(&self, agent_id: &str) -> AgentsResult<DeployedAgent> {
        self.request::<DeployedAgent>(
            Method::GET,
            &format!("/v1/agents/{agent_id}"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for get"))
    }

    pub async fn update(
        &self,
        agent_id: &str,
        patch: UpdateAgentRequest,
    ) -> AgentsResult<DeployedAgent> {
        self.request::<DeployedAgent>(
            Method::PATCH,
            &format!("/v1/agents/{agent_id}"),
            Some(&patch),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for update"))
    }

    pub async fn delete(&self, agent_id: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/agents/{agent_id}"),
                Option::<&()>::None,
                None,
            )
            .await?;
        Ok(())
    }

    // ── Revisions (ECS-style versioning) ─────────────────────────

    /// `POST /v1/agents/:id/revisions` — mint an immutable revision.
    ///
    /// The server validates `custom_preset` (size, depth, states, cycle,
    /// tools, quota) and stores an immutable snapshot keyed by SHA-256.
    /// When `set_active` is `true` (default) the new revision becomes
    /// the agent's live revision; `false` stages it for review.
    pub async fn create_revision(
        &self,
        agent_id: &str,
        req: CreateRevisionRequest,
    ) -> AgentsResult<CreateRevisionResponse> {
        self.request::<CreateRevisionResponse>(
            Method::POST,
            &format!("/v1/agents/{agent_id}/revisions"),
            Some(&req),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for create_revision"))
    }

    /// `GET /v1/agents/:id/revisions` — list revisions newest-first.
    pub async fn list_revisions(&self, agent_id: &str) -> AgentsResult<ListRevisionsResponse> {
        self.request::<ListRevisionsResponse>(
            Method::GET,
            &format!("/v1/agents/{agent_id}/revisions"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for list_revisions"))
    }

    /// `GET /v1/agents/:id/revisions/:rev` — fetch one revision verbatim.
    pub async fn get_revision(&self, agent_id: &str, rev: u32) -> AgentsResult<AgentRevision> {
        self.request::<AgentRevision>(
            Method::GET,
            &format!("/v1/agents/{agent_id}/revisions/{rev}"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for get_revision"))
    }

    /// `PATCH /v1/agents/:id/active-revision` — O(1) rollback / promotion.
    ///
    /// No LLM cost — the revision is already validated and stored. Use
    /// this to roll back to a previous good revision when the current
    /// one breaks in production.
    pub async fn set_active_revision(
        &self,
        agent_id: &str,
        rev: u32,
    ) -> AgentsResult<DeployedAgent> {
        let body = serde_json::json!({ "rev": rev });
        self.request::<DeployedAgent>(
            Method::PATCH,
            &format!("/v1/agents/{agent_id}/active-revision"),
            Some(&body),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for set_active_revision"))
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn stub_agent_server() -> MockServer {
        MockServer::start().await
    }

    fn sample_agent_json() -> serde_json::Value {
        serde_json::json!({
            "id": "a1",
            "tenant_id": "t",
            "name": "bot",
            "description": "",
            "wauldo_toml": "[agent]\n[model]",
            "model_provider": "openrouter",
            "model_name": "qwen",
            "created_at": 0u64,
            "updated_at": 0u64,
        })
    }

    #[tokio::test]
    async fn test_create_sends_full_body_and_headers() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents"))
            .and(header("authorization", "Bearer k"))
            .and(header("x-rapidapi-user", "t"))
            .and(body_partial_json(serde_json::json!({
                "name": "bot",
                "preset": "general_task",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(sample_agent_json()))
            .mount(&server)
            .await;

        let client = AgentsClient::new(server.uri())
            .with_api_key("k")
            .with_tenant("t");
        let out = client
            .create(CreateAgentRequest {
                name: "bot".into(),
                wauldo_toml: "[agent]\n[model]".into(),
                preset: Some("general_task".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(out.id, "a1");
    }

    #[tokio::test]
    async fn test_list_builds_query_string() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "agents": [],
                "pagination": { "total": 0, "limit": 10, "offset": 5 },
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client.list(10, 5).await.unwrap();
        assert_eq!(out.pagination.limit, 10);
        assert_eq!(out.pagination.offset, 5);
    }

    #[tokio::test]
    async fn test_get_round_trip() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_agent_json()))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client.get("abc").await.unwrap();
        assert_eq!(out.id, "a1");
    }

    #[tokio::test]
    async fn test_delete_returns_unit_on_204() {
        let server = stub_agent_server().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/agents/xyz"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.delete("xyz").await.unwrap();
    }

    #[tokio::test]
    async fn test_run_forwards_verification_mode() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(body_partial_json(serde_json::json!({
                "input": "Hello",
                "verification_mode": "strict",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client.run("bot", "Hello", Some("strict")).await.unwrap();
        assert_eq!(out.task_id, "tk1");
    }

    #[tokio::test]
    async fn test_run_rejects_empty_input() {
        let client = AgentsClient::new("http://localhost:1");
        let err = client.run("bot", "", None).await.unwrap_err();
        assert!(matches!(err, AgentsError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn test_run_forwards_fact_check_mode() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(body_partial_json(serde_json::json!({
                "input": "Hello",
                "fact_check_mode": "hybrid",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client
            .run_with_fact_check("bot", "Hello", None, Some("hybrid"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_run_omits_fact_check_mode_when_none() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.run("bot", "Hello", None).await.unwrap();
        let received = &server.received_requests().await.unwrap()[0];
        let body: serde_json::Value = serde_json::from_slice(&received.body).unwrap();
        assert!(
            body.get("fact_check_mode").is_none(),
            "fact_check_mode should be omitted when None, got: {}",
            body
        );
    }

    #[tokio::test]
    async fn test_a2a_invoke_sends_trace_header() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/a2a/target"))
            .and(header("x-a2a-trace", "caller"))
            .and(body_partial_json(serde_json::json!({"input": "do"})))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk",
                "agent_id": "target",
                "trace": ["caller", "target"],
                "depth": 2u64,
                "status": "queued",
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client
            .a2a_invoke("target", "do", &["caller"])
            .await
            .unwrap();
        assert_eq!(out.depth, 2);
        assert_eq!(out.trace, vec!["caller", "target"]);
    }

    #[tokio::test]
    async fn test_a2a_rejects_empty_input() {
        let client = AgentsClient::new("http://localhost:1");
        let err = client.a2a_invoke("target", "", &[]).await.unwrap_err();
        assert!(matches!(err, AgentsError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn test_non_2xx_becomes_status_error() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_string("{\"error\":\"not found\"}"))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let err = client.get("missing").await.unwrap_err();
        match err {
            AgentsError::Status { status, body } => {
                assert_eq!(status, 404);
                assert!(body.contains("not found"));
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    // ── Tasks + SSE tests ────────────────────────────────────────

    fn sample_task_json(status: &str) -> serde_json::Value {
        serde_json::json!({
            "task_id": "t1",
            "tenant_id": "tn",
            "status": status,
            "prompt": "hi",
            "created_at": 1u64,
            "updated_at": 2u64,
            "result": "hello",
            "verification": {
                "verdict": "UNVERIFIED",
                "hallucination_rate": 0.0,
                "confidence": 1.0,
                "trust_score": 0.0,
                "verification_source": "prompt_only",
                "claims": [],
                "verification_retries": 0u32,
                "message": "No source documents uploaded.",
            }
        })
    }

    #[tokio::test]
    async fn test_task_status_is_terminal() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
        assert!(!TaskStatus::Queued.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
    }

    #[tokio::test]
    async fn test_get_task_parses_typed_task() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_task_json("completed")))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri()).with_api_key("k");
        let task = client.get_task("t1").await.unwrap();
        assert_eq!(task.task_id, "t1");
        assert_eq!(task.status, TaskStatus::Completed);
        assert!(task.is_done());
        let verif = task.verification.as_ref().expect("verif present");
        assert_eq!(verif.verdict, crate::agents::Verdict::Unverified);
        assert_eq!(verif.trust_score, 0.0);
        assert_eq!(
            verif.message.as_deref(),
            Some("No source documents uploaded.")
        );
    }

    /// The fields `GET /v1/tasks/:id` sends beyond the base record: the
    /// public share and the outcome of a fact-check race.
    #[tokio::test]
    async fn test_get_task_reads_share_and_race_result() {
        let server = stub_agent_server().await;
        let mut body = sample_task_json("completed");
        body["share_id"] = serde_json::json!("r_x");
        body["share_url"] = serde_json::json!("https://wauldo.com/r/r_x");
        body["race_result"] = serde_json::json!({
            "winner": "model-a",
            "winner_score": 0.9,
            "candidates": [
                {"model": "model-a", "score": 0.9, "timed_out": false},
                {"model": "model-b", "score": null, "timed_out": true}
            ]
        });
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        let task = AgentsClient::new(server.uri())
            .get_task("t1")
            .await
            .expect("task");
        assert_eq!(task.share_id.as_deref(), Some("r_x"));
        assert_eq!(task.share_url.as_deref(), Some("https://wauldo.com/r/r_x"));
        let race = task.race_result.expect("race result");
        assert_eq!(race.winner, "model-a");
        assert!(race.candidates[1].timed_out);
        assert_eq!(race.candidates[1].score, None);
    }

    #[tokio::test]
    async fn test_cancel_task_sends_put_and_returns_the_task() {
        let server = stub_agent_server().await;
        Mock::given(method("PUT"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task_json("cancelled", None)))
            .expect(1)
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let task = client.cancel_task("t1").await.unwrap();
        assert_eq!(task.status, TaskStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_delete_task_sends_delete() {
        let server = stub_agent_server().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.delete_task("t1").await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_task_returns_on_terminal() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_task_json("completed")))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let task = client
            .wait_for_task("t1", Duration::from_secs(5), Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_stream_task_parses_sse_frames() {
        use futures_util::StreamExt;
        let events = [
            serde_json::json!({
                "state_name": "Analysis",
                "to_state": "Tradeoffs",
                "condition": "Sequential execution",
                "raw_output": "",
                "validation_notes": [],
                "timestamp": 1u64,
                "success": true,
                "retry_count": 0u32,
                "duration_ms": 1000u64,
                "prompt_tokens": 10usize,
                "completion_tokens": 200usize,
                "repair_count": 0u32,
                "cache_hit": false,
            }),
            serde_json::json!({
                "state_name": "Tradeoffs",
                "to_state": null,
                "condition": "Sequential execution",
                "raw_output": "",
                "validation_notes": [],
                "timestamp": 2u64,
                "success": true,
                "retry_count": 0u32,
                "duration_ms": 2000u64,
                "prompt_tokens": 10usize,
                "completion_tokens": 300usize,
                "repair_count": 0u32,
                "cache_hit": false,
            }),
        ];
        let body = events
            .iter()
            .map(|e| format!("data: {}\n\n", e))
            .collect::<String>()
            + ": keep-alive\n\n";

        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1/stream"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task_json("completed", None)))
            .mount(&server)
            .await;

        let client = AgentsClient::new(server.uri());
        let stream = client.stream_task("t1").await.unwrap();
        futures_util::pin_mut!(stream);
        let mut got = Vec::new();
        while let Some(ev) = stream.next().await {
            got.push(ev.unwrap());
        }
        // The stream closed without its terminal event: the last one is
        // built from the task (see `stream::tests`).
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].state_name, "Analysis");
        assert_eq!(got[1].state_name, "Tradeoffs");
        assert_eq!(got[2].state_name, "TASK_COMPLETED");
        assert_eq!(got[0].duration_ms, 1000);
    }

    #[tokio::test]
    async fn test_stream_task_surfaces_http_errors() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1/stream"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        match client.stream_task("t1").await {
            Err(AgentsError::Status { status, .. }) => assert_eq!(status, 500),
            Ok(_) => panic!("expected Err, got Ok(stream)"),
            Err(other) => panic!("expected Status, got {other:?}"),
        }
    }

    // ── Wait / one-call result (0.18.0) ──────────────────────────────

    fn task_json(status: &str, result: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "task_id": "t1",
            "tenant_id": "tn",
            "status": status,
            "prompt": "",
            "created_at": 0u64,
            "updated_at": 0u64,
            "result": result,
        })
    }

    #[tokio::test]
    async fn test_run_with_options_adds_wait_query_and_parses_task() {
        use wiremock::matchers::query_param;
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(query_param("wait", "30"))
            .and(body_partial_json(
                serde_json::json!({ "verification_mode": "strict" }),
            ))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "t1",
                "agent_id": "bot",
                "status": "completed",
                "created_at": 0u64,
                "task": task_json("completed", Some("42")),
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client
            .run_with_options(
                "bot",
                "Hello",
                RunOptions {
                    verification_mode: Some("strict".into()),
                    wait: Some(Duration::from_secs(30)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(out.status, "completed");
        let task = out.task.expect("task carried by the response");
        assert_eq!(task.result.as_deref(), Some("42"));
        assert!(task.is_settled());
    }

    /// The run options reach the wire under the server's names, and
    /// the share link comes back.
    #[tokio::test]
    async fn test_run_with_options_sends_webhook_retries_and_auto_share() {
        use wiremock::matchers::body_json;
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(body_json(serde_json::json!({
                "input": "Hello",
                "webhook_url": "https://example.com/hook",
                "max_retries": 5,
                "auto_share": true,
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "t1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
                "share_id": "r_x",
                "share_url": "https://wauldo.com/r/r_x",
            })))
            .expect(1)
            .mount(&server)
            .await;
        let out = AgentsClient::new(server.uri())
            .run_with_options(
                "bot",
                "Hello",
                RunOptions {
                    webhook_url: Some("https://example.com/hook".into()),
                    max_retries: Some(5),
                    auto_share: Some(true),
                    ..Default::default()
                },
            )
            .await
            .expect("run");
        assert_eq!(out.share_id.as_deref(), Some("r_x"));
        assert_eq!(out.share_url.as_deref(), Some("https://wauldo.com/r/r_x"));
    }

    /// `custom_preset` is sent at creation, and left out when unset.
    #[tokio::test]
    async fn test_create_sends_custom_preset_only_when_set() {
        let server = stub_agent_server().await;
        let contract = serde_json::json!({ "name": "flow", "states": [] });
        Mock::given(method("POST"))
            .and(path("/v1/agents"))
            .and(body_partial_json(
                serde_json::json!({ "custom_preset": contract }),
            ))
            .respond_with(ResponseTemplate::new(201).set_body_json(sample_agent_json()))
            .expect(1)
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client
            .create(CreateAgentRequest {
                name: "bot".into(),
                wauldo_toml: "[agent]".into(),
                custom_preset: Some(contract.clone()),
                ..Default::default()
            })
            .await
            .expect("create with custom_preset");
        let plain = serde_json::to_value(CreateAgentRequest::default()).expect("serialize");
        assert!(plain.get("custom_preset").is_none(), "{plain}");
    }

    /// The suggested wait is made of several long polls; it covers ordinary
    /// runs, not every run (see the timeout test for how to keep waiting).
    #[test]
    fn test_default_wait_timeout_spans_several_long_polls() {
        assert!(Duration::from_secs(LONG_POLL_SECS) < DEFAULT_WAIT_TIMEOUT);
    }
    #[tokio::test]
    async fn test_get_task_with_include_journal_false_reaches_the_server() {
        use wiremock::matchers::query_param;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .and(query_param("include_journal", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task_json("completed", None)))
            .expect(1)
            .mount(&server)
            .await;
        let task = AgentsClient::new(server.uri())
            .get_task_with("t1", None, Some(false))
            .await
            .expect("lean read");
        assert_eq!(task.status, TaskStatus::Completed);
    }

    /// The wait gives up while the run goes on: the error names the task and
    /// says how to keep waiting, and doing so returns the run's result.
    #[tokio::test]
    async fn test_wait_timeout_says_the_task_may_still_run_and_waiting_again_gets_it() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task_json("running", None)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task_json("completed", None)))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let err = client
            .wait_for_task("t1", Duration::ZERO, Duration::from_millis(1))
            .await
            .expect_err("the wait runs out");
        let message = err.to_string();
        assert!(
            message.contains("may still be running on the server"),
            "{message}"
        );
        assert!(message.contains("wait_for_task(\"t1\""), "{message}");
        let AgentsError::Timeout { task_id, .. } = err else {
            panic!("expected AgentsError::Timeout");
        };
        let task = client
            .wait_for_task(&task_id, Duration::from_secs(5), Duration::from_millis(1))
            .await
            .expect("waiting again gets the result");
        assert_eq!(task.status, TaskStatus::Completed);
        let hits = server.received_requests().await.expect("recorded");
        assert!(hits
            .iter()
            .all(|r| !r.url.query().unwrap_or("").contains("include_journal")));
    }

    #[test]
    fn test_wait_query_is_clamped_and_absent_without_a_wait() {
        assert_eq!(wait_query(None), "");
        assert_eq!(wait_query(Some(Duration::ZERO)), "");
        assert_eq!(wait_query(Some(Duration::from_secs(7))), "?wait=7");
        assert_eq!(
            wait_query(Some(Duration::from_secs(3600))),
            format!("?wait={MAX_WAIT_SECS}")
        );
    }

    #[tokio::test]
    async fn test_wait_for_task_long_polls_and_returns_a_paused_task() {
        use wiremock::matchers::query_param;
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .and(query_param("wait", "10"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(task_json("paused_for_approval", None)),
            )
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let task = client
            .wait_for_task("t1", Duration::from_secs(10), Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(task.status, TaskStatus::PausedForApproval);
        assert!(task.is_settled() && !task.is_done());
    }

    #[tokio::test]
    async fn test_run_and_wait_returns_the_task_in_one_round_trip() {
        use wiremock::matchers::query_param;
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(query_param("wait", "50"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "t1",
                "agent_id": "bot",
                "status": "completed",
                "created_at": 0u64,
                "task": task_json("completed", Some("42")),
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let task = client
            .run_and_wait(
                "bot",
                "Hello",
                RunOptions::default(),
                Duration::from_secs(120),
            )
            .await
            .unwrap();
        assert_eq!(task.result.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn test_run_and_wait_falls_back_to_polling() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "t1",
                "agent_id": "bot",
                "status": "running",
                "created_at": 0u64,
                "task": task_json("running", None),
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(task_json("completed", Some("late"))),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let task = client
            .run_and_wait(
                "bot",
                "Hello",
                RunOptions::default(),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert_eq!(task.result.as_deref(), Some("late"));
    }

    /// At its deadline, `wait_for_task` reads the status once, without asking
    /// the server to hold the request, then reports the timeout.
    #[tokio::test]
    async fn test_wait_for_task_past_its_deadline_reads_once_without_holding() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task_json("running", None)))
            .expect(1)
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let err = client
            .wait_for_task("t1", Duration::ZERO, Duration::from_millis(1))
            .await
            .unwrap_err();
        // A dedicated variant, carrying the task and its last status
        // — it used to be `InvalidInput`, the error of a malformed call.
        match &err {
            AgentsError::Timeout {
                task_id,
                last_status,
                waited,
            } => {
                assert_eq!(task_id, "t1");
                assert_eq!(*last_status, TaskStatus::Running);
                assert_eq!(*waited, Duration::ZERO);
            }
            other => panic!("expected AgentsError::Timeout, got {other:?}"),
        }
        assert!(err
            .to_string()
            .starts_with("task t1 still in status Running after 0ns"));
        let hit = server.received_requests().await.expect("recorded");
        assert_eq!(hit.len(), 1);
        assert!(
            hit[0].url.query().is_none(),
            "no ?wait= past the deadline: {:?}",
            hit[0].url
        );
    }

    // ── Retries ──────────────────────────────────────────────

    fn quick_client(server: &MockServer) -> AgentsClient {
        AgentsClient::new(server.uri()).with_retry_backoff(Duration::from_millis(1))
    }

    /// Two 503s then a 200: the caller sees the 200, the server saw three
    /// requests. Set `max_retries` to 0 in `quick_client` and this goes red.
    #[tokio::test]
    async fn test_a_503_is_retried_until_the_server_answers() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/abc"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_agent_json()))
            .mount(&server)
            .await;
        let out = quick_client(&server).get("abc").await.unwrap();
        assert_eq!(out.id, "a1");
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    /// A 500 on a `POST` is not replayed: the run may already have started.
    #[tokio::test]
    async fn test_a_500_on_post_is_not_retried() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let err = quick_client(&server)
            .run("bot", "Hello", None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, AgentsError::Status { status: 500, .. }),
            "{err}"
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    /// A 429 is retried on any method — the server did not take the request
    /// — and `Retry-After` sets the wait.
    #[tokio::test]
    async fn test_a_429_on_post_is_retried() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let out = quick_client(&server)
            .run("bot", "Hello", None)
            .await
            .unwrap();
        assert_eq!(out.task_id, "tk1");
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_max_retries_zero_fails_on_the_first_503() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/abc"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let err = quick_client(&server)
            .with_max_retries(0)
            .get("abc")
            .await
            .unwrap_err();
        assert!(matches!(err, AgentsError::Status { status: 503, .. }));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
    // ── Terminal event ────────────────────────────────────────────────

    #[test]
    fn test_terminal_payload_is_read_off_the_last_event() {
        let ev: StateTransition = serde_json::from_value(serde_json::json!({
            "state_name": "TASK_COMPLETED",
            "to_state": null,
            "condition": "terminal",
            "raw_output": "{\"task_id\":\"t1\",\"status\":\"completed\",\"result\":\"30 days\",\"error\":null,\"verification\":null}",
            "timestamp": 0u64,
            "success": true,
        }))
        .unwrap();
        assert!(ev.is_terminal());
        let payload = ev.terminal_payload().expect("payload");
        assert_eq!(payload.task_id, "t1");
        assert_eq!(payload.status, TaskStatus::Completed);
        assert_eq!(payload.result.as_deref(), Some("30 days"));

        let ordinary: StateTransition = serde_json::from_value(serde_json::json!({
            "state_name": "Analysis",
            "to_state": "Answer",
            "condition": "ok",
            "raw_output": "{\"task_id\":\"t1\"}",
            "timestamp": 0u64,
            "success": true,
        }))
        .unwrap();
        assert!(!ordinary.is_terminal());
        assert!(ordinary.terminal_payload().is_none());
    }
}
