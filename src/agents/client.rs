//! `AgentsClient` — HTTP client for the `/v1/agents` and `/v1/tasks` endpoints.

use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{header::HeaderMap, Client, Method, StatusCode};
use serde::{Deserialize, Serialize};

use super::types::*;

// ─── Client ───────────────────────────────────────────────────────────

pub struct AgentsClient {
    base_url: String,
    api_key: Option<String>,
    tenant: Option<String>,
    client: Client,
}

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

    fn headers(&self, extra: Option<HeaderMap>) -> HeaderMap {
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

    async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
        extra_headers: Option<HeaderMap>,
    ) -> AgentsResult<Option<T>> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .client
            .request(method, &url)
            .headers(self.headers(extra_headers));
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
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
        Ok(Some(serde_json::from_slice(&bytes)?))
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

    // ── Runs ─────────────────────────────────────────────────────

    pub async fn run(
        &self,
        agent_id: &str,
        input: &str,
        verification_mode: Option<&str>,
    ) -> AgentsResult<AgentRunResponse> {
        self.run_with_fact_check(agent_id, input, verification_mode, None)
            .await
    }

    /// `POST /v1/agents/:id/runs` with explicit fact-checker mode.
    ///
    /// `fact_check_mode` accepts `"lexical"` (default, fastest), `"hybrid"`
    /// (lexical + embeddings, ~3-5s), or `"semantic"` (embeddings + LLM-judge,
    /// ~5-15s). Hybrid/semantic silently fall back to lexical when the
    /// server's BGE cache is unavailable.
    pub async fn run_with_fact_check(
        &self,
        agent_id: &str,
        input: &str,
        verification_mode: Option<&str>,
        fact_check_mode: Option<&str>,
    ) -> AgentsResult<AgentRunResponse> {
        if input.is_empty() {
            return Err(AgentsError::InvalidInput("input is required".into()));
        }
        let mut body = serde_json::json!({ "input": input });
        if let Some(v) = verification_mode {
            body["verification_mode"] = serde_json::Value::String(v.to_string());
        }
        if let Some(f) = fact_check_mode {
            body["fact_check_mode"] = serde_json::Value::String(f.to_string());
        }
        self.request::<AgentRunResponse>(
            Method::POST,
            &format!("/v1/agents/{agent_id}/runs"),
            Some(&body),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for run"))
    }

    pub async fn a2a_invoke(
        &self,
        agent_id: &str,
        input: &str,
        trace: &[&str],
    ) -> AgentsResult<A2aResponse> {
        self.a2a_invoke_with_fact_check(agent_id, input, trace, None)
            .await
    }

    /// `POST /v1/a2a/:agent_id` with explicit fact-checker mode.
    pub async fn a2a_invoke_with_fact_check(
        &self,
        agent_id: &str,
        input: &str,
        trace: &[&str],
        fact_check_mode: Option<&str>,
    ) -> AgentsResult<A2aResponse> {
        if input.is_empty() {
            return Err(AgentsError::InvalidInput("input is required".into()));
        }
        let mut body = serde_json::json!({ "input": input });
        if let Some(f) = fact_check_mode {
            body["fact_check_mode"] = serde_json::Value::String(f.to_string());
        }
        let mut extra = HeaderMap::new();
        if !trace.is_empty() {
            if let Ok(val) = trace.join(",").parse() {
                extra.insert("x-a2a-trace", val);
            }
        }
        self.request::<A2aResponse>(
            Method::POST,
            &format!("/v1/a2a/{agent_id}"),
            Some(&body),
            Some(extra),
        )
        .await
        .map(|o| o.expect("server returned empty body for a2a_invoke"))
    }

    // ── Tasks (poll + stream) ───────────────────────────────────────

    /// `GET /v1/tasks/:id` — fetch the current state of a task.
    pub async fn get_task(&self, task_id: &str) -> AgentsResult<Task> {
        self.request::<Task>(
            Method::GET,
            &format!("/v1/tasks/{task_id}"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for get_task"))
    }

    /// `DELETE /v1/tasks/:id` — cancel a queued or running task.
    pub async fn cancel_task(&self, task_id: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/tasks/{task_id}"),
                Option::<&()>::None,
                None,
            )
            .await?;
        Ok(())
    }

    // ── Shareable runs ──────────────────────────────────────────────

    /// `POST /v1/tasks/:id/share` — publish a run as a public URL.
    ///
    /// Idempotent : calling on an already-shared task returns the
    /// existing [`ShareResponse`] without bumping the per-tenant cap.
    /// The returned `url` (form `https://wauldo.com/r/<id>`) can be
    /// pasted anywhere — anyone with the link sees the verdict +
    /// claims + sources + timeline through a strict-whitelist
    /// projection (no `custom_preset` / `wauldo_toml` / system prompt /
    /// tool args ever leave the tenant).
    ///
    /// Free-tier tenants get a 30-day TTL ; paid tenants get
    /// `expires_at = None` (no expiration).
    pub async fn share_task(&self, task_id: &str) -> AgentsResult<ShareResponse> {
        self.request::<ShareResponse>(
            Method::POST,
            &format!("/v1/tasks/{task_id}/share"),
            Some(&serde_json::json!({})),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for share_task"))
    }

    /// `DELETE /v1/tasks/:id/share` — make a published run private again.
    ///
    /// Idempotent : calling on a never-published task returns `Ok(())`.
    /// Subsequent `GET /v1/runs/<share_id>` for the cleared id returns
    /// 404.
    pub async fn unshare_task(&self, task_id: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/tasks/{task_id}/share"),
                Option::<&()>::None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Poll [`Self::get_task`] until the task reaches a terminal status.
    ///
    /// Returns the final [`Task`] snapshot. Returns `AgentsError::InvalidInput`
    /// if the task is still running after `timeout`. Use [`Self::stream_task`]
    /// when you need per-state progress events instead of a single final
    /// snapshot.
    pub async fn wait_for_task(
        &self,
        task_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> AgentsResult<Task> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let task = self.get_task(task_id).await?;
            if task.is_done() {
                return Ok(task);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AgentsError::InvalidInput(format!(
                    "task {task_id} still in status {:?} after {:?}",
                    task.status, timeout
                )));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Subscribe to `GET /v1/tasks/:id/stream` and return a `Stream` of
    /// typed [`StateTransition`] events. The stream closes when the
    /// upstream SSE stream closes (task reached terminal status) or on
    /// connection error.
    ///
    /// # Example
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures_util::StreamExt;
    /// use wauldo::agents::AgentsClient;
    ///
    /// let agents = AgentsClient::new("https://api.wauldo.com").with_api_key("sk");
    /// let stream = agents.stream_task("task_xyz").await?;
    /// futures_util::pin_mut!(stream);
    /// while let Some(ev) = stream.next().await {
    ///     let ev = ev?;
    ///     println!("{}: {}ms", ev.state_name, ev.duration_ms);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn stream_task(
        &self,
        task_id: &str,
    ) -> AgentsResult<impl futures_util::Stream<Item = AgentsResult<StateTransition>>> {
        let url = format!("{}/v1/tasks/{task_id}/stream", self.base_url);
        let mut headers = self.headers(None);
        headers.insert("Accept", "text/event-stream".parse().unwrap());
        let resp = self
            .client
            .request(Method::GET, &url)
            .headers(headers)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let stream = async_stream::try_stream! {
            let mut bytes = resp.bytes_stream();
            let mut buf: Vec<u8> = Vec::new();
            while let Some(chunk) = bytes.next().await {
                let chunk = chunk.map_err(AgentsError::Http)?;
                buf.extend_from_slice(&chunk);
                while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line_bytes = buf.drain(..=pos).collect::<Vec<u8>>();
                    let line = std::str::from_utf8(&line_bytes)
                        .unwrap_or("")
                        .trim_end_matches(['\n', '\r']);
                    if !line.starts_with("data:") {
                        continue;
                    }
                    let payload = line[5..].trim();
                    if payload.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<StateTransition>(payload) {
                        Ok(ev) => yield ev,
                        Err(_) => continue, // keep-alive or partial frame
                    }
                }
            }
        };
        Ok(stream)
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
        assert_eq!(verif.verdict, Verdict::Unverified);
        assert_eq!(verif.trust_score, 0.0);
        assert_eq!(
            verif.message.as_deref(),
            Some("No source documents uploaded.")
        );
    }

    #[tokio::test]
    async fn test_cancel_task_sends_delete() {
        let server = stub_agent_server().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.cancel_task("t1").await.unwrap();
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

        let client = AgentsClient::new(server.uri());
        let stream = client.stream_task("t1").await.unwrap();
        futures_util::pin_mut!(stream);
        let mut got = Vec::new();
        while let Some(ev) = stream.next().await {
            got.push(ev.unwrap());
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].state_name, "Analysis");
        assert_eq!(got[1].state_name, "Tradeoffs");
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
}
