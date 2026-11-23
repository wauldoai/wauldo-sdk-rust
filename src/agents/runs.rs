//! Runs: starting an agent run (optionally waiting for its verified result)
//! and agent-to-agent invocation.

use std::time::Duration;

use reqwest::{header::HeaderMap, Method};

use super::client::AgentsClient;
use super::types::*;

impl AgentsClient {
    // ── Runs ─────────────────────────────────────────────────────

    /// `POST /v1/agents/:id/runs`. `verification_mode` is `"strict"`,
    /// `"balanced"` or `"permissive"` (the server's default); any other value
    /// is refused with a 400.
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
        self.run_with_options(
            agent_id,
            input,
            RunOptions {
                verification_mode: verification_mode.map(str::to_string),
                fact_check_mode: fact_check_mode.map(str::to_string),
                ..RunOptions::default()
            },
        )
        .await
    }

    /// `POST /v1/agents/:id/runs` with every option.
    ///
    /// With [`RunOptions::wait`] the server holds the response until the run
    /// settles or the delay elapses (clamped to [`MAX_WAIT_SECS`]), and fills
    /// [`AgentRunResponse::task`] with the task as it stands — the verified
    /// result in one round-trip when the run is quick. Use
    /// [`Self::run_and_wait`] to always get the final [`Task`].
    pub async fn run_with_options(
        &self,
        agent_id: &str,
        input: &str,
        opts: RunOptions,
    ) -> AgentsResult<AgentRunResponse> {
        if input.is_empty() {
            return Err(AgentsError::InvalidInput("input is required".into()));
        }
        let mut body = serde_json::json!({ "input": input });
        if let Some(v) = opts.verification_mode {
            body["verification_mode"] = serde_json::Value::String(v);
        }
        if let Some(f) = opts.fact_check_mode {
            body["fact_check_mode"] = serde_json::Value::String(f);
        }
        if let Some(url) = opts.webhook_url {
            body["webhook_url"] = serde_json::Value::String(url);
        }
        if let Some(n) = opts.max_retries {
            body["max_retries"] = n.into();
        }
        if let Some(share) = opts.auto_share {
            body["auto_share"] = share.into();
        }
        self.request::<AgentRunResponse>(
            Method::POST,
            &format!("/v1/agents/{agent_id}/runs{}", wait_query(opts.wait)),
            Some(&body),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for run"))
    }

    /// Run an agent and return the settled [`Task`] — one call.
    ///
    /// The run is created with the server holding the response for up to
    /// [`MAX_WAIT_SECS`]; a quick run therefore comes back verified in a
    /// single round-trip. A longer one is followed with
    /// [`Self::wait_for_task`] until `timeout` ([`DEFAULT_WAIT_TIMEOUT`]
    /// covers ordinary runs, not every run), which returns
    /// [`AgentsError::Timeout`] if the task has not settled by then; the run
    /// goes on on the server, and `wait_for_task` with the error's `task_id`
    /// keeps waiting.
    /// `opts.wait` is ignored: the wait is `timeout`.
    pub async fn run_and_wait(
        &self,
        agent_id: &str,
        input: &str,
        opts: RunOptions,
        timeout: Duration,
    ) -> AgentsResult<Task> {
        let deadline = tokio::time::Instant::now() + timeout;
        let resp = self
            .run_with_options(
                agent_id,
                input,
                RunOptions {
                    wait: Some(timeout.min(Duration::from_secs(MAX_WAIT_SECS))),
                    ..opts
                },
            )
            .await?;
        if let Some(task) = resp.task.filter(|t| t.is_settled()) {
            return Ok(task);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.wait_for_task(&resp.task_id, remaining, Duration::from_secs(2))
            .await
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
}
