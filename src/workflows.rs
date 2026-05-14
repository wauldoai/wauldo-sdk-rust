//! Workflows API client — Wauldo Workflow Runtime (Step Functions style).
//!
//! State-machine workflows authored as `Task` / `Choice` / `Wait` / `Pass` /
//! `Fail` / `Succeed` states. Runs are async: [`WorkflowsClient::start_run`]
//! returns an `execution_id`, then poll [`WorkflowsClient::get_run`] (or use
//! [`WorkflowsClient::wait_for_run`]) until a terminal status.
//!
//! # Example
//! ```no_run
//! use wauldo::workflows::{CreateWorkflowRequest, WorkflowsClient};
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let wf = WorkflowsClient::new("https://api.wauldo.com")
//!     .with_api_key("tig_live_...");
//!
//! let mut states = HashMap::new();
//! states.insert(
//!     "Compute".to_string(),
//!     json!({ "type": "Task", "resource": "tool:calculator", "next": "Done" }),
//! );
//! states.insert("Done".to_string(), json!({ "type": "Succeed" }));
//!
//! let created = wf.create(CreateWorkflowRequest {
//!     name: "triage".into(),
//!     start_at: "Compute".into(),
//!     states,
//!     description: None,
//! }).await?;
//!
//! let run = wf.start_run(&created.id, Some(json!({"operation": "add", "a": 21, "b": 21}))).await?;
//! let final_exec = wf.wait_for_run(&created.id, &run.execution_id, None, None).await?;
//! println!("status={} output={:?}", final_exec.status, final_exec.output);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use reqwest::{header::HeaderMap, Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Instant};

use crate::agents::{bounded_read, AgentsError, AgentsResult, MAX_RESPONSE_SIZE};

/// Statuses that terminate a workflow run.
pub const TERMINAL_WORKFLOW_STATUSES: &[&str] = &["succeeded", "failed", "timed_out"];

/// `true` when the supplied status string is one of the terminal statuses
/// returned by the workflow runtime.
pub fn is_workflow_run_terminal(status: &str) -> bool {
    TERMINAL_WORKFLOW_STATUSES.contains(&status)
}

// ─── Wire types ───────────────────────────────────────────────────────

/// A workflow definition (`GET /v1/workflows/:id`).
///
/// `states` is kept as raw JSON values so future state types added on the
/// server side don't require a SDK release.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub start_at: String,
    pub states: HashMap<String, serde_json::Value>,
    pub version: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CreateWorkflowRequest {
    pub name: String,
    pub start_at: String,
    pub states: HashMap<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowListResponse {
    #[serde(default)]
    pub workflows: Vec<Workflow>,
}

/// 202 response from `POST /v1/workflows/:id/runs`.
#[derive(Debug, Clone, Deserialize)]
pub struct StartRunResponse {
    pub execution_id: String,
    pub workflow_id: String,
    pub status: String,
}

/// A workflow execution record. `status` is one of `running`, `succeeded`,
/// `failed`, `timed_out`. `output` is populated on success; `error` on
/// terminal failure.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowExecution {
    pub id: String,
    pub workflow_id: String,
    pub tenant_id: String,
    pub status: String,
    #[serde(default)]
    pub current_state: Option<String>,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub output: Option<serde_json::Value>,
    pub started_at: u64,
    #[serde(default)]
    pub ended_at: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
}

impl WorkflowExecution {
    pub fn is_terminal(&self) -> bool {
        is_workflow_run_terminal(&self.status)
    }

    pub fn succeeded(&self) -> bool {
        self.status == "succeeded"
    }
}

// Internal wire envelopes — the server wraps singletons in
// `{ "workflow": ... }` / `{ "execution": ... }`.
#[derive(Debug, Deserialize)]
struct WorkflowEnvelope {
    workflow: Workflow,
}

#[derive(Debug, Deserialize)]
struct ExecutionEnvelope {
    execution: WorkflowExecution,
}

// ─── Client ───────────────────────────────────────────────────────────

pub struct WorkflowsClient {
    base_url: String,
    api_key: Option<String>,
    tenant: Option<String>,
    client: Client,
}

impl WorkflowsClient {
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
        body: Option<&impl Serialize>,
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

    // ── CRUD ─────────────────────────────────────────────────────────

    /// `POST /v1/workflows` — create a workflow definition.
    ///
    /// The server validates cycles, transition targets, choice operators,
    /// and the per-tenant cap (100) before returning 201.
    pub async fn create(&self, req: CreateWorkflowRequest) -> AgentsResult<Workflow> {
        let env: WorkflowEnvelope = self
            .request::<WorkflowEnvelope>(Method::POST, "/v1/workflows", Some(&req))
            .await?
            .ok_or_else(|| {
                AgentsError::InvalidInput("server returned empty body for create".into())
            })?;
        Ok(env.workflow)
    }

    /// `GET /v1/workflows` — list workflows for the calling tenant.
    pub async fn list(&self) -> AgentsResult<WorkflowListResponse> {
        self.request::<WorkflowListResponse>(Method::GET, "/v1/workflows", Option::<&()>::None)
            .await
            .map(|o| o.unwrap_or(WorkflowListResponse { workflows: vec![] }))
    }

    /// `GET /v1/workflows/:id`
    pub async fn get(&self, workflow_id: &str) -> AgentsResult<Workflow> {
        let env: WorkflowEnvelope = self
            .request::<WorkflowEnvelope>(
                Method::GET,
                &format!("/v1/workflows/{workflow_id}"),
                Option::<&()>::None,
            )
            .await?
            .ok_or_else(|| {
                AgentsError::InvalidInput("server returned empty body for get".into())
            })?;
        Ok(env.workflow)
    }

    /// `DELETE /v1/workflows/:id`
    pub async fn delete(&self, workflow_id: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/workflows/{workflow_id}"),
                Option::<&()>::None,
            )
            .await?;
        Ok(())
    }

    // ── Runs ─────────────────────────────────────────────────────────

    /// `POST /v1/workflows/:id/runs` — start an async execution.
    ///
    /// Returns 202 with an `execution_id` immediately. Poll [`Self::get_run`]
    /// or use [`Self::wait_for_run`] to await completion.
    pub async fn start_run(
        &self,
        workflow_id: &str,
        input: Option<serde_json::Value>,
    ) -> AgentsResult<StartRunResponse> {
        #[derive(Serialize)]
        struct Body {
            #[serde(skip_serializing_if = "Option::is_none")]
            input: Option<serde_json::Value>,
        }
        self.request::<StartRunResponse>(
            Method::POST,
            &format!("/v1/workflows/{workflow_id}/runs"),
            Some(&Body { input }),
        )
        .await?
        .ok_or_else(|| AgentsError::InvalidInput("server returned empty body for start_run".into()))
    }

    /// `GET /v1/workflows/:id/runs/:execution_id` — fetch one execution.
    pub async fn get_run(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> AgentsResult<WorkflowExecution> {
        let env: ExecutionEnvelope = self
            .request::<ExecutionEnvelope>(
                Method::GET,
                &format!("/v1/workflows/{workflow_id}/runs/{execution_id}"),
                Option::<&()>::None,
            )
            .await?
            .ok_or_else(|| {
                AgentsError::InvalidInput("server returned empty body for get_run".into())
            })?;
        Ok(env.execution)
    }

    /// Poll [`Self::get_run`] until the run reaches a terminal status.
    ///
    /// Returns [`AgentsError::InvalidInput`] if the run hasn't terminated
    /// within `timeout`. The server enforces its own 60s wall-clock cap per
    /// run, so a timeout larger than ~75s is just slack for polling overhead.
    ///
    /// Defaults: `timeout = 90s`, `poll_interval = 1s`.
    pub async fn wait_for_run(
        &self,
        workflow_id: &str,
        execution_id: &str,
        timeout: Option<Duration>,
        poll_interval: Option<Duration>,
    ) -> AgentsResult<WorkflowExecution> {
        let timeout = timeout.unwrap_or_else(|| Duration::from_secs(90));
        let poll_interval = poll_interval.unwrap_or_else(|| Duration::from_secs(1));
        let deadline = Instant::now() + timeout;
        loop {
            let execution = self.get_run(workflow_id, execution_id).await?;
            if execution.is_terminal() {
                return Ok(execution);
            }
            if Instant::now() >= deadline {
                return Err(AgentsError::InvalidInput(format!(
                    "workflow run {execution_id} did not terminate within {timeout:?} \
                     (last status: {})",
                    execution.status
                )));
            }
            sleep(poll_interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_terminal() {
        assert!(is_workflow_run_terminal("succeeded"));
        assert!(is_workflow_run_terminal("failed"));
        assert!(is_workflow_run_terminal("timed_out"));
        assert!(!is_workflow_run_terminal("running"));
        assert!(!is_workflow_run_terminal("queued"));
    }

    #[test]
    fn test_execution_terminal_helpers() {
        let succeeded = WorkflowExecution {
            id: "wfr_1".into(),
            workflow_id: "wf_1".into(),
            tenant_id: "t1".into(),
            status: "succeeded".into(),
            current_state: None,
            input: serde_json::json!(null),
            output: Some(serde_json::json!({"ok": true})),
            started_at: 100,
            ended_at: Some(110),
            error: None,
        };
        assert!(succeeded.is_terminal());
        assert!(succeeded.succeeded());

        let running = WorkflowExecution {
            id: "wfr_2".into(),
            workflow_id: "wf_1".into(),
            tenant_id: "t1".into(),
            status: "running".into(),
            current_state: Some("Compute".into()),
            input: serde_json::json!(null),
            output: None,
            started_at: 100,
            ended_at: None,
            error: None,
        };
        assert!(!running.is_terminal());
        assert!(!running.succeeded());
    }

    #[test]
    fn test_workflow_deserialize() {
        let json = serde_json::json!({
            "id": "wf_1",
            "tenant_id": "t1",
            "name": "triage",
            "start_at": "Compute",
            "states": { "Compute": { "type": "Succeed" } },
            "version": "1.0",
            "created_at": 100,
            "updated_at": 200
        });
        let wf: Workflow = serde_json::from_value(json).unwrap();
        assert_eq!(wf.id, "wf_1");
        assert_eq!(wf.start_at, "Compute");
        assert!(wf.states.contains_key("Compute"));
        assert!(wf.description.is_none());
    }

    #[test]
    fn test_envelope_unwrap() {
        let json = serde_json::json!({
            "workflow": {
                "id": "wf_1",
                "tenant_id": "t1",
                "name": "triage",
                "start_at": "S",
                "states": { "S": { "type": "Succeed" } },
                "version": "1.0",
                "created_at": 1,
                "updated_at": 2,
            }
        });
        let env: WorkflowEnvelope = serde_json::from_value(json).unwrap();
        assert_eq!(env.workflow.id, "wf_1");
    }

    #[test]
    fn test_client_construction() {
        let c = WorkflowsClient::new("http://localhost:3000/")
            .with_api_key("k")
            .with_tenant("t");
        let h = c.headers();
        assert_eq!(h.get("Authorization").unwrap(), "Bearer k");
        assert_eq!(h.get("x-rapidapi-user").unwrap(), "t");
        assert_eq!(c.base_url, "http://localhost:3000");
    }
}
