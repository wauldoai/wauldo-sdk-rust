//! Human-in-the-loop calls of `AgentsClient`: pause a task for a human
//! decision, then approve it (queued, it runs again) or reject it (cancelled).
//! A task left paused 7 days without a decision is cancelled by the server;
//! a failed or cancelled task runs again, as a new task, with `resume_task`.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use super::client::AgentsClient;
use super::types::{AgentRunResponse, AgentsResult, TaskStatus};

/// `error` of a task the server cancelled because it stayed paused for
/// approval 7 days without a decision.
pub const PAUSED_EXPIRED_ERROR: &str = "Cancelled: no approval decision within 7 days";

/// Longest `reason` accepted with a decision, in characters (HTTP 400 above).
pub const MAX_HITL_REASON_CHARS: usize = 1000;

/// A human decision taken on a task ([`crate::agents::Task::decisions`]), with
/// the reviewer's note if one was sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDecision {
    /// `"pause"`, `"approve"` or `"reject"`.
    pub action: String,
    /// The reviewer's note, when one was sent.
    #[serde(default)]
    pub reason: Option<String>,
    /// When the decision was applied (Unix seconds).
    #[serde(default)]
    pub at: u64,
}

/// Result of [`AgentsClient::pause_task`], [`AgentsClient::approve_task`] and
/// [`AgentsClient::reject_task`]: the task's status before and after.
#[derive(Debug, Clone, Deserialize)]
pub struct HitlResponse {
    pub task_id: String,
    pub previous_status: TaskStatus,
    pub new_status: TaskStatus,
}

impl AgentsClient {
    /// `POST /v1/tasks/:id/pause` — stop a queued or running task until a
    /// human decides (status `PausedForApproval`). The run in flight is
    /// stopped, not only relabelled; `wait_for_task` and `run_and_wait`
    /// return as soon as a task is paused. `reason` is an optional note (at
    /// most [`MAX_HITL_REASON_CHARS`] characters, HTTP 400 above) stored with
    /// the decision: it is listed in `Task::decisions`, which keeps the 20 most
    /// recent decisions (the oldest is dropped past that).
    ///
    /// A task left paused 7 days without a decision is cancelled by the
    /// server, with the error [`PAUSED_EXPIRED_ERROR`]; approving or rejecting
    /// it then answers HTTP 409, and [`Self::resume_task`] runs it again as a
    /// new task. HTTP 409 here: the task cannot be paused (already over or
    /// paused); 404: it does not exist.
    pub async fn pause_task(
        &self,
        task_id: &str,
        reason: Option<&str>,
    ) -> AgentsResult<HitlResponse> {
        self.hitl(task_id, "pause", reason).await
    }

    /// `POST /v1/tasks/:id/approve` — resume a paused task: it is queued and
    /// runs again. `reason` is stored with the decision (see
    /// [`Self::pause_task`]). HTTP 409 if the task is not paused for approval,
    /// including a task cancelled after 7 days without a decision (its
    /// `error` is then [`PAUSED_EXPIRED_ERROR`]).
    pub async fn approve_task(
        &self,
        task_id: &str,
        reason: Option<&str>,
    ) -> AgentsResult<HitlResponse> {
        self.hitl(task_id, "approve", reason).await
    }

    /// `POST /v1/tasks/:id/reject` — cancel a paused task (final status
    /// `Cancelled`). `reason` is stored with the decision (see
    /// [`Self::pause_task`]). HTTP 409 if the task is not paused for approval,
    /// including a task already cancelled after 7 days without a decision.
    pub async fn reject_task(
        &self,
        task_id: &str,
        reason: Option<&str>,
    ) -> AgentsResult<HitlResponse> {
        self.hitl(task_id, "reject", reason).await
    }

    /// `POST /v1/tasks/:id/resume` — run a failed or cancelled task again.
    /// The server creates a **new** task from the same prompt and settings
    /// (webhook included); the original stays as it was. Returns the new
    /// task's id and status (`queued`): follow it with `wait_for_task` or
    /// `stream_task`. HTTP 409 if the task is not failed or cancelled; 404 if
    /// it does not exist.
    pub async fn resume_task(&self, task_id: &str) -> AgentsResult<AgentRunResponse> {
        self.request::<AgentRunResponse>(
            Method::POST,
            &format!("/v1/tasks/{task_id}/resume"),
            Some(&serde_json::json!({})),
            None,
        )
        .await?
        .ok_or_else(|| {
            super::types::AgentsError::InvalidInput(
                "server returned empty body for resume_task".into(),
            )
        })
    }

    async fn hitl(
        &self,
        task_id: &str,
        action: &str,
        reason: Option<&str>,
    ) -> AgentsResult<HitlResponse> {
        let body = match reason {
            Some(r) => serde_json::json!({ "reason": r }),
            None => serde_json::json!({}),
        };
        self.request::<HitlResponse>(
            Method::POST,
            &format!("/v1/tasks/{task_id}/{action}"),
            Some(&body),
            None,
        )
        .await?
        .ok_or_else(|| {
            super::types::AgentsError::InvalidInput(format!(
                "server returned empty body for {action}_task"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn server_answering(action: &str, body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/v1/tasks/t1/{action}")))
            .and(body_json(body))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "task_id": "t1",
                "previous_status": "running",
                "new_status": "paused_for_approval",
            })))
            .expect(1)
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn test_hitl_calls_post_their_transition() {
        for action in ["pause", "approve", "reject"] {
            let server = server_answering(action, serde_json::json!({})).await;
            let client = AgentsClient::new(server.uri());
            let out = match action {
                "pause" => client.pause_task("t1", None).await,
                "approve" => client.approve_task("t1", None).await,
                _ => client.reject_task("t1", None).await,
            }
            .expect(action);
            assert_eq!(out.task_id, "t1");
            assert_eq!(out.previous_status, TaskStatus::Running);
            assert_eq!(out.new_status, TaskStatus::PausedForApproval);
        }
    }

    #[tokio::test]
    async fn test_hitl_sends_the_reason_when_given() {
        let server = server_answering("reject", serde_json::json!({ "reason": "wrong" })).await;
        let client = AgentsClient::new(server.uri());
        client
            .reject_task("t1", Some("wrong"))
            .await
            .expect("reject with a reason");
    }

    #[tokio::test]
    async fn test_resume_task_posts_and_returns_the_new_task() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tasks/t1/resume"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "task_id": "t2",
                "status": "queued",
                "created_at": 9,
            })))
            .expect(1)
            .mount(&server)
            .await;
        let out = AgentsClient::new(server.uri())
            .resume_task("t1")
            .await
            .expect("resume");
        assert_eq!(out.task_id, "t2");
        assert_eq!(out.status, "queued");
    }

    #[test]
    fn test_task_decisions_are_read() {
        let task: crate::agents::Task = serde_json::from_value(serde_json::json!({
            "task_id": "t1",
            "status": "cancelled",
            "error": PAUSED_EXPIRED_ERROR,
            "decisions": [
                { "action": "pause", "reason": "check the total", "at": 10 },
                { "action": "approve", "at": 11 }
            ]
        }))
        .expect("task");
        assert_eq!(task.decisions.len(), 2);
        assert_eq!(task.decisions[0].reason.as_deref(), Some("check the total"));
        assert_eq!(task.decisions[1].reason, None);
        let none: crate::agents::Task =
            serde_json::from_value(serde_json::json!({ "task_id": "t1", "status": "running" }))
                .expect("task without decisions");
        assert!(none.decisions.is_empty());
    }

    #[tokio::test]
    async fn test_hitl_conflict_is_a_status_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/tasks/t1/approve"))
            .respond_with(ResponseTemplate::new(409).set_body_string("not paused"))
            .mount(&server)
            .await;
        let err = AgentsClient::new(server.uri())
            .approve_task("t1", None)
            .await
            .expect_err("409");
        assert!(
            matches!(
                err,
                super::super::types::AgentsError::Status { status: 409, .. }
            ),
            "{err:?}"
        );
    }
}
