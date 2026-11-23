//! Tasks: reading, cancelling, deleting and sharing a task, and waiting
//! until it settles.

use std::time::Duration;

use reqwest::Method;

use super::client::AgentsClient;
use super::types::*;

impl AgentsClient {
    // ── Tasks (poll + stream) ───────────────────────────────────────

    /// `GET /v1/tasks/:id` — fetch the current state of a task.
    pub async fn get_task(&self, task_id: &str) -> AgentsResult<Task> {
        self.get_task_with(task_id, None, None).await
    }

    /// `GET /v1/tasks/:id`, holding the response until the task settles or
    /// `wait` elapses (clamped to [`MAX_WAIT_SECS`]): a long poll, so the
    /// result arrives the moment it lands. The task is returned as it stands
    /// either way.
    pub async fn get_task_wait(&self, task_id: &str, wait: Option<Duration>) -> AgentsResult<Task> {
        self.get_task_with(task_id, wait, None).await
    }

    /// `GET /v1/tasks/:id` with every read option: `wait` as in
    /// [`Self::get_task_wait`], and `include_journal`. `Some(false)` leaves
    /// the execution journal (every state's raw output) out of the response:
    /// `journal` is then `None`. `None` does not send it, so the journal is
    /// included.
    pub async fn get_task_with(
        &self,
        task_id: &str,
        wait: Option<Duration>,
        include_journal: Option<bool>,
    ) -> AgentsResult<Task> {
        self.request::<Task>(
            Method::GET,
            &format!("/v1/tasks/{task_id}{}", task_query(wait, include_journal)),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for get_task"))
    }

    /// `PUT /v1/tasks/:id` — cancel a queued or running task; returns the task
    /// as cancelled. (Up to 0.17, this sent `DELETE`, which the server maps to
    /// *deleting a finished task*: a running task could not be cancelled, and
    /// a finished one was erased.)
    pub async fn cancel_task(&self, task_id: &str) -> AgentsResult<Task> {
        self.request::<Task>(
            Method::PUT,
            &format!("/v1/tasks/{task_id}"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for cancel_task"))
    }

    /// `DELETE /v1/tasks/:id` — delete a finished task (completed, failed,
    /// cancelled).
    pub async fn delete_task(&self, task_id: &str) -> AgentsResult<()> {
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
    /// A share expires 30 days after it is published (`expires_at`, epoch
    /// milliseconds; `None` would mean no expiry, which no plan has today).
    /// The shared run stays available
    /// for as long as its link is valid: until it expires, is withdrawn with
    /// [`Self::unshare_task`], or the task is deleted.
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

    /// Wait until the task settles — terminal, or paused for approval.
    ///
    /// Returns the final [`Task`] snapshot, or [`AgentsError::Timeout`]
    /// (with the task id and its last status) if the task has not settled
    /// after `timeout`; the task keeps running on the server, and calling
    /// this again with the error's `task_id` keeps waiting.
    /// [`DEFAULT_WAIT_TIMEOUT`] covers ordinary runs, not every run.
    /// Use [`Self::stream_task`] when you need per-state progress events
    /// instead of a single final snapshot.
    pub async fn wait_for_task(
        &self,
        task_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> AgentsResult<Task> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let started = tokio::time::Instant::now();
            // Each request is a long poll: the server answers the moment the
            // task settles, so the result is not delayed by `poll_interval`.
            // Never ask the server to hold longer than what is left of the
            // caller's own deadline; at or past it, a plain read then the error.
            let hold = Duration::from_secs(LONG_POLL_SECS).min(remaining);
            let task = self.get_task_wait(task_id, Some(hold)).await?;
            if task.is_settled() {
                return Ok(task);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AgentsError::Timeout {
                    task_id: task_id.to_string(),
                    last_status: task.status,
                    waited: timeout,
                });
            }
            // A reply within half a second means the server did not hold the
            // request (older server): pace the polls ourselves.
            if started.elapsed() < Duration::from_millis(500) {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(poll_interval.min(remaining)).await;
            }
        }
    }
}

/// `?wait=N` and/or `include_journal=true|false`; empty when neither is set.
fn task_query(wait: Option<Duration>, include_journal: Option<bool>) -> String {
    let wait = wait_query(wait);
    match include_journal {
        None => wait,
        Some(keep) if wait.is_empty() => format!("?include_journal={keep}"),
        Some(keep) => format!("{wait}&include_journal={keep}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_query_sends_include_journal_only_when_given() {
        assert_eq!(task_query(None, None), "");
        assert_eq!(task_query(None, Some(false)), "?include_journal=false");
        assert_eq!(
            task_query(Some(Duration::from_secs(5)), Some(true)),
            "?wait=5&include_journal=true"
        );
        assert_eq!(task_query(Some(Duration::from_secs(5)), None), "?wait=5");
    }
}
