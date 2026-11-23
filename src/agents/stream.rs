//! `AgentsClient::stream_task` — the SSE stream of a task's state transitions,
//! always ended by the terminal event: when the server has no stream for the
//! task, or the stream closes before that event, it is built from the task.

use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{header::HeaderValue, Method, StatusCode};

use super::client::{retry_delay, AgentsClient};
use super::types::*;

/// Longest [`AgentsClient::stream_task`] waits for the task once the stream is
/// gone (no stream on the server, or closed before its terminal event): the
/// lifetime the server gives a stream.
pub const STREAM_FALLBACK_TIMEOUT: Duration = Duration::from_secs(1800);

/// `condition` of the terminal event: the one the server sends when the task
/// was already over, and the one [`AgentsClient::stream_task`] gives the event
/// it builds from the task when the stream could not deliver it.
pub const ALREADY_FINISHED_CONDITION: &str = "Already finished";

/// Connect timeout of the stream's client. It has no total timeout: a stream
/// lives up to 30 minutes.
pub(super) const STREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest [`AgentsClient::stream_task`] waits on the open stream without a
/// byte (the headers, then each read) before treating it as cut. The server
/// sends a keep-alive every 15 s.
pub const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Pace of the fallback polls against a server that does not hold them.
const FALLBACK_POLL_INTERVAL: Duration = Duration::from_secs(2);

impl AgentsClient {
    /// Subscribe to `GET /v1/tasks/:id/stream` and return a `Stream` of
    /// typed [`StateTransition`] events, one per completed workflow state.
    /// The last one is the terminal event ([`StateTransition::is_terminal`]),
    /// carrying the result.
    ///
    /// When the server has no stream for the task (404: still queued,
    /// already over, server restarted) or the stream closes before its
    /// terminal event (connection cut, 30-minute limit, a minute without even
    /// a keep-alive or without headers), the task is long-polled until it
    /// settles (up to [`STREAM_FALLBACK_TIMEOUT`], through the 429/5xx/refused
    /// connections of a server restarting) and a terminal event built from
    /// it, with condition `"Already finished"`, ends the stream. A task paused
    /// for approval ends it without a terminal event: approve, then stream
    /// again.
    ///
    /// An event named [`STREAM_LAGGED`] is a notice, not a state of the run:
    /// the stream fell behind, so some transitions were skipped and the next
    /// ones are withheld. It does not end the stream; the terminal event still
    /// comes, and [`AgentsClient::get_task`] holds the full run.
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
        let resp = self.open_stream(task_id).await?;
        let client = self.clone();
        let idle = self.stream_idle_timeout;
        let task_id = task_id.to_string();
        Ok(async_stream::try_stream! {
            let mut ended = false;
            if let Some(resp) = resp {
                let mut bytes = resp.bytes_stream();
                let mut buf: Vec<u8> = Vec::new();
                'read: loop {
                    // The end of the body, a cut, or a silence longer than the
                    // keep-alive allows: the task is read below.
                    let chunk = match tokio::time::timeout(idle, bytes.next()).await {
                        Ok(Some(Ok(chunk))) => chunk,
                        _ => break,
                    };
                    buf.extend_from_slice(&chunk);
                    while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                        let line: Vec<u8> = buf.drain(..=pos).collect();
                        if let Some(ev) = parse_sse_line(&line) {
                            let terminal = ev.is_terminal();
                            yield ev;
                            if terminal {
                                ended = true;
                                break 'read;
                            }
                        }
                    }
                }
            }
            if !ended {
                let raw = client.settled_task_through_outages(&task_id).await?;
                if let Some(ev) = terminal_transition(&raw)? {
                    yield ev;
                }
            }
        })
    }

    /// The open stream, or `None` when the task is to be read instead: the
    /// server has no stream for it (404; a task that does not exist answers
    /// 404 too, and the poll then returns it), or its headers did not come
    /// within the idle timeout.
    async fn open_stream(&self, task_id: &str) -> AgentsResult<Option<reqwest::Response>> {
        let url = format!("{}/v1/tasks/{task_id}/stream", self.base_url);
        let mut headers = self.headers(None);
        headers.insert("Accept", HeaderValue::from_static("text/event-stream"));
        let send = self
            .stream_client
            .request(Method::GET, &url)
            .headers(headers)
            .send();
        let Ok(resp) = tokio::time::timeout(self.stream_idle_timeout, send).await else {
            return Ok(None);
        };
        let resp = resp?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body,
            });
        }
        Ok(Some(resp))
    }

    /// Long-poll the task (as the server sent it) until it settles, within
    /// [`STREAM_FALLBACK_TIMEOUT`]. A transient failure (429, 5xx, refused
    /// connection: the server restarting under a deploy) is waited out until
    /// that deadline, not only for `max_retries` attempts: the stream was most
    /// likely cut by that very restart.
    async fn settled_task_through_outages(&self, task_id: &str) -> AgentsResult<serde_json::Value> {
        let deadline = tokio::time::Instant::now() + STREAM_FALLBACK_TIMEOUT;
        let mut failures = 0u32;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let started = tokio::time::Instant::now();
            let hold = Duration::from_secs(LONG_POLL_SECS).min(remaining);
            let path = format!("/v1/tasks/{task_id}{}", wait_query(Some(hold)));
            let read = self
                .request::<serde_json::Value>(Method::GET, &path, Option::<&()>::None, None)
                .await;
            let raw = match read {
                Ok(raw) => raw.unwrap_or(serde_json::Value::Null),
                Err(e) if is_transient(&e) && tokio::time::Instant::now() < deadline => {
                    let pause = retry_delay(failures, self.retry_backoff, None);
                    tokio::time::sleep(pause.min(remaining)).await;
                    failures = failures.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            };
            failures = 0;
            let status = serde_json::from_value::<Task>(raw.clone())?.status;
            if status.is_settled() {
                return Ok(raw);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AgentsError::Timeout {
                    task_id: task_id.to_string(),
                    last_status: status,
                    waited: STREAM_FALLBACK_TIMEOUT,
                });
            }
            // The server did not hold the request (older server): pace the polls.
            if started.elapsed() < Duration::from_millis(500) {
                tokio::time::sleep(FALLBACK_POLL_INTERVAL.min(remaining)).await;
            }
        }
    }
}

/// A failure a deploy or restart causes (429, 5xx, refused or reset
/// connection, timeout): worth reading the task again.
fn is_transient(err: &AgentsError) -> bool {
    match err {
        AgentsError::Status { status, .. } => *status == 429 || *status >= 500,
        AgentsError::Http(_) => true,
        _ => false,
    }
}

/// An SSE `data:` line, as a transition; `None` for a keep-alive, another
/// field, or a frame that is not a transition.
fn parse_sse_line(line: &[u8]) -> Option<StateTransition> {
    let line = std::str::from_utf8(line)
        .ok()?
        .trim_end_matches(['\n', '\r']);
    let payload = line.strip_prefix("data:")?.trim();
    if payload.is_empty() {
        return None;
    }
    serde_json::from_str(payload).ok()
}

/// The terminal event the server would have sent for the task `raw` (the body
/// of `GET /v1/tasks/:id`): same state name, condition and payload, with the
/// verification as the server sent it. A run whose stream withheld its output
/// carries [`WITHHELD_RESULT`] in place of its result, as on the stream.
/// `None` while the task is not over.
fn terminal_transition(raw: &serde_json::Value) -> AgentsResult<Option<StateTransition>> {
    let task: Task = serde_json::from_value(raw.clone())?;
    let state_name = match task.status {
        TaskStatus::Completed => "TASK_COMPLETED",
        TaskStatus::Failed => "TASK_FAILED",
        TaskStatus::Cancelled => "TASK_CANCELLED",
        _ => return Ok(None),
    };
    let result = if task.output_withheld {
        Some(WITHHELD_RESULT.to_string())
    } else {
        task.result.clone()
    };
    let payload = serde_json::json!({
        "task_id": task.task_id,
        "status": task.status,
        "result": result,
        "error": task.error,
        "verification": raw.get("verification").cloned().unwrap_or(serde_json::Value::Null),
    });
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(Some(StateTransition {
        state_name: state_name.to_string(),
        to_state: None,
        condition: ALREADY_FINISHED_CONDITION.to_string(),
        raw_output: payload.to_string(),
        validation_notes: Vec::new(),
        timestamp,
        success: task.status == TaskStatus::Completed,
        retry_count: 0,
        duration_ms: 0,
        prompt_tokens: 0,
        completion_tokens: 0,
        repair_count: 0,
        cache_hit: false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn task_json(status: &str) -> serde_json::Value {
        let completed = status == "completed";
        serde_json::json!({
            "task_id": "t1",
            "tenant_id": "tn",
            "status": status,
            "prompt": "q",
            "created_at": 1u64,
            "updated_at": 2u64,
            "result": if completed { Some("30 days") } else { None },
            "error": if status == "failed" { Some("boom") } else { None },
            "verification": if completed {
                serde_json::json!({
                    "verdict": "SAFE",
                    "hallucination_rate": 0.0,
                    "confidence": 0.9,
                    "trust_score": 1.0,
                    "verification_source": "sources",
                    "claims": [{ "text": "30 days", "supported": true, "confidence": 0.9 }],
                })
            } else {
                serde_json::Value::Null
            },
        })
    }

    fn frame(name: &str, to_state: Option<&str>, condition: &str, raw_output: &str) -> String {
        let ev = serde_json::json!({
            "state_name": name,
            "to_state": to_state,
            "condition": condition,
            "raw_output": raw_output,
            "timestamp": 1u64,
            "success": true,
        });
        format!("data: {ev}\n\n")
    }

    fn plan() -> String {
        frame("PLAN", Some("NEXT"), "ok", "draft")
    }

    fn terminal() -> String {
        frame(
            "TASK_COMPLETED",
            None,
            "terminal",
            r#"{"task_id":"t1","status":"completed","result":"ok"}"#,
        )
    }

    async fn mount(server: &MockServer, route: &str, resp: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path(route))
            .respond_with(resp)
            .mount(server)
            .await;
    }

    fn sse(body: String) -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string(format!(": keep-alive\n\n{body}"))
    }

    async fn collect(client: &AgentsClient) -> Vec<AgentsResult<StateTransition>> {
        let stream = client.stream_task("t1").await.expect("stream opens");
        futures_util::pin_mut!(stream);
        let mut got = Vec::new();
        while let Some(ev) = stream.next().await {
            got.push(ev);
        }
        got
    }

    fn names(got: Vec<AgentsResult<StateTransition>>) -> Vec<String> {
        got.into_iter()
            .map(|ev| ev.expect("event").state_name)
            .collect()
    }

    #[tokio::test]
    async fn test_stream_task_404_falls_back_to_the_task_terminal_event() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/v1/tasks/t1/stream",
            ResponseTemplate::new(404).set_body_string("No active stream for task: t1"),
        )
        .await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(200).set_body_json(task_json("completed")),
        )
        .await;
        // Without the fallback, `stream_task` returned Err(Status 404).
        let mut got = collect(&AgentsClient::new(server.uri())).await;
        assert_eq!(got.len(), 1);
        let last = got.remove(0).expect("terminal event");
        assert!(last.is_terminal());
        assert_eq!(last.state_name, "TASK_COMPLETED");
        assert_eq!(last.condition, ALREADY_FINISHED_CONDITION);
        assert!(last.success);
        let payload = last.terminal_payload().expect("payload");
        assert_eq!(payload.status, TaskStatus::Completed);
        assert_eq!(payload.result.as_deref(), Some("30 days"));
        let verification = payload.verification.expect("verification");
        assert_eq!(verification.verdict, crate::agents::Verdict::Safe);
        assert_eq!(verification.claims[0].text, "30 days");
    }

    #[tokio::test]
    async fn test_stream_task_closed_before_its_terminal_event_is_completed_from_the_task() {
        let server = MockServer::start().await;
        mount(&server, "/v1/tasks/t1/stream", sse(plan())).await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(200).set_body_json(task_json("failed")),
        )
        .await;
        let got = collect(&AgentsClient::new(server.uri())).await;
        let last = got.last().expect("events").as_ref().expect("event").clone();
        // Without the fallback, the stream ended on PLAN, with no result.
        assert_eq!(names(got), ["PLAN", "TASK_FAILED"]);
        assert!(!last.success);
        assert_eq!(
            last.terminal_payload().and_then(|p| p.error).as_deref(),
            Some("boom")
        );
    }

    #[tokio::test]
    async fn test_stream_task_ending_on_its_terminal_event_does_not_read_the_task() {
        let server = MockServer::start().await;
        mount(&server, "/v1/tasks/t1/stream", sse(plan() + &terminal())).await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let got = collect(&AgentsClient::new(server.uri())).await;
        assert_eq!(names(got), ["PLAN", "TASK_COMPLETED"]);
    }

    /// The notice the server sends when the stream fell behind the run, then
    /// a transition withheld after it, as the server shapes them.
    fn lag_notice_then_withheld() -> String {
        let condition =
            "Output withheld: the stream fell behind; the task record holds the full run";
        let notice = serde_json::json!({
            "state_name": STREAM_LAGGED,
            "to_state": null,
            "condition": condition,
            "raw_output": "",
            "validation_notes": ["3 transitions skipped"],
            "timestamp": 1u64,
            "success": false,
            "retry_count": 0,
        });
        format!("data: {notice}\n\n") + &frame("REVIEW", Some("NEXT"), condition, "")
    }

    #[tokio::test]
    async fn test_stream_task_lag_notice_does_not_end_the_stream() {
        let server = MockServer::start().await;
        let body = lag_notice_then_withheld() + &terminal();
        mount(&server, "/v1/tasks/t1/stream", sse(body)).await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let got = collect(&AgentsClient::new(server.uri())).await;
        let notice = got[0].as_ref().expect("notice").clone();
        assert!(!notice.is_terminal());
        assert!(notice.terminal_payload().is_none());
        assert_eq!(notice.validation_notes, ["3 transitions skipped"]);
        assert_eq!(names(got), [STREAM_LAGGED, "REVIEW", "TASK_COMPLETED"]);
    }

    #[tokio::test]
    async fn test_stream_task_closed_after_a_lag_notice_is_completed_from_the_task() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/v1/tasks/t1/stream",
            sse(lag_notice_then_withheld()),
        )
        .await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(200).set_body_json(task_json("completed")),
        )
        .await;
        let got = collect(&AgentsClient::new(server.uri())).await;
        assert_eq!(names(got), [STREAM_LAGGED, "REVIEW", "TASK_COMPLETED"]);
    }

    #[tokio::test]
    async fn test_stream_task_paused_for_approval_ends_without_a_terminal_event() {
        let server = MockServer::start().await;
        mount(&server, "/v1/tasks/t1/stream", ResponseTemplate::new(404)).await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(200).set_body_json(task_json("paused_for_approval")),
        )
        .await;
        assert!(collect(&AgentsClient::new(server.uri())).await.is_empty());
    }

    #[tokio::test]
    async fn test_stream_task_of_an_unknown_task_still_returns_the_404() {
        let server = MockServer::start().await;
        mount(&server, "/v1/tasks/t1/stream", ResponseTemplate::new(404)).await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(404).set_body_string("Task not found: t1"),
        )
        .await;
        let got = collect(&AgentsClient::new(server.uri())).await;
        assert_eq!(got.len(), 1);
        match &got[0] {
            Err(AgentsError::Status { status, .. }) => assert_eq!(*status, 404),
            other => panic!("expected the 404, got {other:?}"),
        }
    }

    /// Serves `/v1/tasks/t1/stream` with `script`, writing on a real socket;
    /// any other path gets the completed task.
    fn raw_server(script: fn(&mut std::net::TcpStream)) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            for sock in listener.incoming() {
                let Ok(mut sock) = sock else { return };
                std::thread::spawn(move || {
                    let mut head = [0u8; 4096];
                    let n = sock.read(&mut head).unwrap_or(0);
                    let request = String::from_utf8_lossy(&head[..n]).to_string();
                    if request.starts_with("GET /v1/tasks/t1/stream ") {
                        script(&mut sock);
                    } else {
                        let body = task_json("completed").to_string();
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        sock.write_all(resp.as_bytes()).expect("write task");
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    const SSE_HEAD: &str =
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";

    #[tokio::test]
    async fn test_stream_task_outlives_the_request_timeout() {
        // A run longer than the client's request timeout, kept alive by the
        // server's keep-alives: the stream must not be cut at that timeout.
        let uri = raw_server(|sock| {
            sock.write_all(SSE_HEAD.as_bytes()).expect("head");
            sock.write_all(plan().as_bytes()).expect("plan");
            for _ in 0..4 {
                std::thread::sleep(Duration::from_millis(300));
                sock.write_all(b": keep-alive\n\n").expect("keep-alive");
            }
            sock.write_all(terminal().as_bytes()).expect("terminal");
        });
        let mut client = AgentsClient::new(uri);
        client.client = reqwest::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .expect("client");
        // On the request client, the stream fails at 500 ms with a timeout:
        // the fallback then builds a TASK_COMPLETED too, but with the
        // condition "Already finished". Only the stream's own terminal event
        // says "terminal".
        let got = collect(&client).await;
        let last = got.last().expect("events").as_ref().expect("event").clone();
        assert_eq!(names(got), ["PLAN", "TASK_COMPLETED"]);
        assert_eq!(last.condition, "terminal");
    }

    #[tokio::test]
    async fn test_stream_task_does_not_stop_on_a_workflow_state_named_like_a_status() {
        let server = MockServer::start().await;
        // A custom workflow whose last state is called TASK_COMPLETED: the
        // sequential executor sends it with no next state.
        let workflow_state = frame(
            "TASK_COMPLETED",
            None,
            "Sequential execution",
            r#"{"summary":"draft"}"#,
        );
        mount(
            &server,
            "/v1/tasks/t1/stream",
            sse(workflow_state + &terminal()),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let got: Vec<StateTransition> = collect(&AgentsClient::new(server.uri()))
            .await
            .into_iter()
            .map(|ev| ev.expect("event"))
            .collect();
        // Without the condition check, the stream stopped on the workflow state.
        let conditions: Vec<&str> = got.iter().map(|e| e.condition.as_str()).collect();
        assert_eq!(conditions, ["Sequential execution", "terminal"]);
        assert!(!got[0].is_terminal());
    }

    /// The `raw_output` of the single event the fallback builds from `task`.
    async fn fallback_payload(task: serde_json::Value) -> serde_json::Value {
        let server = MockServer::start().await;
        mount(&server, "/v1/tasks/t1/stream", ResponseTemplate::new(404)).await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(200).set_body_json(task),
        )
        .await;
        let mut got = collect(&AgentsClient::new(server.uri())).await;
        assert_eq!(got.len(), 1);
        let ev = got.remove(0).expect("terminal event");
        serde_json::from_str(&ev.raw_output).expect("payload")
    }

    #[tokio::test]
    async fn test_stream_task_fallback_of_a_withheld_run_carries_the_marker() {
        let mut task = task_json("completed");
        task["output_withheld"] = serde_json::json!(true);
        task["verification"] = serde_json::Value::Null;
        let payload = fallback_payload(task).await;
        assert_eq!(payload["result"], WITHHELD_RESULT);
        assert!(!payload.to_string().contains("30 days"), "{payload}");
    }

    #[tokio::test]
    async fn test_stream_task_fallback_keeps_the_verification_the_server_sent() {
        let verification = serde_json::json!({
            "verdict": "SOMETHING_NEW",
            "hallucination_rate": 1.0,
            "confidence": 0.0,
            "trust_score": 0.0,
            "verification_source": "sources",
            "claims": [],
            "failure_reason": "injection blocked at stage 2",
            "failure_stage": "guard",
        });
        let mut task = task_json("failed");
        task["verification"] = verification.clone();
        let payload = fallback_payload(task).await;
        assert_eq!(payload["verification"], verification);
    }

    #[tokio::test]
    async fn test_stream_task_fallback_waits_out_a_restart() {
        let server = MockServer::start().await;
        mount(&server, "/v1/tasks/t1/stream", ResponseTemplate::new(404)).await;
        // More 503s than the client's own retry budget (1 + 1 attempts).
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(5)
            .mount(&server)
            .await;
        mount(
            &server,
            "/v1/tasks/t1",
            ResponseTemplate::new(200).set_body_json(task_json("completed")),
        )
        .await;
        let client = AgentsClient::new(server.uri())
            .with_max_retries(1)
            .with_retry_backoff(Duration::from_millis(1));
        let got = collect(&client).await;
        // Without the outage loop, the third 503 came out as Err(Status 503).
        let last = got.last().expect("events").as_ref().expect("event").clone();
        assert_eq!(
            last.terminal_payload().and_then(|p| p.result).as_deref(),
            Some("30 days")
        );
    }

    #[tokio::test]
    async fn test_stream_task_whose_headers_never_come_falls_back_to_the_task() {
        let uri = raw_server(|_sock| {
            // Accepts the connection, then says nothing.
            std::thread::sleep(Duration::from_secs(5));
        });
        let mut client = AgentsClient::new(uri);
        client.stream_idle_timeout = Duration::from_millis(300);
        let started = std::time::Instant::now();
        let got = collect(&client).await;
        // Without the bound, `stream_task` waited for the server to close.
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{:?}",
            started.elapsed()
        );
        let last = got.last().expect("events").as_ref().expect("event").clone();
        assert_eq!(last.condition, ALREADY_FINISHED_CONDITION);
    }

    #[tokio::test]
    async fn test_stream_task_cut_mid_body_is_completed_from_the_task() {
        let uri = raw_server(|sock| {
            // Announces more than it sends, then drops the connection.
            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                        content-length: 100000\r\n\r\n";
            sock.write_all(head.as_bytes()).expect("head");
            sock.write_all(plan().as_bytes()).expect("plan");
        });
        let got = collect(&AgentsClient::new(uri)).await;
        // Without the fallback, the cut came out as an Err after PLAN.
        assert_eq!(names(got), ["PLAN", "TASK_COMPLETED"]);
    }
}
