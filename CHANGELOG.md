# Changelog

All notable changes to the Wauldo Rust SDK.

## [0.18.0] - 2026-11-23

The first release on crates.io since 0.16.0: it also carries the changes listed under 0.17.0 and 0.17.1, which were never published there.

### Fixed
- `HttpClient::chat_stream` is no longer cut after `timeout_secs` (120 s by default): the stream has no total timeout. One that sends nothing for 45 s, not even a keep-alive, yields `Error::Timeout` and closes. Other requests keep their timeout. `AgentsError::Http` says "(timed out)" when the request timed out.

### Added
- `AgentsClient::run_and_wait(agent_id, input, RunOptions, timeout)`: run an agent and get the settled `Task` back in one call. The server holds the response for up to `MAX_WAIT_SECS` (50 s), so a quick run comes back verified in a single round-trip; a longer one is followed with `wait_for_task`.
- `AgentsClient::run_with_options(agent_id, input, RunOptions { verification_mode, fact_check_mode, wait })` and `get_task_wait(task_id, Some(duration))`: the server holds the response until the task settles or the delay elapses (`?wait=`). `AgentRunResponse.task` carries the task when a wait was given.
- `TaskStatus::PausedForApproval`, `TaskStatus::is_settled`, `Task::is_settled`, `MAX_WAIT_SECS`, `LONG_POLL_SECS`.
- `AgentsClient::with_max_retries(n)` (default 3) and `with_retry_backoff(duration)` (default 500 ms): transient failures are retried — 429 and 503 on any call, 500/502/504 and connection failures on `GET` only, so a replayed `POST` can never run an agent twice and a replayed cancel never answers 409 for a cancel that worked. `Retry-After` is honoured (capped at 30 s), the wait otherwise doubles (capped at 8 s, with jitter); timeouts are never retried. `with_max_retries(0)` disables. `RETRY_STATUSES` and `RETRY_STATUSES_IDEMPOTENT` are exported.
- `StateTransition::is_terminal`, `StateTransition::terminal_payload` and `TerminalPayload`: the stream's last event (`TASK_COMPLETED` / `TASK_FAILED` / `TASK_CANCELLED`) carries `task_id`, `status`, `result`, `error` and `verification`, so a streaming client has nothing left to fetch.
- `AgentsClient` is `Clone`; `TaskVerification`, `TaskClaim` and `TerminalPayload` implement `Serialize`.
- `RunOptions { webhook_url, max_retries, auto_share }`: the server POSTs the task to `webhook_url` when the run completes or fails (a private or internal address is refused with a 400); `max_retries` sets the retries of the run on the server (default 3, capped at 10 — not `with_max_retries`, the client's HTTP retries); `auto_share: Some(true)` publishes the run at creation and `AgentRunResponse::share_id` / `share_url` carry the link.
- `CreateAgentRequest::custom_preset`: an inline workflow at creation, used from the agent's first run (it used to need `create` then `create_revision`).
- `pause_task`, `approve_task`, `reject_task` (optional `reason`), returning `HitlResponse { task_id, previous_status, new_status }`: pause a queued or running task for a human decision, then approve it (it is queued and runs again) or reject it (cancelled). The `reason` (at most 1000 characters, `MAX_HITL_REASON_CHARS`) is stored with the decision and listed in `Task::decisions` (`TaskDecision`), which keeps the 20 most recent decisions. A task left paused 7 days without a decision is cancelled by the server (its `error` is `PAUSED_EXPIRED_ERROR`); approving or rejecting it then answers HTTP 409.
- `resume_task(task_id)`: run a failed or cancelled task again, as a new task (`AgentRunResponse` with the new `task_id`; `agent_id` is empty). `AgentRunResponse::agent_id` defaults to empty when absent.
- `get_task_with(task_id, wait, include_journal)`: every read option of `GET /v1/tasks/:id`; `Some(false)` leaves the execution journal out of the response. `get_task` and `get_task_wait` are unchanged.
- `AgentsError::Timeout { task_id, last_status, waited }`: what `wait_for_task`, `run_and_wait` and the fallback of `stream_task` return when the wait runs out. They returned `AgentsError::InvalidInput`, the error of a malformed call. Its message says the task may still be running and how to keep waiting (`wait_for_task` again with `task_id`).
- `DEFAULT_WAIT_TIMEOUT` (360 s): a `timeout` for `wait_for_task` / `run_and_wait` that covers ordinary runs. It is not a bound on how long a run may take: for a long run, pass a larger timeout, use `stream_task`, or give the run a `webhook_url`.
- `Task::error_info()`, `TerminalPayload::error_info()` and `TaskErrorInfo::parse(error)`: a failed task's `error` (`code: message`) split into its stable `code` and its `message` (`TaskErrorInfo::KNOWN_CODES`: `invalid_preset`, `invalid_agent_config`, `rate_limited`, `timeout`, `model_unavailable`, `run_failed`, one associated constant each). An unknown code is kept as sent; an error that does not start with a code has `code: None`. `Task::error` is unchanged. `TaskErrorInfo` is re-exported at the crate root.
- `impl From<AgentsError> for wauldo::Error`: `?` works on the agents, memory and workflows clients in a function that returns `wauldo::Result`. `Status { status, body }` becomes `Error::Server { code: status, message: body }`, a timed-out request `Error::Timeout`, another transport failure `Error::Connection`, `InvalidInput` `Error::Validation`.
- The crate root re-exports the rest of the agents surface: `HitlResponse`, `TaskDecision`, `DEFAULT_WAIT_TIMEOUT`, `A2aResponse`, `ShareResponse`, `PAUSED_EXPIRED_ERROR`, `MAX_HITL_REASON_CHARS`, `RETRY_STATUSES`, `RETRY_STATUSES_IDEMPOTENT`, `STREAM_FALLBACK_TIMEOUT`, `STREAM_IDLE_TIMEOUT`, `ALREADY_FINISHED_CONDITION`, `TERMINAL_CONDITIONS`, `TERMINAL_STATE_NAMES`, `WITHHELD_RESULT` (`use wauldo::DEFAULT_WAIT_TIMEOUT;` works like `use wauldo::agents::DEFAULT_WAIT_TIMEOUT;`).
- `STREAM_LAGGED` (also at the crate root): the `state_name` of the notice the server sends when the stream fell behind the run. It is not a workflow state and not terminal: the run goes on, the terminal event still comes, and `get_task` holds the full run. `stream_task` passes it on and keeps reading.
- `HttpClient::upload_file(path, title, Option<UploadFileOptions>)` and `upload_file_bytes(data, filename, title, options)`: upload a PDF, DOCX, image or text file (`POST /v1/upload-file`, multipart, 10 MB at most for the whole request: file, file name and form fields, `MAX_UPLOAD_FILE_SIZE`), with `UploadFileOptions { pii_policy, timeout_ms }`; returns `UploadFileResponse` (with `DocumentQuality`). A missing or empty file, or one whose request would exceed the limit, is an `Error::Validation` before anything is sent. The README example now compiles against it.
- `HttpClient::rag_query_with_options(query, RagQueryOptions { top_k, debug, quality_mode })`: `quality_mode` (`"fast"`, `"balanced"`, `"premium"`) on `/v1/query`, as the Python and TypeScript SDKs already allowed. `rag_query` and `rag_query_debug` are unchanged.
- `TaskVerification::honesty_signals` (`HonestySignals`), `contract_attestation` (`ContractAttestation`, `ContractVerdict`, `ContractViolation`), `failure_reason` and `failure_stage`; `Task::share_id`, `Task::share_url` and `Task::race_result` (`RaceResult`, `RaceCandidate`). All `Option`, `None` when the server does not send them; re-exported at the crate root.

### Fixed
- `rag_upload`, `rag_upload_with_timeout` and `rag_upload_with_options` refuse a request over the 10 MB limit with `Error::Validation` before sending it, instead of letting the server refuse it. The limit counts the JSON body in UTF-8 bytes, file name included, not the characters of the text (an accented letter is two bytes).
- `cancel_task` sent `DELETE /v1/tasks/:id`, which the server maps to *deleting a finished task*: a running task could not be cancelled, and a finished one was erased. It now sends `PUT` (the server's cancel) and returns the cancelled `Task` (**BREAKING:** it used to return `()`). The old behaviour is available as `delete_task`.
- `stream_task` always ends on the terminal event. When the server has no stream for the task (404: still queued, server restarted) or the stream closes before its last event (connection cut, 30-minute limit), the task is long-polled until it settles (`STREAM_FALLBACK_TIMEOUT`, waiting out the 429/5xx/refused connections of a server restarting) and a terminal event built from it is yielded — condition `"Already finished"` (`ALREADY_FINISHED_CONDITION`), same payload, with the verification exactly as the server sent it, and `WITHHELD_RESULT` in place of the result when the stream withheld the run's output (`Task::output_withheld`) — so a loop that stops on `is_terminal` always gets the result. It used to return the 404, or end without the result. A task paused for approval ends the stream without a terminal event: approve it, then stream again.
- `stream_task` no longer shares the 120 s request timeout, which cut every stream at two minutes while the server keeps it open for thirty. The stream has a connect timeout and treats a minute without data (`STREAM_IDLE_TIMEOUT`; the server sends keep-alives far more often), or without response headers, as a cut, recovered as above.
- `StateTransition::is_terminal` also requires a terminal `condition` (`"terminal"` or `"Already finished"`, `TERMINAL_CONDITIONS`): a workflow state named `TASK_COMPLETED` no longer ends `stream_task` before the run's real terminal event.
- `Task::output_withheld`: true when the run's stream withheld its output.
- Two broken links in the API documentation (`HistoryClient`, `WorkflowsClient::update`).
- README: the install line asked for `wauldo = "0.1"`, which Cargo resolves to 0.1.1, without the agents client and most of what the README shows. It names the current minor version.

### Changed
- **BREAKING:** `AgentsError` is `#[non_exhaustive]` and gains `Timeout`: an exhaustive `match` needs a wildcard arm (once, so that later variants are not breaking), and code that matched a wait's timeout as `InvalidInput` must match `Timeout`. `CreateAgentRequest` and `RunOptions` gain fields: build them with `..Default::default()`. `Task` gains `decisions`.
- **BREAKING:** `TaskStatus` gains `PausedForApproval` (exhaustive `match`es must add an arm; the value used to fall into `Unknown`), and `wait_for_task` returns such a task (the caller has to act) instead of erroring on timeout. It also long-polls the server (`LONG_POLL_SECS`, 30 s per request) instead of sleeping `poll_interval` between polls, so the result arrives the moment it lands; the sleep remains as the fallback against a server that answers at once.
- **BREAKING:** `Verdict` gains `InsufficientClaims` (`"INSUFFICIENT_CLAIMS"`, an answer too short or empty to extract a claim to verify; it used to fall into `Unknown`): an exhaustive `match` must add an arm. `TaskVerification` and `Task` gain fields: code that builds them with a struct literal must set them.

## [0.17.1] - 2026-11-15

### Changed
- README and package metadata: the RapidAPI example now uses the RapidAPI host (a free-plan key does not work on `api.wauldo.com`), the free tier is 500 verifications/month, claims that could not be checked were removed, and benchmarks link to the public weekly page. No code change.

## [0.17.0] - 2026-11-15

### Added
- `rag_upload_with_options` with `RagUploadOptions { pii_policy, timeout_ms }` and the `PiiPolicy` enum (`Redact`, the server default, or `Keep` to store contact details as sent). `RagUploadRequest` is unchanged.

### Fixed
- `chat_stream`: a failure reported by the server mid-stream (`data: {"error": {...}}`) is now delivered on the channel as `Error::Server` (code `0`: the HTTP status was already 200) with the server's message and error body, and the stream stops. It was skipped, so the stream ended as an empty, apparently successful answer.

### Removed
- **Breaking:** `agents_md` and `mcp_json` are removed from agent create/update and from the agent type. The server never used them and now ignores them; requests that still send them are accepted unchanged.

## [0.16.0] - 2026-06-10

### Added
- `fact_check` / `guard`: optional `query` and `relevance_mode` args — when `query` is provided, the response carries a `relevance` block (`RelevanceResult`: `score`, `verdict` relevant/partial/off_topic, `rationale`) scoring how well the text answers the question, fully decoupled from the factual verdict. `relevance_warning` explains when relevance could not be computed. Only `relevance_mode = Some("fast")` is supported server-side today.

### Breaking
- `fact_check` / `guard` signatures gained two trailing `Option<&str>` parameters (`query`, `relevance_mode`). Existing callers must append `None, None`.

## [0.15.1] - 2026-06-07

### Fixed
- `fact_check`: `source_context` is now **required** (`impl Into<String>`, was `Option<String>`). The server rejects a missing context with 400, so the optional form always failed at runtime. Surfaced by a live dogfooding pass.

### Added
- Client-side validation on `fact_check` (empty `text` / `source_context`, invalid `mode`) — returns `Error::Validation` instead of a server round-trip.

## [0.15.0] - 2026-06-07

### Added
- `HttpClient::fact_check(text, source_context, mode)` — canonical name for the `/v1/fact-check` verify call, matching the `fact_check` naming used across the Python and TypeScript SDKs. `source_context` is now `Option<String>` (the server accepts a missing context).
- `HttpClient::verify_citation(text, sources, threshold)` — validate inline citations via `POST /v1/verify`. Closes the cross-SDK parity gap (was Python-only). New types `VerifyCitationResponse`, `CitationDetail`, `SourceChunk`, `VerifyCitationRequest`.

### Changed
- `guard()` is now a deprecated back-compat alias delegating to `fact_check()`.

## [0.14.0] - 2026-05-16

### Added
- `WorkflowsClient::update(workflow_id, req)` — edit a workflow in place via `PATCH /v1/workflows/:id`. Body shape is identical to `create` (reuses `CreateWorkflowRequest`). Server keeps id/tenant_id/created_at, bumps `updated_at` and the monotonic `version`. Closes the SDK-parity gap left when the PATCH endpoint shipped in monorepo commit `54d533b` without the matching SDK method.

## [0.13.0] - 2026-05-14

### Added
- `wauldo::workflows::WorkflowsClient` — six methods covering the Wauldo Workflow Runtime surface (`create`, `list`, `get`, `delete`, `start_run`, `get_run`) plus a `wait_for_run` polling helper. Mirrors the `/v1/workflows*` endpoints shipped in rev 63 (Phase 1+2 runtime: Task / Choice / Wait / Pass / Fail / Succeed state machines).
- Re-exports at the crate root: `WorkflowsClient`, `is_workflow_run_terminal`, `TERMINAL_WORKFLOW_STATUSES`, plus types `Workflow`, `CreateWorkflowRequest`, `WorkflowListResponse`, `StartRunResponse`, `WorkflowExecution`.

## [0.12.0] - 2026-05-08

### Added
- `AgentsClient::share_task(task_id)` → `AgentsResult<ShareResponse>` — publish a verified run as a public URL (`https://wauldo.com/r/<id>`). Idempotent ; free tier gets a 30-day TTL, paid tenants get `expires_at = None`.
- `AgentsClient::unshare_task(task_id)` → `AgentsResult<()>` — revoke a published run.
- `ShareResponse` struct (re-exported from crate root).

## [0.11.0] - 2026-05-05

### Added
- `AgentsClient::create_revision()`, `list_revisions()`, `get_revision()`, `set_active_revision()` — ECS-style immutable revisions for `custom_preset` agents (O(1) rollback, no LLM cost).
- Types: `AgentRevision`, `CreateRevisionRequest`, `CreateRevisionResponse`, `ListRevisionsResponse` (re-exported from crate root).

## [0.10.0] - 2026-04-30

### Added
- `src/agents.rs` and `src/memory.rs` modules — Tasks API client + agent memory bindings.
- `tests/guard_test.rs` integration test.

### Changed
- Repository URL migrated to github.com/wauldoai.
- README hero refresh + footer alignment with WAULDO_README_TEMPLATE.

## [0.8.0] - 2026-04-15

### Added
- Typed deserialisable structs for the Tasks API: `Task`, `TaskClaim`,
  `TaskVerification`, `StateTransition`, plus `Verdict` and
  `TaskStatus` `#[serde(rename_all)]` enums with `#[serde(other)]`
  fallback so new server-side variants don't break old SDK builds.
  All re-exported from the crate root.
- `AgentsClient::get_task(&str) -> Task`.
- `AgentsClient::cancel_task(&str)` (`DELETE /v1/tasks/:id`).
- `AgentsClient::wait_for_task(&str, Duration, Duration) -> Task` —
  async blocking poll helper; returns `AgentsError::InvalidInput` on
  timeout.
- `AgentsClient::stream_task(&str) -> impl Stream<Item = AgentsResult<StateTransition>>`
  — consumes the new SSE endpoint `GET /v1/tasks/:id/stream`. Built on
  `reqwest::bytes_stream` + `async_stream`, yields one typed
  `StateTransition` per server-sent frame.
- `Task::is_done()` and `TaskStatus::is_terminal()` helpers for UI
  state machines.
- `TaskVerification.message: Option<String>` — human-readable context
  for non-SAFE verdicts (e.g. explains `Verdict::Unverified` +
  `prompt_only` combo so callers don't have to decode the
  `confidence=1.0 / trust_score=0.0` discrepancy alone).

## [0.1.0] - 2026-03-16

### Added
- `HttpClient` — REST API client (OpenAI-compatible)
  - `chat()`, `chat_stream()`, `list_models()`, `embeddings()`
  - `rag_upload()`, `rag_query()`, `rag_ask()`
  - `orchestrate()`, `orchestrate_parallel()`
- `AgentClient` — MCP client (stdio JSON-RPC)
  - `reason()`, `extract_concepts()`, `plan_task()`
  - `chunk_document()`, `retrieve_context()`, `summarize()`
  - `search_knowledge()`, `add_to_knowledge()`
- `Conversation` — automatic chat history management
- `MockHttpClient` — offline testing without server
- Retry with exponential backoff (429/503/network errors)
- Structured logging via `tracing` crate
- Response validation with detailed error messages
- 30 unit tests + 22 doc tests
