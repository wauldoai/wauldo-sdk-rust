# Changelog

All notable changes to the Wauldo Rust SDK.

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
