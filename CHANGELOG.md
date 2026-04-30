# Changelog

All notable changes to the Wauldo Rust SDK.

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
