//! Agents API client — Wauldo Deploy deployed-agent registry.
//!
//! Standalone client that talks to the `/v1/agents` endpoints. Designed
//! to work alongside `HttpClient` without depending on its types —
//! construct `AgentsClient` with the same base URL + API key you'd pass
//! to `HttpClient`.
//!
//! # Example
//! ```no_run
//! use wauldo::agents::{AgentsClient, CreateAgentRequest, DEFAULT_WAIT_TIMEOUT};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let agents = AgentsClient::new("http://localhost:3000")
//!     .with_api_key("sk-...")
//!     .with_tenant("my-org");
//! let agent = agents.create(CreateAgentRequest {
//!     name: "sdr-bot".into(),
//!     wauldo_toml: "[agent]\nname = 'sdr-bot'\n[model]\nprovider = 'openrouter'\nname = 'qwen'".into(),
//!     description: "Outbound sales".into(),
//!     ..Default::default()
//! }).await?;
//! // One call: the server holds the response while the run completes.
//! let task = agents
//!     .run_and_wait(&agent.id, "Qualify acme.com", Default::default(), DEFAULT_WAIT_TIMEOUT)
//!     .await?;
//! println!("{} — verdict {:?}", task.result.unwrap_or_default(), task.verification.map(|v| v.verdict));
//! # Ok(())
//! # }
//! ```

mod client;
// Next to `client` on purpose: rustdoc lists `AgentsClient`'s methods in
// module order, and these two continue its impl.
mod runs;
mod tasks;

mod hitl;
mod stream;
mod task_error;
mod types;
mod verification;

pub use client::AgentsClient;
pub use client::{RETRY_STATUSES, RETRY_STATUSES_IDEMPOTENT};
pub use hitl::{HitlResponse, TaskDecision, MAX_HITL_REASON_CHARS, PAUSED_EXPIRED_ERROR};
pub use stream::{ALREADY_FINISHED_CONDITION, STREAM_FALLBACK_TIMEOUT, STREAM_IDLE_TIMEOUT};
pub use task_error::TaskErrorInfo;
pub(crate) use types::bounded_read;
pub use types::{
    A2aResponse, AgentListResponse, AgentPagination, AgentRevision, AgentRunResponse, AgentsError,
    AgentsResult, CreateAgentRequest, CreateRevisionRequest, CreateRevisionResponse, DeployedAgent,
    ListRevisionsResponse, RunOptions, ShareResponse, StateTransition, Task, TaskStatus,
    TerminalPayload, UpdateAgentRequest, DEFAULT_WAIT_TIMEOUT, LONG_POLL_SECS, MAX_RESPONSE_SIZE,
    MAX_WAIT_SECS, STREAM_LAGGED, TERMINAL_CONDITIONS, TERMINAL_STATE_NAMES, WITHHELD_RESULT,
};
pub use verification::{
    ContractAttestation, ContractVerdict, ContractViolation, HonestySignals, RaceCandidate,
    RaceResult, TaskClaim, TaskVerification, Verdict,
};
