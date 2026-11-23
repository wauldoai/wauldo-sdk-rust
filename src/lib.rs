//! Wauldo Rust SDK
//!
//! Provides two client interfaces:
//! - `AgentClient` — MCP server client (stdio JSON-RPC) for reasoning, planning, tools
//! - `HttpClient` — REST API client (OpenAI-compatible) for chat, embeddings, RAG, orchestrator
//!
//! # Quick Start (HTTP API)
//!
//! ```rust,no_run
//! use wauldo::{HttpClient, ChatRequest, ChatMessage, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = HttpClient::localhost()?;
//!
//!     let models = client.list_models().await?;
//!     println!("Models: {:?}", models.data.iter().map(|m| &m.id).collect::<Vec<_>>());
//!
//!     let req = ChatRequest::new("qwen2.5:7b", vec![ChatMessage::user("Hello!")]);
//!     let resp = client.chat(req).await?;
//!     println!("{}", resp.choices[0].message.content.as_deref().unwrap_or(""));
//!
//!     Ok(())
//! }
//! ```

mod client;
pub mod conversation;
mod error;
pub mod http_client;
pub mod http_config;
mod http_request;
pub mod http_types;
pub mod mock_client;
mod retry;
mod sse_parser;
mod transport;
mod types;
pub mod upload_file;

// Wauldo Deploy — standalone clients for /v1/agents, /v1/memory,
// /v1/a2a. Keep separate from HttpClient so they don't depend on
// Guard/http_types which are undergoing pre-existing modifications.
pub mod agents;
// Audit history (read + export + right to be forgotten). Same standalone shape
// as memory and agents — no coupling to HttpClient.
pub mod history;
pub mod memory;
// Workflow Runtime — state-machine workflows (Step Functions style).
pub mod workflows;

pub use client::AgentClient;
pub use conversation::Conversation;
pub use error::{Error, Result};
pub use http_client::HttpClient;
pub use http_config::HttpConfig;
pub use http_types::*;
pub use mock_client::MockHttpClient;
pub use types::*;
pub use upload_file::{
    DocumentQuality, UploadFileOptions, UploadFileResponse, MAX_UPLOAD_FILE_SIZE,
};

// Re-export the deployed-agents + tasks surface so callers can
// `use wauldo::AgentsClient;` without reaching into `wauldo::agents`.
pub use agents::{
    A2aResponse, AgentListResponse, AgentPagination, AgentRevision, AgentRunResponse, AgentsClient,
    AgentsError, AgentsResult, ContractAttestation, ContractVerdict, ContractViolation,
    CreateAgentRequest, CreateRevisionRequest, CreateRevisionResponse, DeployedAgent, HitlResponse,
    HonestySignals, ListRevisionsResponse, RaceCandidate, RaceResult, RunOptions, ShareResponse,
    StateTransition, Task, TaskClaim, TaskDecision, TaskErrorInfo, TaskStatus, TaskVerification,
    TerminalPayload, UpdateAgentRequest, Verdict, ALREADY_FINISHED_CONDITION, DEFAULT_WAIT_TIMEOUT,
    LONG_POLL_SECS, MAX_HITL_REASON_CHARS, MAX_WAIT_SECS, PAUSED_EXPIRED_ERROR, RETRY_STATUSES,
    RETRY_STATUSES_IDEMPOTENT, STREAM_FALLBACK_TIMEOUT, STREAM_IDLE_TIMEOUT, STREAM_LAGGED,
    TERMINAL_CONDITIONS, TERMINAL_STATE_NAMES, WITHHELD_RESULT,
};

// Re-export the workflows surface so callers can
// `use wauldo::WorkflowsClient;` without reaching into `wauldo::workflows`.
pub use workflows::{
    is_workflow_run_terminal, CreateWorkflowRequest, StartRunResponse, Workflow, WorkflowExecution,
    WorkflowListResponse, WorkflowsClient, TERMINAL_WORKFLOW_STATUSES,
};
