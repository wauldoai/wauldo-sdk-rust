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

// Wauldo Deploy — standalone clients for /v1/agents, /v1/memory,
// /v1/a2a. Keep separate from HttpClient so they don't depend on
// Guard/http_types which are undergoing pre-existing modifications.
pub mod agents;
// Funnel #1 — audit log (read + export + RTBF). Same standalone shape
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

// Re-export the deployed-agents + tasks surface so callers can
// `use wauldo::AgentsClient;` without reaching into `wauldo::agents`.
pub use agents::{
    AgentListResponse, AgentPagination, AgentRevision, AgentRunResponse, AgentsClient, AgentsError,
    AgentsResult, CreateAgentRequest, CreateRevisionRequest, CreateRevisionResponse, DeployedAgent,
    ListRevisionsResponse, StateTransition, Task, TaskClaim, TaskStatus, TaskVerification,
    UpdateAgentRequest, Verdict,
};

// Re-export the workflows surface so callers can
// `use wauldo::WorkflowsClient;` without reaching into `wauldo::workflows`.
pub use workflows::{
    is_workflow_run_terminal, CreateWorkflowRequest, StartRunResponse, Workflow, WorkflowExecution,
    WorkflowListResponse, WorkflowsClient, TERMINAL_WORKFLOW_STATUSES,
};
