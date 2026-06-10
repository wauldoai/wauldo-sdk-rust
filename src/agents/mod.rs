//! Agents API client — Wauldo Deploy deployed-agent registry.
//!
//! Standalone client that talks to the `/v1/agents` endpoints. Designed
//! to work alongside `HttpClient` without depending on its types —
//! construct `AgentsClient` with the same base URL + API key you'd pass
//! to `HttpClient`.
//!
//! # Example
//! ```no_run
//! use wauldo::agents::{AgentsClient, CreateAgentRequest};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let agents = AgentsClient::new("http://localhost:3000")
//!     .with_api_key("sk-...")
//!     .with_tenant("my-org");
//! let agent = agents.create(CreateAgentRequest {
//!     name: "sdr-bot".into(),
//!     wauldo_toml: "[agent]\nname = 'sdr-bot'\n[model]\nprovider = 'openrouter'\nname = 'qwen'".into(),
//!     description: "Outbound sales".into(),
//!     agents_md: None,
//!     mcp_json: None,
//!     preset: None,
//! }).await?;
//! let run = agents.run(&agent.id, "Qualify acme.com", None).await?;
//! println!("task: {}", run.task_id);
//! # Ok(())
//! # }
//! ```

mod client;
mod types;

pub use client::AgentsClient;
pub(crate) use types::bounded_read;
pub use types::{
    A2aResponse, AgentListResponse, AgentPagination, AgentRevision, AgentRunResponse, AgentsError,
    AgentsResult, CreateAgentRequest, CreateRevisionRequest, CreateRevisionResponse, DeployedAgent,
    ListRevisionsResponse, ShareResponse, StateTransition, Task, TaskClaim, TaskStatus,
    TaskVerification, UpdateAgentRequest, Verdict, MAX_RESPONSE_SIZE,
};
