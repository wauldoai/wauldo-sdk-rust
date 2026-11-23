//! Error types for Wauldo SDK

use thiserror::Error;

use crate::agents::AgentsError;

/// Result type alias for SDK operations
pub type Result<T> = std::result::Result<T, Error>;

/// SDK error types
#[derive(Debug, Error)]
pub enum Error {
    /// Connection to MCP server failed
    #[error("Connection error: {0}")]
    Connection(String),

    /// Server returned an error response
    #[error("Server error [{code}]: {message}")]
    Server {
        code: i32,
        message: String,
        data: Option<serde_json::Value>,
    },

    /// Input validation failed
    #[error("Validation error: {message}")]
    Validation {
        message: String,
        field: Option<String>,
    },

    /// Operation timed out
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Tool not found
    #[error("Tool not found: {0}")]
    ToolNotFound(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// Create a validation error
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
            field: None,
        }
    }

    /// Create a validation error with field name
    pub fn validation_field(message: impl Into<String>, field: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
            field: Some(field.into()),
        }
    }

    /// Create a connection error
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection(message.into())
    }

    /// Create a server error
    pub fn server(code: i32, message: impl Into<String>) -> Self {
        Self::Server {
            code,
            message: message.into(),
            data: None,
        }
    }
}

/// So `?` works on the agents, memory and workflows clients in a function
/// that returns [`Result`].
impl From<AgentsError> for Error {
    fn from(e: AgentsError) -> Self {
        match e {
            AgentsError::Status { status, body } => Self::Server {
                code: i32::from(status),
                message: body,
                data: None,
            },
            AgentsError::Http(e) if e.is_timeout() => {
                Self::Timeout(format!("Request timed out: {e}"))
            }
            AgentsError::Http(e) => Self::connection(format!("Request failed: {e}")),
            AgentsError::Json(e) => Self::Json(e),
            AgentsError::InvalidInput(message) => Self::validation(message),
            e @ AgentsError::Timeout { .. } => Self::Timeout(e.to_string()),
            e @ AgentsError::BodyTooLarge(_) => Self::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::agents::{AgentsError, TaskStatus};

    #[test]
    fn test_error_from_agents_status_is_server_error() {
        let e: Error = AgentsError::Status {
            status: 401,
            body: r#"{"error":"bad key"}"#.into(),
        }
        .into();
        match e {
            Error::Server { code, message, .. } => {
                assert_eq!(code, 401);
                assert_eq!(message, r#"{"error":"bad key"}"#);
            }
            other => panic!("expected Error::Server, got {other:?}"),
        }
    }

    #[test]
    fn test_error_from_agents_other_variants() {
        let timeout: Error = AgentsError::Timeout {
            task_id: "t1".into(),
            last_status: TaskStatus::Running,
            waited: std::time::Duration::from_secs(5),
        }
        .into();
        assert!(matches!(timeout, Error::Timeout(ref m) if m.contains("t1")));
        let invalid: Error = AgentsError::InvalidInput("empty input".into()).into();
        assert!(
            matches!(invalid, Error::Validation { ref message, .. } if message == "empty input")
        );
        let big: Error = AgentsError::BodyTooLarge(10).into();
        assert!(matches!(big, Error::Io(_)));
    }

    /// A stubbed 401 from the agents client reaches a `wauldo::Result` caller
    /// as `Error::Server`, with the status and the server's body.
    #[tokio::test]
    async fn test_error_agents_client_http_status_is_server_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(401).set_body_string(r#"{"error":"bad key"}"#))
            .mount(&server)
            .await;
        async fn read(url: &str) -> crate::Result<crate::agents::Task> {
            let client = crate::agents::AgentsClient::new(url).with_max_retries(0);
            Ok(client.get_task("t1").await?)
        }
        match read(&server.uri()).await {
            Err(Error::Server { code, message, .. }) => {
                assert_eq!(code, 401);
                assert_eq!(message, r#"{"error":"bad key"}"#);
            }
            other => panic!("expected Error::Server, got {other:?}"),
        }
    }

    /// The point of the conversion: `?` on an agents call inside a function
    /// that returns `wauldo::Result`.
    #[test]
    fn test_error_question_mark_on_agents_result() {
        fn call() -> crate::Result<()> {
            let failed: crate::agents::AgentsResult<()> = Err(AgentsError::Status {
                status: 429,
                body: "slow down".into(),
            });
            failed?;
            Ok(())
        }
        assert!(matches!(call(), Err(Error::Server { code: 429, .. })));
    }
}
