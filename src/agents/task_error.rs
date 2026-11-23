//! What a failed task's `error` says, parsed.
//!
//! The server writes a failed task's `error` as `code: message`: the code is
//! stable, for programs; the message is for people. [`TaskErrorInfo`] splits
//! the two.

use serde::Serialize;

use super::types::{Task, TerminalPayload};

/// A task's `error`, split into its stable `code` and its `message`.
///
/// A value, not an error type. `code` is kept as sent, even one this SDK
/// version does not know (see [`TaskErrorInfo::KNOWN_CODES`]); it is `None`
/// when the error does not start with a code (the 7-day approval
/// cancellation, for one), and `message` is then the whole error. New codes
/// may be added: handle an unknown one like [`TaskErrorInfo::RUN_FAILED`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TaskErrorInfo {
    pub code: Option<String>,
    pub message: String,
}

impl TaskErrorInfo {
    /// The agent names a workflow preset that does not exist. Fix the agent.
    pub const INVALID_PRESET: &'static str = "invalid_preset";
    /// The agent's inline workflow could not be loaded. Fix the agent.
    pub const INVALID_AGENT_CONFIG: &'static str = "invalid_agent_config";
    /// The model service is saturated. Retry in a few minutes.
    pub const RATE_LIMITED: &'static str = "rate_limited";
    /// The run did not finish within its time limit. Retry, or simplify the request.
    pub const TIMEOUT: &'static str = "timeout";
    /// The model service is temporarily unavailable. Retry later.
    pub const MODEL_UNAVAILABLE: &'static str = "model_unavailable";
    /// The run failed for another reason. Retry later.
    pub const RUN_FAILED: &'static str = "run_failed";
    /// Every code this SDK version knows.
    pub const KNOWN_CODES: [&'static str; 6] = [
        Self::INVALID_PRESET,
        Self::INVALID_AGENT_CONFIG,
        Self::RATE_LIMITED,
        Self::TIMEOUT,
        Self::MODEL_UNAVAILABLE,
        Self::RUN_FAILED,
    ];

    /// Parse a task's `error` (also the `error` of a stream's terminal
    /// payload). Never fails: text without a code is kept whole in `message`.
    pub fn parse(raw: &str) -> Self {
        if let Some((code, message)) = raw.split_once(": ") {
            if is_code(code) {
                return Self {
                    code: Some(code.to_string()),
                    message: message.to_string(),
                };
            }
        }
        Self {
            code: None,
            message: raw.to_string(),
        }
    }

    /// `code`, as a `&str` (`None` when the error has no code).
    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }
}

/// `[a-z][a-z0-9_]*`: the shape of the server's codes.
fn is_code(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn parse_opt(raw: Option<&str>) -> Option<TaskErrorInfo> {
    raw.filter(|s| !s.is_empty()).map(TaskErrorInfo::parse)
}

impl Task {
    /// [`Task::error`] split into its stable code and its message; `None`
    /// when the task has no error. `error` itself is unchanged.
    pub fn error_info(&self) -> Option<TaskErrorInfo> {
        parse_opt(self.error.as_deref())
    }
}

impl TerminalPayload {
    /// [`TerminalPayload::error`] split into its stable code and its message.
    pub fn error_info(&self) -> Option<TaskErrorInfo> {
        parse_opt(self.error.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::TaskErrorInfo;
    use crate::agents::Task;

    #[test]
    fn test_task_error_info_known_codes() {
        for code in TaskErrorInfo::KNOWN_CODES {
            let info = TaskErrorInfo::parse(&format!("{code}: Something to read."));
            assert_eq!(info.code(), Some(code));
            assert_eq!(info.message, "Something to read.");
        }
    }

    #[test]
    fn test_task_error_info_unknown_code_kept() {
        let info = TaskErrorInfo::parse("quota_exceeded: Limit: 10 runs");
        assert_eq!(info.code(), Some("quota_exceeded"));
        assert_eq!(info.message, "Limit: 10 runs");
        assert!(!TaskErrorInfo::KNOWN_CODES.contains(&"quota_exceeded"));
    }

    #[test]
    fn test_task_error_info_without_code_keeps_whole_error() {
        for raw in [
            "Cancelled: no approval decision within 7 days",
            "internal error: the run did not complete",
            "plain text",
            ": no code",
        ] {
            let info = TaskErrorInfo::parse(raw);
            assert_eq!(info.code(), None, "{raw}");
            assert_eq!(info.message, raw);
        }
    }

    #[test]
    fn test_task_error_info_on_task() {
        let task: Task = serde_json::from_value(serde_json::json!({
            "task_id": "t1",
            "status": "failed",
            "error": "invalid_preset: Unknown preset: my_presett",
        }))
        .expect("task parses");
        let info = task.error_info().expect("failed task has an error");
        assert_eq!(info.code(), Some(TaskErrorInfo::INVALID_PRESET));
        assert_eq!(info.message, "Unknown preset: my_presett");
        // The constants work as patterns (the README's match).
        assert!(matches!(
            info.code(),
            Some(TaskErrorInfo::INVALID_PRESET | TaskErrorInfo::INVALID_AGENT_CONFIG)
        ));
        assert_eq!(
            task.error.as_deref(),
            Some("invalid_preset: Unknown preset: my_presett")
        );

        let done: Task =
            serde_json::from_value(serde_json::json!({"task_id": "t2", "status": "completed"}))
                .expect("task parses");
        assert!(done.error_info().is_none());
        let empty: Task = serde_json::from_value(
            serde_json::json!({"task_id": "t3", "status": "failed", "error": ""}),
        )
        .expect("task parses");
        assert!(empty.error_info().is_none());
    }
}
