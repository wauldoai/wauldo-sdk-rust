//! Verification block of a task: verdict, claims, honesty signals, run
//! contract attestation, and the outcome of a fact-check race.

use serde::{Deserialize, Serialize};

/// Verification verdict returned on completed tasks (`verification.verdict`).
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Verdict {
    Safe,
    Uncertain,
    Partial,
    Block,
    Conflict,
    Unverified,
    /// The answer was too short or empty to extract a claim to verify.
    #[serde(rename = "INSUFFICIENT_CLAIMS")]
    InsufficientClaims,
    /// Fallback for server-side verdicts added after this SDK version.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskClaim {
    pub text: String,
    pub supported: bool,
    pub confidence: f64,
}

/// Verification block attached to completed tasks.
///
/// When `verification_source == "prompt_only"`, `confidence` and
/// `hallucination_rate` reflect self-consistency only — rely on
/// `verdict` + [`support_score`](Self::support_score) + `message` as authoritative.
///
/// **Naming note:** the JSON wire field is `trust_score` (kept stable
/// for backward compatibility). The public Rust method
/// [`support_score`](Self::support_score) returns the same value under
/// the public marketing name. New code should prefer that method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskVerification {
    pub verdict: Verdict,
    pub hallucination_rate: f64,
    pub confidence: f64,
    pub trust_score: f64,
    pub verification_source: String,
    #[serde(default)]
    pub claims: Vec<TaskClaim>,
    #[serde(default)]
    pub verification_retries: u32,
    /// Human-readable context for non-SAFE verdicts.
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub sources_cited: Vec<usize>,
    #[serde(default)]
    pub stripped_claims: Vec<String>,
    /// Checks of the final answer itself (empty answer, raw JSON in place of
    /// prose). `None` when they were not run.
    #[serde(default)]
    pub honesty_signals: Option<HonestySignals>,
    /// Whether the run honoured the agent's contract (allowed tools,
    /// transitions, output schema, forbidden behaviours). Independent of
    /// `verdict`: an answer can be `SAFE` and its run `DEGRADED`. `None` for
    /// runs without a contract.
    #[serde(default)]
    pub contract_attestation: Option<ContractAttestation>,
    /// Why the run failed before reaching a verdict; `None` on success.
    #[serde(default)]
    pub failure_reason: Option<String>,
    /// Stage of the pipeline where the run failed; `None` on success.
    #[serde(default)]
    pub failure_stage: Option<String>,
}

impl TaskVerification {
    /// Public name for [`trust_score`](Self::trust_score).
    ///
    /// Returns the fraction of claims (0-1) that are supported by the
    /// provided sources. Identical to the `trust_score` field — the
    /// JSON wire format keeps `trust_score` for backward compatibility,
    /// but the public marketing name is *support score*. Prefer this
    /// method in new code.
    #[inline]
    pub fn support_score(&self) -> f64 {
        self.trust_score
    }
}

/// Checks of the final answer itself ([`TaskVerification::honesty_signals`]).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HonestySignals {
    /// The answer is empty or says nothing (e.g. "no answer").
    #[serde(default)]
    pub is_non_answer: bool,
    /// The answer is a raw JSON dump with no readable prose.
    #[serde(default)]
    pub json_garbage: bool,
    /// Which checks fired, so a client can say why the answer was flagged.
    #[serde(default)]
    pub matched_patterns: Vec<String>,
}

/// Verdict of a [`ContractAttestation`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContractVerdict {
    Success,
    Degraded,
    Fail,
    /// Fallback for verdicts added after this SDK version.
    #[serde(other)]
    Unknown,
}

/// One way a run broke its agent's contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContractViolation {
    DisallowedTool {
        state: String,
        tool: String,
    },
    UnknownState {
        state: String,
    },
    TransitionViolation {
        from: String,
        to: String,
    },
    OutputSchemaMismatch {
        format_key: String,
        error: String,
    },
    ForbiddenPatternMatched {
        pattern: String,
    },
    Misconfigured {
        reason: String,
    },
    /// Fallback for kinds added after this SDK version.
    #[serde(other)]
    Unknown,
}

/// Did the run honour its agent's contract
/// ([`TaskVerification::contract_attestation`])?
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractAttestation {
    pub matches: bool,
    #[serde(default)]
    pub violations: Vec<ContractViolation>,
    pub verdict: ContractVerdict,
    #[serde(default)]
    pub replay_token: String,
}

/// One model's result in a fact-check race.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaceCandidate {
    pub model: String,
    #[serde(default)]
    pub score: Option<f32>,
    #[serde(default)]
    pub timed_out: bool,
}

/// Outcome of a fact-check race ([`super::Task::race_result`]): the winning
/// model and every candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaceResult {
    pub winner: String,
    pub winner_score: f32,
    #[serde(default)]
    pub candidates: Vec<RaceCandidate>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verdict_insufficient_claims_is_named() {
        let v: Verdict = serde_json::from_str("\"INSUFFICIENT_CLAIMS\"").expect("verdict");
        assert_eq!(v, Verdict::InsufficientClaims);
        assert_eq!(
            serde_json::to_string(&Verdict::InsufficientClaims).expect("ser"),
            "\"INSUFFICIENT_CLAIMS\""
        );
        let later: Verdict = serde_json::from_str("\"SOMETHING_NEW\"").expect("verdict");
        assert_eq!(later, Verdict::Unknown);
    }

    #[test]
    fn test_verification_reads_signals_attestation_and_failure() {
        let v: TaskVerification = serde_json::from_value(serde_json::json!({
            "verdict": "SAFE",
            "hallucination_rate": 0.0,
            "confidence": 0.9,
            "trust_score": 1.0,
            "verification_source": "source_documents",
            "honesty_signals": {"is_non_answer": true, "json_garbage": false},
            "contract_attestation": {
                "matches": false,
                "verdict": "DEGRADED",
                "replay_token": "rt",
                "violations": [
                    {"kind": "disallowed_tool", "state": "PLAN", "tool": "web_search"},
                    {"kind": "a_kind_from_later"}
                ]
            },
            "failure_reason": "LLM provider timeout",
            "failure_stage": "fact_check"
        }))
        .expect("verification");
        assert!(v.honesty_signals.as_ref().expect("signals").is_non_answer);
        let att = v.contract_attestation.expect("attestation");
        assert_eq!(att.verdict, ContractVerdict::Degraded);
        assert_eq!(
            att.violations[0],
            ContractViolation::DisallowedTool {
                state: "PLAN".into(),
                tool: "web_search".into()
            }
        );
        assert_eq!(att.violations[1], ContractViolation::Unknown);
        assert_eq!(v.failure_reason.as_deref(), Some("LLM provider timeout"));
        assert_eq!(v.failure_stage.as_deref(), Some("fact_check"));
    }

    #[test]
    fn test_verification_without_new_fields_still_parses() {
        let v: TaskVerification = serde_json::from_value(serde_json::json!({
            "verdict": "UNVERIFIED",
            "hallucination_rate": 0.0,
            "confidence": 0.0,
            "trust_score": 0.0,
            "verification_source": "prompt_only"
        }))
        .expect("verification");
        assert!(v.honesty_signals.is_none());
        assert!(v.contract_attestation.is_none());
        assert!(v.failure_stage.is_none());
    }
}
