//! HTTP client for Wauldo REST API (OpenAI-compatible)

use crate::conversation::Conversation;
use crate::error::{Error, Result};
use crate::http_config::{HttpConfig, STREAM_IDLE_TIMEOUT};
use crate::http_types::*;
use crate::retry::RetryConfig;
use crate::sse_parser::parse_sse_stream;
use reqwest::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE};
use tokio::sync::mpsc;

/// HTTP client for the Wauldo REST API
#[derive(Clone)]
pub struct HttpClient {
    pub(crate) client: reqwest::Client,
    /// For `chat_stream`: no total timeout, only `STREAM_IDLE_TIMEOUT`.
    pub(crate) stream_client: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) retry_config: RetryConfig,
    pub(crate) on_request: Option<fn(&str, &str)>,
    pub(crate) on_response: Option<fn(u16, u64)>,
    pub(crate) on_error: Option<fn(&Error)>,
}

impl HttpClient {
    /// Create client with full configuration
    pub fn new(config: HttpConfig) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "application/json".parse().expect("valid header"),
        );
        if let Some(key) = &config.api_key {
            let val = format!("Bearer {}", key)
                .parse()
                .map_err(|e| Error::connection(format!("Invalid API key: {}", e)))?;
            headers.insert(AUTHORIZATION, val);
        }
        for (name, value) in &config.extra_headers {
            let header_name: reqwest::header::HeaderName = name
                .parse()
                .map_err(|e| Error::connection(format!("Invalid header name '{}': {}", name, e)))?;
            let header_value = value.parse().map_err(|e| {
                Error::connection(format!("Invalid header value '{}': {}", value, e))
            })?;
            headers.insert(header_name, header_value);
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .default_headers(headers.clone())
            .build()
            .map_err(|e| Error::connection(format!("HTTP client error: {}", e)))?;
        // A total timeout would cut a stream that is still alive.
        let stream_client = reqwest::Client::builder()
            .read_timeout(STREAM_IDLE_TIMEOUT)
            .default_headers(headers)
            .build()
            .map_err(|e| Error::connection(format!("HTTP client error: {}", e)))?;
        Ok(Self {
            client,
            stream_client,
            base_url: config.base_url,
            retry_config: RetryConfig {
                max_retries: config.max_retries,
                backoff_ms: config.retry_backoff_ms,
            },
            on_request: config.on_request,
            on_response: config.on_response,
            on_error: config.on_error,
        })
    }

    /// Create client pointing to localhost:3000
    pub fn localhost() -> Result<Self> {
        Self::new(HttpConfig::default())
    }

    /// Create client with custom base URL
    pub fn with_url(base_url: impl Into<String>) -> Result<Self> {
        Self::new(HttpConfig {
            base_url: base_url.into(),
            ..Default::default()
        })
    }

    /// List available models -- GET /v1/models
    pub async fn list_models(&self) -> Result<ModelList> {
        self.get("/v1/models").await
    }

    /// Chat completion (non-streaming) -- POST /v1/chat/completions
    pub async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        if request.messages.is_empty() {
            return Err(Error::validation("messages cannot be empty"));
        }
        self.chat_with_timeout(request, None).await
    }

    /// Chat completion with an optional per-request timeout override
    pub async fn chat_with_timeout(
        &self,
        request: ChatRequest,
        timeout_ms: Option<u64>,
    ) -> Result<ChatResponse> {
        let mut req = request;
        req.stream = Some(false);
        self.post_with_timeout("/v1/chat/completions", &req, timeout_ms)
            .await
    }

    /// Chat completion with SSE streaming -- POST /v1/chat/completions
    ///
    /// The stream is not bounded by `timeout_secs`. If nothing arrives for
    /// 45 s (not even a keep-alive), the channel yields `Error::Timeout`
    /// and closes.
    pub async fn chat_stream(
        &self,
        request: ChatRequest,
    ) -> Result<mpsc::Receiver<Result<String>>> {
        let mut req = request;
        req.stream = Some(true);
        if let Some(hook) = self.on_request {
            hook("POST", "/v1/chat/completions");
        }
        let url = format!("{}/v1/chat/completions", self.base_url);
        let start = std::time::Instant::now();
        let resp = self
            .stream_client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| {
                let err = if e.is_timeout() {
                    Error::Timeout(format!("Request timed out: {}", e))
                } else {
                    Error::connection(format!("Request failed: {}", e))
                };
                if let Some(hook) = self.on_error {
                    hook(&err);
                }
                err
            })?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            let message = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("error")?.get("message")?.as_str().map(String::from))
                .unwrap_or(body);
            let err = Error::server(status as i32, message);
            if let Some(hook) = self.on_error {
                hook(&err);
            }
            return Err(err);
        }
        if let Some(hook) = self.on_response {
            hook(status, start.elapsed().as_millis() as u64);
        }
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            if let Err(e) = parse_sse_stream(resp, tx).await {
                tracing::error!("SSE stream error: {}", e);
            }
        });
        Ok(rx)
    }

    /// Generate embeddings -- POST /v1/embeddings
    pub async fn embeddings(
        &self,
        input: EmbeddingInput,
        model: impl Into<String>,
    ) -> Result<EmbeddingResponse> {
        self.post(
            "/v1/embeddings",
            &EmbeddingRequest {
                input,
                model: model.into(),
            },
        )
        .await
    }

    /// Upload document for RAG -- POST /v1/upload
    pub async fn rag_upload(
        &self,
        content: impl Into<String>,
        filename: Option<String>,
    ) -> Result<RagUploadResponse> {
        self.rag_upload_with_timeout(content, filename, None).await
    }

    /// Upload document for RAG with an optional per-request timeout override
    pub async fn rag_upload_with_timeout(
        &self,
        content: impl Into<String>,
        filename: Option<String>,
        timeout_ms: Option<u64>,
    ) -> Result<RagUploadResponse> {
        self.rag_upload_with_options(
            content,
            filename,
            RagUploadOptions {
                timeout_ms,
                ..Default::default()
            },
        )
        .await
    }

    /// Upload document for RAG with options: the PII policy
    /// ([`PiiPolicy::Keep`] stores contact details as sent) and a timeout.
    ///
    /// The request is 10 MB at most, counted in UTF-8 bytes with the file
    /// name: a larger one is refused with [`Error::Validation`] before
    /// sending (this applies to [`Self::rag_upload`] too).
    pub async fn rag_upload_with_options(
        &self,
        content: impl Into<String>,
        filename: Option<String>,
        options: RagUploadOptions,
    ) -> Result<RagUploadResponse> {
        let body = RagUploadBody {
            content: content.into(),
            filename,
            pii_policy: options.pii_policy,
        };
        crate::upload_file::check_text_upload_size(&body)?;
        self.post_with_timeout("/v1/upload", &body, options.timeout_ms)
            .await
    }

    /// Query RAG -- POST /v1/query
    pub async fn rag_query(
        &self,
        query: impl Into<String>,
        top_k: Option<usize>,
    ) -> Result<RagQueryResponse> {
        let options = RagQueryOptions {
            top_k,
            ..Default::default()
        };
        self.rag_query_with_options(query, options).await
    }

    /// Query RAG with debug mode — returns retrieval funnel diagnostics
    pub async fn rag_query_debug(
        &self,
        query: impl Into<String>,
        top_k: Option<usize>,
    ) -> Result<RagQueryResponse> {
        let options = RagQueryOptions {
            top_k,
            debug: true,
            ..Default::default()
        };
        self.rag_query_with_options(query, options).await
    }

    /// Query RAG with every option -- POST /v1/query: `top_k`, `debug` and
    /// `quality_mode` (`"fast"`, `"balanced"` or `"premium"`).
    pub async fn rag_query_with_options(
        &self,
        query: impl Into<String>,
        options: RagQueryOptions,
    ) -> Result<RagQueryResponse> {
        self.post("/v1/query", &rag_query_body(query.into(), options))
            .await
    }

    /// Execute orchestrator (best agent) -- POST /v1/orchestrator/execute
    pub async fn orchestrate(&self, prompt: impl Into<String>) -> Result<OrchestratorResponse> {
        self.post(
            "/v1/orchestrator/execute",
            &OrchestratorRequest {
                prompt: prompt.into(),
            },
        )
        .await
    }

    /// Execute parallel swarm (all specialists) -- POST /v1/orchestrator/parallel
    pub async fn orchestrate_parallel(
        &self,
        prompt: impl Into<String>,
    ) -> Result<OrchestratorResponse> {
        self.post(
            "/v1/orchestrator/parallel",
            &OrchestratorRequest {
                prompt: prompt.into(),
            },
        )
        .await
    }

    /// Verify text claims against source context -- POST /v1/fact-check
    ///
    /// Guard is a hallucination firewall: checks whether LLM output is
    /// supported by source documents. Blocks wrong answers before users see them.
    ///
    /// When `query` is provided, the response carries a `relevance` block
    /// scoring how well `text` addresses the question — fully decoupled from
    /// the factual verdict (verified + off_topic is a valid combination).
    /// `relevance_mode`: only `"fast"` (embedding cosine) is currently
    /// supported server-side; requires `query`.
    ///
    /// # Example
    /// ```no_run
    /// # async fn example() -> wauldo::Result<()> {
    /// # let client = wauldo::HttpClient::localhost()?;
    /// let result = client.guard(
    ///     "Returns accepted within 60 days",
    ///     "Our return policy: 14 days.",
    ///     None,
    ///     Some("What is the return window?"),
    ///     None,
    /// ).await?;
    /// if result.is_blocked() {
    ///     println!("Hallucination caught: {:?}", result.claims[0].reason);
    /// }
    /// if let Some(relevance) = &result.relevance {
    ///     println!("Relevance: {} ({:.2})", relevance.verdict, relevance.score);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn fact_check(
        &self,
        text: impl Into<String>,
        source_context: impl Into<String>,
        mode: Option<&str>,
        query: Option<&str>,
        relevance_mode: Option<&str>,
    ) -> Result<GuardResponse> {
        let text = text.into();
        let source_context = source_context.into();
        if text.is_empty() {
            return Err(Error::validation_field("text cannot be empty", "text"));
        }
        if source_context.is_empty() {
            return Err(Error::validation_field(
                "source_context is required for verification",
                "source_context",
            ));
        }
        if let Some(m) = mode {
            if !matches!(m, "lexical" | "hybrid" | "semantic") {
                return Err(Error::validation_field(
                    "mode must be one of: lexical, hybrid, semantic",
                    "mode",
                ));
            }
        }
        if relevance_mode.is_some() && query.is_none() {
            return Err(Error::validation_field(
                "relevance_mode requires query to be provided",
                "relevance_mode",
            ));
        }
        self.post(
            "/v1/fact-check",
            &GuardRequest {
                text,
                source_context,
                mode: mode.map(|m| m.to_string()),
                query: query.map(|q| q.to_string()),
                relevance_mode: relevance_mode.map(|r| r.to_string()),
            },
        )
        .await
    }

    /// Alias for [`HttpClient::fact_check`], kept for parity with the other
    /// SDKs (all expose `guard`).
    pub async fn guard(
        &self,
        text: impl Into<String>,
        source_context: impl Into<String>,
        mode: Option<&str>,
        query: Option<&str>,
        relevance_mode: Option<&str>,
    ) -> Result<GuardResponse> {
        self.fact_check(text, source_context, mode, query, relevance_mode)
            .await
    }

    /// Validate inline citations against sources -- POST /v1/verify
    pub async fn verify_citation(
        &self,
        text: impl Into<String>,
        sources: Option<Vec<SourceChunk>>,
        threshold: Option<f64>,
    ) -> Result<VerifyCitationResponse> {
        self.post(
            "/v1/verify",
            &VerifyCitationRequest {
                text: text.into(),
                sources,
                threshold,
            },
        )
        .await
    }

    /// Create a stateful conversation helper using this client
    pub fn conversation(&self) -> Conversation {
        Conversation::new(self.clone())
    }

    /// Upload text into RAG and immediately query it -- convenience one-shot
    pub async fn rag_ask(&self, question: &str, text: &str) -> Result<String> {
        self.rag_upload(text, None).await?;
        Ok(self.rag_query(question, None).await?.answer)
    }
}

/// Wire body of `POST /v1/query`: `debug` is sent only when set.
fn rag_query_body(query: String, options: RagQueryOptions) -> RagQueryRequest {
    RagQueryRequest {
        query,
        top_k: options.top_k,
        debug: options.debug.then_some(true),
        stream: None,
        quality_mode: options.quality_mode,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> HttpClient {
        HttpClient::new(HttpConfig::new("http://localhost:3000").with_api_key("k")).unwrap()
    }

    #[tokio::test]
    async fn rag_query_with_options_sends_quality_mode_and_debug() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/query"))
            .and(body_json(serde_json::json!({
                "query": "What are the payment terms?",
                "top_k": 3,
                "debug": true,
                "quality_mode": "premium",
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"answer": "30 days", "sources": []})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = HttpClient::new(HttpConfig::new(server.uri())).unwrap();
        let options = RagQueryOptions {
            top_k: Some(3),
            debug: true,
            quality_mode: Some("premium".into()),
        };
        let resp = client
            .rag_query_with_options("What are the payment terms?", options)
            .await
            .expect("query");
        assert_eq!(resp.answer, "30 days");
    }

    #[test]
    fn rag_query_body_leaves_unset_options_out() {
        let body = rag_query_body("q".into(), RagQueryOptions::default());
        let json = serde_json::to_value(&body).expect("serializes");
        assert_eq!(json, serde_json::json!({"query": "q"}));
    }

    #[tokio::test]
    async fn fact_check_empty_source_context_is_validation_error() {
        // Validation fires before any network call.
        let err = client()
            .fact_check("Some claim.", "", None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Validation { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn fact_check_invalid_mode_is_validation_error() {
        let err = client()
            .fact_check("Some claim.", "ctx", Some("banana"), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Validation { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn fact_check_relevance_mode_without_query_is_validation_error() {
        let err = client()
            .fact_check("Some claim.", "ctx", None, None, Some("fast"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Validation { .. }), "got {err:?}");
    }

    #[test]
    fn guard_request_omits_relevance_fields_when_none() {
        let req = GuardRequest {
            text: "claim".into(),
            source_context: "ctx".into(),
            mode: None,
            query: None,
            relevance_mode: None,
        };
        let json = serde_json::to_value(&req).unwrap();
        assert!(json.get("query").is_none());
        assert!(json.get("relevance_mode").is_none());
    }
}
