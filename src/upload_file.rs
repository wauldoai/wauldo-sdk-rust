//! `POST /v1/upload-file`: upload a document (PDF, DOCX, image, text file)
//! as `multipart/form-data`; the server extracts its text and indexes it.

use crate::error::{Error, Result};
use crate::http_client::HttpClient;
use crate::http_types::PiiPolicy;
use crate::retry::request_with_retry;
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Largest upload the server reads (10 MB). The limit counts the whole
/// request (the file, its name and the form fields), so the file itself must
/// be a few hundred bytes smaller: a larger file is refused before it is
/// read, a larger request before it is sent.
pub const MAX_UPLOAD_FILE_SIZE: usize = 10 * 1024 * 1024;

/// Options for [`HttpClient::upload_file`] and [`HttpClient::upload_file_bytes`].
#[derive(Debug, Clone, Default)]
pub struct UploadFileOptions {
    /// `None`: the server default, [`PiiPolicy::Redact`].
    pub pii_policy: Option<PiiPolicy>,
    /// Per-request timeout override, in milliseconds.
    pub timeout_ms: Option<u64>,
}

/// Quality of the text extracted from the file (absent for images).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentQuality {
    /// Composite score, 0.0-1.0.
    pub score: f32,
    /// `"high"`, `"medium"` or `"low"`.
    pub label: String,
    #[serde(default)]
    pub word_count: usize,
    #[serde(default)]
    pub line_density: f32,
    #[serde(default)]
    pub avg_line_length: f32,
    #[serde(default)]
    pub paragraph_count: usize,
}

/// Response of `POST /v1/upload-file`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadFileResponse {
    pub document_id: String,
    pub chunks_count: usize,
    #[serde(default)]
    pub indexed_at: Option<String>,
    /// `"text"`, `"image"`, `"pdf"` or `"docx"`.
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub quality: Option<DocumentQuality>,
}

impl HttpClient {
    /// Upload a file for RAG -- POST /v1/upload-file (multipart).
    ///
    /// PDF, DOCX, images and text files, 10 MB at most for the whole request
    /// (file, name and fields). The file name sent is the path's last
    /// component.
    pub async fn upload_file(
        &self,
        path: impl AsRef<Path>,
        title: Option<String>,
        options: Option<UploadFileOptions>,
    ) -> Result<UploadFileResponse> {
        let path = path.as_ref().to_path_buf();
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("upload")
            .to_string();
        let data = tokio::task::spawn_blocking(move || read_upload(&path))
            .await
            .map_err(|e| Error::connection(format!("File read task failed: {}", e)))??;
        self.upload_file_bytes(data, filename, title, options).await
    }

    /// Upload a file held in memory -- POST /v1/upload-file (multipart).
    /// `filename` tells the server the file type (`report.pdf`).
    pub async fn upload_file_bytes(
        &self,
        data: Vec<u8>,
        filename: impl Into<String>,
        title: Option<String>,
        options: Option<UploadFileOptions>,
    ) -> Result<UploadFileResponse> {
        check_upload_size(data.len())?;
        let options = options.unwrap_or_default();
        let mut fields: Vec<(&str, String)> = Vec::new();
        if let Some(title) = title {
            fields.push(("title", title));
        }
        if let Some(policy) = options.pii_policy {
            fields.push(("pii_policy", pii_policy_value(policy).to_string()));
        }
        let boundary = boundary_for(&data);
        let body = multipart_body(&boundary, &filename.into(), &data, &fields);
        check_request_size(body.len())?;
        self.post_multipart("/v1/upload-file", &boundary, body, options.timeout_ms)
            .await
    }

    async fn post_multipart<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        boundary: &str,
        body: Vec<u8>,
        timeout_ms: Option<u64>,
    ) -> Result<T> {
        tracing::debug!("SDK request: POST {}", path);
        if let Some(hook) = self.on_request {
            hook("POST", path);
        }
        let url = format!("{}{}", self.base_url, path);
        let content_type = format!("multipart/form-data; boundary={}", boundary);
        let start = std::time::Instant::now();
        let result = request_with_retry(&self.client, &self.retry_config, || {
            let mut req = self
                .client
                .post(&url)
                .header(CONTENT_TYPE, content_type.as_str())
                .body(body.clone());
            if let Some(ms) = timeout_ms {
                req = req.timeout(std::time::Duration::from_millis(ms));
            }
            req.build()
        })
        .await;
        self.fire_response_hooks(&result, start);
        result
    }
}

/// Read a file to upload, refusing one larger than the server accepts
/// before reading it.
fn read_upload(path: &Path) -> Result<Vec<u8>> {
    let not_read = |e: std::io::Error| {
        Error::validation_field(format!("Cannot read {}: {}", path.display(), e), "path")
    };
    let len = std::fs::metadata(path).map_err(not_read)?.len();
    check_upload_size(usize::try_from(len).unwrap_or(usize::MAX))?;
    std::fs::read(path).map_err(not_read)
}

fn check_upload_size(len: usize) -> Result<()> {
    if len == 0 {
        return Err(Error::validation_field("The file is empty", "file"));
    }
    if len > MAX_UPLOAD_FILE_SIZE {
        return Err(Error::validation_field(
            format!(
                "File exceeds the 10 MB limit ({} > {} bytes)",
                len, MAX_UPLOAD_FILE_SIZE
            ),
            "file",
        ));
    }
    Ok(())
}

/// The server's limit applies to the multipart body, not to the file alone.
fn check_request_size(len: usize) -> Result<()> {
    if len > MAX_UPLOAD_FILE_SIZE {
        return Err(Error::validation_field(
            format!(
                "Upload exceeds the 10 MB limit ({} > {} bytes, file name and form fields included)",
                len, MAX_UPLOAD_FILE_SIZE
            ),
            "file",
        ));
    }
    Ok(())
}

/// Refuse a text upload (`POST /v1/upload`) whose JSON body is over the
/// server's limit. The limit counts the bytes sent (UTF-8, file name and
/// fields included), not the characters of the text: an accented letter
/// takes two bytes, most CJK characters three.
pub(crate) fn check_text_upload_size(body: &impl Serialize) -> Result<()> {
    let len = serde_json::to_vec(body)
        .map_err(|e| Error::connection(format!("Serialize error: {}", e)))?
        .len();
    if len > MAX_UPLOAD_FILE_SIZE {
        return Err(Error::validation_field(
            format!(
                "Content exceeds the 10 MB limit once encoded ({} > {} bytes as UTF-8)",
                len, MAX_UPLOAD_FILE_SIZE
            ),
            "content",
        ));
    }
    Ok(())
}

fn pii_policy_value(policy: PiiPolicy) -> &'static str {
    match policy {
        PiiPolicy::Redact => "redact",
        PiiPolicy::Keep => "keep",
    }
}

/// A multipart boundary that does not occur in the file. Always the same
/// length, so the size of the request does not depend on the draw.
fn boundary_for(data: &[u8]) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    loop {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let boundary = format!("----WauldoBoundary{:024x}{:08x}", nanos, n);
        let needle = boundary.as_bytes();
        if !data.windows(needle.len()).any(|w| w == needle) {
            return boundary;
        }
    }
}

/// The `multipart/form-data` body: the `file` part, then each text field.
/// Quotes and line breaks in the file name are replaced, so a name cannot
/// end its header early.
fn multipart_body(
    boundary: &str,
    filename: &str,
    data: &[u8],
    fields: &[(&str, String)],
) -> Vec<u8> {
    let safe_name: String = filename
        .chars()
        .map(|c| {
            if matches!(c, '"' | '\r' | '\n') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let mut body = Vec::with_capacity(data.len() + 512);
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
             filename=\"{safe_name}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n");
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_config::HttpConfig;
    use wiremock::matchers::{header_regex, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    fn upload_json() -> serde_json::Value {
        serde_json::json!({
            "document_id": "doc-1",
            "chunks_count": 3,
            "indexed_at": "2026-11-22T00:00:00Z",
            "content_type": "pdf",
            "trace_id": "tr",
            "quality": {"score": 0.8, "label": "high", "word_count": 120,
                        "line_density": 0.9, "avg_line_length": 40.0, "paragraph_count": 4}
        })
    }

    async fn upload_server() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/upload-file"))
            .and(header_regex(
                "content-type",
                "^multipart/form-data; boundary=",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(upload_json()))
            .expect(1)
            .mount(&server)
            .await;
        server
    }

    /// A text whose upload body is exactly `size` bytes, a million of its
    /// characters two bytes long in UTF-8.
    fn text_of_encoded_size(size: usize) -> String {
        let overhead = serde_json::to_vec(&serde_json::json!({"content": ""}))
            .expect("json")
            .len();
        let accents = "é".repeat(1_000_000);
        let ascii = "a".repeat(size - overhead - accents.len());
        accents + &ascii
    }

    #[tokio::test]
    async fn test_rag_upload_limit_counts_encoded_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/upload"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"document_id": "d", "chunks_count": 1})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let client = HttpClient::new(HttpConfig::new(server.uri())).expect("client");

        let fits = text_of_encoded_size(MAX_UPLOAD_FILE_SIZE);
        client
            .rag_upload(fits.clone(), None)
            .await
            .expect("upload at the limit");
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(requests[0].body.len(), MAX_UPLOAD_FILE_SIZE);

        let over = client.rag_upload(fits.clone() + "a", None).await;
        assert!(matches!(over, Err(Error::Validation { .. })), "{over:?}");
        let named = client.rag_upload(fits, Some("f.txt".into())).await;
        assert!(matches!(named, Err(Error::Validation { .. })), "{named:?}");
        // 6 M characters, 12 MB once encoded: refused before sending.
        let wide = "é".repeat(6_000_000);
        assert!(wide.chars().count() < MAX_UPLOAD_FILE_SIZE);
        let refused = client.rag_upload(wide, None).await;
        assert!(
            matches!(refused, Err(Error::Validation { .. })),
            "{refused:?}"
        );
    }

    fn sent_body(requests: &[Request]) -> String {
        String::from_utf8_lossy(&requests[0].body).into_owned()
    }

    #[tokio::test]
    async fn test_upload_file_sends_file_title_and_pii_policy() {
        let server = upload_server().await;
        let dir = std::env::temp_dir().join(format!("wauldo-upload-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let file = dir.join("contract.pdf");
        std::fs::write(&file, b"%PDF-1.4 fake").expect("write");

        let client = HttpClient::new(HttpConfig::new(server.uri())).expect("client");
        let options = UploadFileOptions {
            pii_policy: Some(PiiPolicy::Keep),
            ..Default::default()
        };
        let resp = client
            .upload_file(&file, Some("Q3 Contract".into()), Some(options))
            .await
            .expect("upload");
        std::fs::remove_dir_all(&dir).ok();

        assert_eq!(resp.document_id, "doc-1");
        assert_eq!(resp.content_type.as_deref(), Some("pdf"));
        assert_eq!(resp.quality.expect("quality").label, "high");
        let body = sent_body(&server.received_requests().await.expect("requests"));
        assert!(
            body.contains("name=\"file\"; filename=\"contract.pdf\""),
            "{body}"
        );
        assert!(body.contains("%PDF-1.4 fake"), "{body}");
        assert!(
            body.contains("name=\"title\"\r\n\r\nQ3 Contract\r\n"),
            "{body}"
        );
        assert!(
            body.contains("name=\"pii_policy\"\r\n\r\nkeep\r\n"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn test_upload_file_bytes_sends_only_the_file_by_default() {
        let server = upload_server().await;
        let client = HttpClient::new(HttpConfig::new(server.uri())).expect("client");
        client
            .upload_file_bytes(b"hello".to_vec(), "notes.txt", None, None)
            .await
            .expect("upload");
        let body = sent_body(&server.received_requests().await.expect("requests"));
        assert!(body.contains("filename=\"notes.txt\""), "{body}");
        assert!(!body.contains("name=\"title\""), "{body}");
        assert!(!body.contains("name=\"pii_policy\""), "{body}");
    }

    #[tokio::test]
    async fn test_upload_file_refuses_empty_oversized_and_missing_files() {
        let client = HttpClient::new(HttpConfig::new("http://127.0.0.1:9")).expect("client");
        let empty = client
            .upload_file_bytes(Vec::new(), "a.pdf", None, None)
            .await;
        assert!(matches!(empty, Err(Error::Validation { .. })), "{empty:?}");
        let big = vec![0u8; MAX_UPLOAD_FILE_SIZE + 1];
        let too_big = client.upload_file_bytes(big, "a.pdf", None, None).await;
        assert!(
            matches!(too_big, Err(Error::Validation { .. })),
            "{too_big:?}"
        );
        let missing = client
            .upload_file("/nonexistent/wauldo/a.pdf", None, None)
            .await;
        assert!(
            matches!(missing, Err(Error::Validation { .. })),
            "{missing:?}"
        );
    }

    #[tokio::test]
    async fn test_upload_file_limit_counts_the_whole_request() {
        // The server's 10 MB limit applies to the multipart body, not the
        // file: a file of exactly 10 MB used to pass the check and be refused
        // unread.
        let server = upload_server().await;
        let client = HttpClient::new(HttpConfig::new(server.uri())).expect("client");
        let fields = [("title", "T".to_string())];
        let overhead = multipart_body(&boundary_for(&[]), "big.pdf", &[], &fields).len();
        let fits = MAX_UPLOAD_FILE_SIZE - overhead;
        let upload = |len: usize| {
            client.upload_file_bytes(vec![b'x'; len], "big.pdf", Some("T".into()), None)
        };
        upload(fits)
            .await
            .expect("a request of exactly 10 MB is sent");
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(requests[0].body.len(), MAX_UPLOAD_FILE_SIZE);
        let over = upload(fits + 1).await;
        assert!(
            matches!(&over, Err(Error::Validation { message, .. }) if message.contains("form fields")),
            "{over:?}"
        );
        let file_at_limit = upload(MAX_UPLOAD_FILE_SIZE).await;
        assert!(
            matches!(file_at_limit, Err(Error::Validation { .. })),
            "{file_at_limit:?}"
        );
    }

    #[test]
    fn test_multipart_body_escapes_the_file_name_and_avoids_the_data() {
        let body = multipart_body("B", "a\"b\r\n.pdf", b"x", &[]);
        let text = String::from_utf8(body).expect("utf8");
        assert!(text.contains("filename=\"a_b__.pdf\""), "{text}");
        assert!(text.ends_with("--B--\r\n"));
        let data = b"----WauldoBoundary".repeat(4);
        let boundary = boundary_for(&data);
        assert!(!data
            .windows(boundary.len())
            .any(|w| w == boundary.as_bytes()));
    }
}
