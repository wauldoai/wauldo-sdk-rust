//! SSE stream parser for chat completion streaming

use crate::error::{Error, Result};
use crate::http_types::ChatChunk;
use tokio::sync::mpsc;

/// Parse SSE stream and send content chunks through channel
pub async fn parse_sse_stream(
    resp: reqwest::Response,
    tx: mpsc::Sender<Result<String>>,
) -> Result<()> {
    use futures_util::StreamExt;
    let mut stream = resp.bytes_stream();
    let mut buf = Vec::<u8>::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                buf.extend_from_slice(&bytes);
                if process_byte_buffer(&mut buf, &tx).await? {
                    return Ok(());
                }
            }
            Err(e) => {
                // Best-effort send; receiver may already be dropped
                let _ = tx.send(Err(Error::connection(e.to_string()))).await;
                return Err(Error::connection(e.to_string()));
            }
        }
    }
    Ok(())
}

/// An in-band stream error: `data: {"error": {"message": ..., "type": ...}}`,
/// how OpenAI-compatible servers report a failure after the stream started.
/// Reading it as an ordinary chunk would end the stream as an empty success.
/// `code` is 0: the HTTP status was already 200 when the failure arrived.
fn stream_error(value: &serde_json::Value) -> Option<Error> {
    let error = value.get("error").filter(|e| is_truthy(e))?;
    let message = error
        .get("message")
        .and_then(|m| m.as_str())
        .filter(|m| !m.is_empty())
        .unwrap_or("An error occurred during streaming")
        .to_string();
    Some(Error::Server {
        code: 0,
        message,
        data: Some(error.clone()),
    })
}

/// Python truthiness, as openai-python's `data.get("error")` applies it: null,
/// false, 0, "" and an empty array or object mean "no error".
fn is_truthy(v: &serde_json::Value) -> bool {
    use serde_json::Value;
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Process buffered bytes, decoding complete lines and extracting SSE deltas.
/// Returns `true` when [DONE] is received.
async fn process_byte_buffer(buf: &mut Vec<u8>, tx: &mpsc::Sender<Result<String>>) -> Result<bool> {
    while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
        let line_bytes = buf[..pos].to_vec();
        buf.drain(..=pos);
        let line = match String::from_utf8(line_bytes) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Non-UTF8 SSE line skipped: {}", e);
                continue;
            }
        };
        let trimmed = line.trim();
        if let Some(data) = trimmed.strip_prefix("data: ") {
            if data == "[DONE]" {
                return Ok(true);
            }
            let value = match serde_json::from_str::<serde_json::Value>(data) {
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!("Malformed SSE chunk skipped: {}", data);
                    continue;
                }
            };
            if let Some(err) = stream_error(&value) {
                // Best-effort send; receiver may already be dropped
                let _ = tx.send(Err(err)).await;
                return Ok(true);
            }
            match serde_json::from_value::<ChatChunk>(value) {
                Ok(chunk) => {
                    if let Some(choice) = chunk.choices.first() {
                        if let Some(content) = &choice.delta.content {
                            if tx.send(Ok(content.clone())).await.is_err() {
                                return Ok(true);
                            }
                        }
                    }
                }
                Err(_) => {
                    tracing::warn!("Malformed SSE chunk skipped: {}", data);
                }
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn parse(sse: &str) -> Vec<Result<String>> {
        let (tx, mut rx) = mpsc::channel(32);
        let mut buf = sse.as_bytes().to_vec();
        process_byte_buffer(&mut buf, &tx)
            .await
            .expect("parser runs");
        drop(tx);
        let mut out = Vec::new();
        while let Some(item) = rx.recv().await {
            out.push(item);
        }
        out
    }

    #[tokio::test]
    async fn test_sse_in_band_error_is_an_error_not_an_empty_success() {
        let out = parse(concat!(
            "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
            "data: {\"error\":{\"message\":\"Internal processing error (trace: t1)\",\"type\":\"server_error\"}}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"ignored\"}}]}\n\n",
        ))
        .await;
        assert_eq!(out.len(), 1, "stops at the error: {out:?}");
        match &out[0] {
            Err(Error::Server { message, data, .. }) => {
                assert_eq!(message, "Internal processing error (trace: t1)");
                assert_eq!(
                    data.as_ref().map(|d| d["type"].clone()),
                    Some("server_error".into())
                );
            }
            other => panic!("expected Error::Server, got {other:?}"),
        }
    }

    #[test]
    fn test_sse_falsy_error_field_is_not_an_error() {
        for v in [
            serde_json::json!({"error": null}),
            serde_json::json!({"error": false}),
            serde_json::json!({"error": {}}),
            serde_json::json!({"error": ""}),
            serde_json::json!({"error": 0}),
        ] {
            assert!(stream_error(&v).is_none(), "{v}");
        }
    }

    #[tokio::test]
    async fn test_sse_content_chunks_still_stream() {
        let out = parse(concat!(
            "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\" world\"}}]}\n\n",
            "data: [DONE]\n\n",
        ))
        .await;
        let text: Vec<String> = out.into_iter().map(|r| r.expect("content")).collect();
        assert_eq!(text, vec!["Hello".to_string(), " world".to_string()]);
    }
}
