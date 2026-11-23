//! Streams are not cut by the 120 s request timeout, and a silent one fails.
//!
//! Each test runs on a paused tokio clock against a raw SSE server in the
//! same runtime: minutes of server time pass instantly, and the server
//! sends a keep-alive every 15 s, as the real one does.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use wauldo::{AgentsClient, AgentsError, ChatMessage, ChatRequest, Error, HttpClient, HttpConfig};

const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// What the fake server sends after the response head.
enum Body {
    /// `first`, then `beats` keep-alives 15 s apart, then `last`, then close.
    Long {
        first: String,
        beats: u32,
        last: String,
    },
    /// `first`, then nothing, connection held open.
    Silent { first: String },
    /// No response at all: the request is read and never answered.
    NoResponse,
}

/// Serves one connection with `body`; returns the base URL.
async fn serve_once(body: Body) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        read_request(&mut sock).await;
        if !matches!(body, Body::NoResponse) {
            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                        cache-control: no-cache\r\nconnection: close\r\n\r\n";
            sock.write_all(head.as_bytes()).await.expect("head");
        }
        match body {
            Body::Long { first, beats, last } => {
                sock.write_all(first.as_bytes()).await.expect("first");
                for _ in 0..beats {
                    tokio::time::sleep(KEEP_ALIVE).await;
                    sock.write_all(b": keep-alive\n\n").await.expect("beat");
                }
                sock.write_all(last.as_bytes()).await.expect("last");
                sock.shutdown().await.expect("shutdown");
            }
            Body::Silent { first } => {
                sock.write_all(first.as_bytes()).await.expect("first");
                std::future::pending::<()>().await;
            }
            Body::NoResponse => std::future::pending::<()>().await,
        }
    });
    format!("http://{addr}")
}

/// Reads the request head and its `content-length` body, so that closing
/// the socket afterwards sends a clean FIN, not a reset.
async fn read_request(sock: &mut TcpStream) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = sock.read(&mut chunk).await.expect("read request");
        assert!(n > 0, "client closed before sending a request");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_lowercase();
    let len: usize = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .map(|v| v.trim().parse().expect("content-length"))
        .unwrap_or(0);
    while buf.len() < head_end + len {
        let n = sock.read(&mut chunk).await.expect("read body");
        assert!(n > 0, "client closed mid-body");
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn content_frame(text: &str) -> String {
    let ev = serde_json::json!({"id": "c1", "choices": [{"delta": {"content": text}}]});
    format!("data: {ev}\n\n")
}

fn chat_request() -> ChatRequest {
    ChatRequest::new("auto", vec![ChatMessage::user("hi")])
}

#[tokio::test(start_paused = true)]
async fn test_chat_stream_longer_than_120s_is_not_cut() {
    let url = serve_once(Body::Long {
        first: content_frame("Hello"),
        beats: 10,
        last: content_frame(" world") + "data: [DONE]\n\n",
    })
    .await;
    let start = Instant::now();
    let client = HttpClient::new(HttpConfig::new(url)).expect("client");
    let mut rx = client.chat_stream(chat_request()).await.expect("open");
    let mut text = String::new();
    while let Some(piece) = rx.recv().await {
        text.push_str(&piece.expect("no error on a live stream"));
    }
    assert_eq!(text, "Hello world");
    assert!(
        start.elapsed() >= Duration::from_secs(150),
        "{:?}",
        start.elapsed()
    );
}

#[tokio::test(start_paused = true)]
async fn test_chat_stream_silent_server_yields_timeout_error() {
    let url = serve_once(Body::Silent {
        first: content_frame("Hello"),
    })
    .await;
    let start = Instant::now();
    let client = HttpClient::new(HttpConfig::new(url)).expect("client");
    let mut rx = client.chat_stream(chat_request()).await.expect("open");
    assert_eq!(rx.recv().await.expect("first").expect("ok"), "Hello");
    let err = match rx.recv().await {
        Some(Err(e)) => e,
        other => panic!("expected an error, got {other:?}"),
    };
    let waited = start.elapsed();
    assert!(matches!(err, Error::Timeout(_)), "{err:?}");
    assert!(
        waited >= Duration::from_secs(45) && waited < Duration::from_secs(60),
        "{waited:?}"
    );
    assert!(rx.recv().await.is_none(), "channel closes after the error");
}

#[tokio::test(start_paused = true)]
async fn test_agents_request_unanswered_times_out_at_120s() {
    let url = serve_once(Body::NoResponse).await;
    let start = Instant::now();
    let err = AgentsClient::new(url).get_task("t1").await.unwrap_err();
    match &err {
        AgentsError::Http(e) => assert!(e.is_timeout(), "{e:?}"),
        other => panic!("expected AgentsError::Http, got {other:?}"),
    }
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_secs(120) && waited < Duration::from_secs(121),
        "{waited:?}"
    );
}
