<h1 align="center">Wauldo Rust SDK</h1>

<p align="center">
  <strong>Verified AI answers from your documents — with the sources to prove it.</strong>
</p>

<p align="center">
  Most RAG APIs guess. Wauldo verifies.
</p>

<p align="center">
  Sources on every answer &nbsp;|&nbsp; claim-by-claim fact-check &nbsp;|&nbsp; <a href="https://wauldo.com/benchmarks">public weekly benchmarks</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/wauldo"><img src="https://img.shields.io/crates/v/wauldo.svg" alt="crates.io" /></a>&nbsp;
  <a href="https://crates.io/crates/wauldo"><img src="https://img.shields.io/crates/d/wauldo.svg" alt="Downloads" /></a>&nbsp;
  <img src="https://img.shields.io/badge/rust-1.70+-orange.svg" alt="Rust" />&nbsp;
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="MIT" />
</p>

<p align="center">
  <a href="https://wauldo.com/demo">Demo</a> &bull;
  <a href="https://wauldo.com/docs">Docs</a> &bull;
  <a href="https://rapidapi.com/binnewzzin/api/smart-rag-api">Free API Key</a> &bull;
  <a href="https://wauldo.com/benchmarks">Benchmarks</a>
</p>

---

## Quickstart (30 seconds)

```toml
[dependencies]
wauldo = "0.18"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use wauldo::{HttpClient, HttpConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = HttpClient::new(
        HttpConfig::new("https://api.wauldo.com").with_api_key("YOUR_API_KEY"),
    )?;

    // Upload a document
    client.rag_upload("Our refund policy allows returns within 60 days...", Some("policy.txt".into())).await?;

    // Ask a question — answer is verified against the source
    let result = client.rag_query("What is the refund policy?", None).await?;
    println!("Answer: {}", result.answer);
    println!("Grounded: {}", result.grounded());
    Ok(())
}
```

> Got your key from RapidAPI? Use the RapidAPI host instead — see [RapidAPI](#rapidapi).

```
Output:
Answer: Returns are accepted within 60 days of purchase.
Grounded: true | Confidence: 92%
```

[Try the demo](https://wauldo.com/demo) | [Get a free API key](https://rapidapi.com/binnewzzin/api/smart-rag-api)

---

## Why Wauldo (and not standard RAG)

**Typical RAG pipeline**

```
retrieve → generate → hope it's correct
```

**Wauldo pipeline**

```
retrieve → extract facts → generate → verify → return or refuse
```

If the answer can't be verified, it returns **"insufficient evidence"** instead of guessing.

### See the difference

```
Document: "Refunds are processed within 60 days"

Typical RAG:  "Refunds are processed within 30 days"     ← wrong
Wauldo:       "Refunds are processed within 60 days"     ← verified
              or "insufficient evidence" if unclear       ← safe
```

---

## Examples

### Upload a PDF and ask questions

```rust
// Upload — text extraction + quality scoring happens server-side
let upload = client.upload_file("contract.pdf", Some("Q3 Contract".into()), None).await?;
let quality = upload.quality.as_ref().map_or("n/a", |q| q.label.as_str());
println!("Extracted {} chunks, quality: {}", upload.chunks_count, quality);

// Query
let result = client.rag_query("What are the payment terms?", None).await?;
println!("Answer: {}", result.answer);
println!("Confidence: {:.0}%", result.confidence() * 100.0);
println!("Grounded: {}", result.grounded());
```

### Guard — fact-check any LLM output

```rust
let result = client.guard(
    "Returns are accepted within 60 days.",
    "Our policy allows returns within 14 days.",
    Some("lexical"),
    None,
    None,
).await?;
println!("Verdict: {}", result.verdict);        // "rejected"
println!("Action: {}", result.action);           // "block"
println!("Reason: {:?}", result.claims[0].reason); // Some("numerical_mismatch")
```

Optionally score how well the text answers the original question — decoupled
from the factual verdict (a verified answer can still be off-topic):

```rust
let result = client.fact_check(
    "Rust was first released in 2010 by Mozilla Research.",
    "Rust is a systems language released in 2010 by Mozilla Research.",
    Some("lexical"),
    Some("What year was Rust released?"),
    None, // relevance_mode defaults to "fast" server-side
).await?;
println!("Verdict: {}", result.verdict); // "verified"
if let Some(relevance) = &result.relevance {
    println!("Relevance: {} ({:.2})", relevance.verdict, relevance.score);
}
```

### Deployed Agents — create, run, stream

```rust
use futures_util::StreamExt;
use wauldo::agents::{
    AgentsClient, AgentsError, CreateAgentRequest, RunOptions, TaskErrorInfo, DEFAULT_WAIT_TIMEOUT,
};

let agents = AgentsClient::new("https://api.wauldo.com")
    .with_api_key("YOUR_API_KEY")
    .with_tenant("my-tenant");
// Transient failures are retried (`with_max_retries`, default 3; Retry-After honoured):
// 429/503 on any call, 500/502/504 and connection errors on GET only.

let agent = agents.create(CreateAgentRequest {
    name: "support-bot".into(),
    wauldo_toml: r#"[agent]
name = "support-bot"
[model]
provider = "openrouter"
name = "auto""#.into(),
    description: "Answers refund questions".into(),
    preset: Some("general_task".into()), // or "rust_backend_architect", ...
    ..Default::default()
}).await?;

// One call: the server holds the response while the run completes,
// so a quick run comes back verified in a single round-trip.
let task = agents.run_and_wait(
    &agent.id,
    "Can I return a shirt 30 days after purchase?",
    RunOptions::default(),
    DEFAULT_WAIT_TIMEOUT, // 360 s: enough for ordinary runs; pass more for long ones
).await?;
println!("{}", task.result.as_deref().unwrap_or(""));
if let Some(v) = &task.verification {
    println!("verdict: {:?}",  v.verdict);        // Safe | Unverified | Block | ...
    println!("trust:   {}",    v.trust_score);    // 0.0 – 1.0
    println!("message: {}",    v.message.as_deref().unwrap_or("<none>"));
}

// Need progress events? Start the run, then stream each workflow state as it completes.
let run = agents.run(&agent.id, "Can I return a shirt 30 days after purchase?", None).await?;
let stream = agents.stream_task(&run.task_id).await?;
futures_util::pin_mut!(stream);
while let Some(ev) = stream.next().await {
    let ev = ev?;
    // STREAM_LAGGED is a notice, not a state: the stream fell behind, the run goes on and
    // the terminal event still comes (agents.get_task holds every state of the run).
    println!("  {}: {}ms  ({} tok)", ev.state_name, ev.duration_ms, ev.completion_tokens);
    if let Some(end) = ev.terminal_payload() { // last event: result + verification, nothing left to fetch
        println!("{}", end.result.unwrap_or_default());
    }
}

// Batch job: no polling, the server POSTs the task to your webhook when the run ends;
// auto_share returns a public link to the run at creation.
let run = agents.run_with_options(&agent.id, "Summarise ticket #4521", RunOptions {
    webhook_url: Some("https://example.com/hooks/wauldo".into()),
    auto_share: Some(true),
    ..Default::default()
}).await?;
println!("{:?}", run.share_url);

// A wait that runs out is typed; the task keeps running on the server,
// and waiting again with its id picks it up.
match agents.wait_for_task(&run.task_id, std::time::Duration::from_secs(60), std::time::Duration::from_secs(2)).await {
    Ok(task) => println!("{:?}", task.status),
    Err(AgentsError::Timeout { task_id, last_status, .. }) => {
        println!("{task_id} still {last_status:?}");
        agents.wait_for_task(&task_id, DEFAULT_WAIT_TIMEOUT, std::time::Duration::from_secs(2)).await?;
    }
    Err(e) => return Err(e.into()),
}

// Human approval: pause a task, then approve (it runs again) or reject (cancelled).
// The reason is stored with the decision (task.decisions). A task left paused 7 days
// without a decision is cancelled; resume_task() runs it again as a new task.
agents.pause_task(&run.task_id, Some("check the refund amount")).await?;
agents.approve_task(&run.task_id, None).await?; // or reject_task

// A failed task is not an error: its `error` is "code: message", parsed by error_info().
if let Some(info) = task.error_info() {
    match info.code() {
        Some(TaskErrorInfo::INVALID_PRESET | TaskErrorInfo::INVALID_AGENT_CONFIG) => {
            eprintln!("Fix the agent: {}", info.message)
        }
        _ => { agents.resume_task(&task.task_id).await?; } // transient: run it again later
    }
}
```

| Code | Meaning | What to do |
|---|---|---|
| `invalid_preset` | The agent names a workflow preset that does not exist | Fix the agent |
| `invalid_agent_config` | The agent's inline workflow could not be loaded | Fix the agent |
| `rate_limited` | The model service is saturated | Retry in a few minutes |
| `timeout` | The run did not finish within its time limit | Retry, or simplify the request |
| `model_unavailable` | The model service is temporarily unavailable | Retry later |
| `run_failed` | The run failed for another reason | Retry later |

New codes may be added: handle an unknown one like `run_failed`. An error that
does not start with a code (a task cancelled after 7 days without an approval
decision) has `code` set to `None` and the whole error in `message`.

`CreateAgentRequest::custom_preset` takes an inline workflow at creation, used from the agent's first run.

### Chat (OpenAI-compatible)

```rust
use wauldo::{ChatRequest, ChatMessage};

let req = ChatRequest::new("auto", vec![ChatMessage::user("Explain ownership in Rust")]);
let resp = client.chat(req).await?;
println!("{}", resp.content());
```

### Streaming

```rust
let req = ChatRequest::new("auto", vec![ChatMessage::user("Hello!")]);
let mut rx = client.chat_stream(req).await?;
while let Some(chunk) = rx.recv().await {
    print!("{}", chunk.unwrap_or_default());
}
```

### Conversation

```rust
let mut conv = client.conversation()
    .with_system("You are an expert on Rust programming.")
    .with_model("auto");
let reply = conv.say("What is the borrow checker?").await?;
let follow_up = conv.say("Give me an example").await?;
```

---

## Features

- **Pre-generation fact extraction** — numbers, dates, limits injected as constraints
- **Post-generation grounding check** — every answer verified against sources
- **Guard API** — verify any claim against any source (3 modes: lexical, hybrid, semantic)
- **Native PDF/DOCX upload** — server-side extraction with quality scoring
- **Smart model routing** — auto-selects cheapest model that meets quality
- **OpenAI-compatible** — swap your `base_url`, keep your existing code
- **Type-safe** — full Rust type system, no unwrap in production

---

## Error Handling

```rust
use wauldo::Error;

match client.chat(req).await {
    Ok(resp) => println!("{}", resp.content()),
    Err(Error::Server { code, message, .. }) => eprintln!("Server error [{}]: {}", code, message),
    Err(Error::Connection(msg)) => eprintln!("Connection failed: {}", msg),
    Err(Error::Timeout(msg)) => eprintln!("Timeout: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

`AgentsClient`, `MemoryClient` and `WorkflowsClient` return `AgentsError`, whose
`Status { status, body }` carries the HTTP status and the server's body. It
converts into `wauldo::Error` (`Status` becomes `Error::Server` with the HTTP
status as `code`), so `?` works in a function that returns `wauldo::Result`:

```rust
async fn latest(agents: &AgentsClient, task_id: &str) -> wauldo::Result<Task> {
    Ok(agents.get_task(task_id).await?) // a 401 arrives as Error::Server { code: 401, .. }
}
```

---

## RapidAPI

Keys from the free RapidAPI plan work on the RapidAPI host, not on `api.wauldo.com` (Enterprise keys):

```rust
let config = HttpConfig::new("https://smart-rag-api.p.rapidapi.com")
    .with_header("X-RapidAPI-Key", "YOUR_RAPIDAPI_KEY")
    .with_header("X-RapidAPI-Host", "smart-rag-api.p.rapidapi.com");
let client = HttpClient::new(config)?;
```

Free tier (500 verifications/month): [RapidAPI](https://rapidapi.com/binnewzzin/api/smart-rag-api)

---

[Website](https://wauldo.com) | [Docs](https://wauldo.com/docs) | [Demo](https://wauldo.com/demo) | [Benchmarks](https://wauldo.com/benchmarks)

## Contributing

PRs welcome. Check the [good first issues](https://github.com/wauldoai/wauldo-sdk-rust/labels/good%20first%20issue).

## License

MIT — see [LICENSE](./LICENSE)
