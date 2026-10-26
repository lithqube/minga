//! Probe: connect [`NatsBus`] to a NATS server and exercise the log path.
//!
//! Usage: `cargo run -p minga-bus --example probe_nats -- nats://127.0.0.1:4222`

use futures::StreamExt;
use minga_bus::{EventBus, LogEntry, NatsBus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
    println!("connecting to {url}");

    let bus = NatsBus::connect(&url).await?;
    println!("CONNECT OK — streams created");

    let mut stream = bus.subscribe_logs("probe-task").await?;
    println!("SUBSCRIBE OK");

    bus.publish_log("probe-task", &LogEntry::stdout("probe-task", b"hello\n"))
        .await?;
    println!("PUBLISH OK");

    match tokio::time::timeout(std::time::Duration::from_secs(10), stream.next()).await {
        Ok(Some(entry)) => println!(
            "RECEIVED OK — task={} data={:?}",
            entry.task_id,
            String::from_utf8_lossy(&entry.data)
        ),
        Ok(None) => println!("RECEIVE FAILED — stream ended"),
        Err(_) => println!("RECEIVE FAILED — timeout"),
    }

    Ok(())
}
