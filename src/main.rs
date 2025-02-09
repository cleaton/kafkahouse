mod network;
mod protocol;
mod storage;
mod handler;

use anyhow::Result;
use tracing::{info, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use crate::storage::MessageStore;
use crate::handler::Handler;
use std::str::FromStr;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Get log level from environment variable or default to WARNING
    let log_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|level| Level::from_str(&level).ok())
        .unwrap_or(Level::WARN);

    // Initialize logging with more detail
    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_span_events(FmtSpan::CLOSE)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("Starting KafkaHouse broker with log level: {}", log_level);
    
    // Initialize the message store
    let message_store = MessageStore::new("http://localhost:8123")?;
    let handler = Handler::new(message_store);
    
    // Start the Kafka protocol server
    let addr = "127.0.0.1:9092";
    network::run_server(addr, handler).await?;

    Ok(())
} 