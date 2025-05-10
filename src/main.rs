mod kafka;
mod storage;

use crate::kafka::Broker;
use log::info;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Starting Kafka broker with storage interface");
    let clickhouse_url = std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let listen_host = std::env::var("LISTEN_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let listen_port = std::env::var("LISTEN_PORT")
        .unwrap_or_else(|_| "9092".to_string())
        .parse::<i32>()
        .expect("LISTEN_PORT must be a number");

    let broker = Broker::new(clickhouse_url.to_string(), listen_host, listen_port).await?;

    // The consumer group cache is now initialized inside the broker
    // and the worker is started in broker.start()

    // Start the Kafka broker
    let _ = broker.start().await?;
    Ok(())
}
