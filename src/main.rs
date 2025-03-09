mod kafka;

use log::info;
use crate::kafka::Broker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    info!("Starting Kafka broker with storage interface");
    let broker = Broker::new();
    
    // Start the Kafka broker
    let _ = broker.start().await?;
    Ok(())
}
