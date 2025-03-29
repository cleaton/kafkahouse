mod kafka;

use log::info;
use crate::kafka::Broker;
use crate::kafka::consumer_group::SharedConsumerGroupCache;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    info!("Starting Kafka broker with storage interface");
    let broker = Broker::new();
    
    // Initialize the consumer group cache
    info!("Initializing consumer group cache");
    let consumer_group_cache = SharedConsumerGroupCache::new();
    
    // Start the consumer group cache worker
    info!("Starting consumer group cache worker");
    consumer_group_cache.start_worker("http://127.0.0.1:8123".to_string()).await?;
    
    // Associate the consumer group cache with the broker
    let broker = broker.with_consumer_group_cache(consumer_group_cache);
    
    // Start the Kafka broker
    let _ = broker.start().await?;
    Ok(())
}
