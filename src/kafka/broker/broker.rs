use std::sync::Arc;

use anyhow::Result;
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::messages::produce_response::{TopicProduceResponse, PartitionProduceResponse, LeaderIdAndEpoch};
use clickhouse::{Client, Row};
use kafka_protocol::records::{RecordBatchDecoder, Compression};
use log::{info, debug};
use serde::Serialize;
use tokio::net::TcpListener;

use crate::kafka::client::KafkaClient;
use crate::kafka::consumer_group::SharedConsumerGroupCache;
use super::types::TopicPartitions;

#[derive(Row, Serialize)]
struct KafkaMessageInsert {
    topic: String,
    key: String,
    value: String,
}

pub struct Broker {
    client: Client,
    consumer_group_cache: Option<SharedConsumerGroupCache>,
}

impl Broker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            client: Client::default()
            .with_url("http://127.0.0.1:8123")
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1"),
            consumer_group_cache: None,
        })
    }
    
    pub fn with_consumer_group_cache(self: &Arc<Self>, cache: SharedConsumerGroupCache) -> Arc<Self> {
        let broker = Arc::new(Self {
            client: self.client.clone(),
            consumer_group_cache: Some(cache),
        });
        broker
    }

    pub async fn produce(&self, mut req: ProduceRequest) -> Result<ProduceResponse, anyhow::Error> {
        let mut insert = self.client.insert("kafka_messages_ingest")?;
        let mut message_count = 0;
        
        // Process all topics and partitions
        for topic_data in &mut req.topic_data {
            let topic_name = topic_data.name.to_string();
            for partition_data in &mut topic_data.partition_data {
                // Process records if they exist
                if let Some(mut record_batch) = partition_data.records.take() {
                    let records = RecordBatchDecoder::decode_with_custom_compression::<_, fn(&mut bytes::Bytes, Compression) -> Result<bytes::Bytes, anyhow::Error>>(
                        &mut record_batch,
                        None,
                    )?;
                    debug!("Processing records for topic {} partition {}", topic_name, partition_data.index);
                    for record in records {
                        insert.write(&KafkaMessageInsert { 
                            topic: topic_name.clone(), 
                            key: record.key.map(|k| String::from_utf8_lossy(&k).to_string()).unwrap_or_default(), 
                            value: record.value.map(|v| String::from_utf8_lossy(&v).to_string()).unwrap_or_default(),
                        }).await?;
                        message_count += 1;
                    }
                }
            }
        }
        
        // Commit the insert
        insert.end().await?;
        info!("Inserted {} messages into ClickHouse", message_count);
        
        // Create response with dummy offsets
        let topic_responses = req.topic_data.iter().map(|topic_data| {
            let partition_responses = topic_data.partition_data.iter().map(|partition_data| {
                PartitionProduceResponse::default()
                    .with_index(partition_data.index)
                    .with_error_code(0) // Success
                    .with_base_offset(0) // Dummy offset
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0)
                    .with_current_leader(LeaderIdAndEpoch::default())
            }).collect();
            
            TopicProduceResponse::default()
                .with_name(topic_data.name.clone())
                .with_partition_responses(partition_responses)
        }).collect();
        
        // Return the response
        Ok(ProduceResponse::default()
            .with_responses(topic_responses)
            .with_throttle_time_ms(0))
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), anyhow::Error> {
        let listener = TcpListener::bind("127.0.0.1:9092").await?;
        info!("Kafka broker listening on port 9092");
    
        loop {
            // Accept new connections
            let (socket, _addr) = listener.accept().await?;

            // Clone the Arc for this client
            let broker_clone = Arc::clone(self);
            
            // Spawn a new task for each client
            tokio::spawn(async move {
                let mut client = KafkaClient::new(broker_clone);
                client.run(socket).await;
            });
        }
    }

    pub async fn fetch_records(&self, topics_partitions: &[TopicPartitions]) -> Result<Vec<(String, i32, String, String, i64)>, anyhow::Error> {
        // Count total partitions
        let total_partitions: usize = topics_partitions.iter()
            .map(|tp| tp.partitions.len())
            .sum();
            
        if total_partitions == 0 {
            return Ok(Vec::new());
        }
        
        // Build a query to fetch records for multiple topics and partitions
        let mut query = String::from("SELECT topic, partition, key, value, offset FROM kafka_messages WHERE ");
        
        let mut conditions = Vec::new();
        
        // Create conditions for each topic and partition
        for _ in 0..total_partitions {
            conditions.push("(topic = ? AND partition = ? AND offset >= ? AND snowflakeIDToDateTime64(toUInt64(offset), 1735689600000) <= now64() - interval 5 seconds)");
        }
        
        query.push_str(&conditions.join(" OR "));
        query.push_str(" ORDER BY topic, partition, offset LIMIT 5000"); // Limit to prevent huge responses
        
        debug!("Executing ClickHouse query: {}", query);
        
        // Execute the query
        let mut query_builder = self.client.query(&query);
        
        // Bind all parameters in order
        for topic_partitions in topics_partitions {
            for partition in &topic_partitions.partitions {
                query_builder = query_builder
                    .bind(&topic_partitions.topic)
                    .bind(partition.partition_id)
                    .bind(partition.offset);
            }
        }
        
        let result: Vec<(String, i32, String, String, i64)> = query_builder.fetch_all().await?;
        Ok(result)
    }

    pub async fn get_latest_offset(&self, topic: &str, partition: i32) -> Result<i64, anyhow::Error> {
        let query = "SELECT max(offset) as max_offset FROM kafka_messages WHERE topic = ? AND partition = ?";
        let result: Option<i64> = self.client.query(query)
            .bind(topic)
            .bind(partition)
            .fetch_optional()
            .await?;
        
        Ok(result.unwrap_or(0))
    }

    pub async fn get_earliest_offset(&self, topic: &str, partition: i32) -> Result<i64, anyhow::Error> {
        let query = "SELECT min(offset) as min_offset FROM kafka_messages WHERE topic = ? AND partition = ?";
        let result: Option<i64> = self.client.query(query)
            .bind(topic)
            .bind(partition)
            .fetch_optional()
            .await?;
        
        Ok(result.unwrap_or(0))
    }

    pub async fn get_offset_at_time(&self, topic: &str, partition: i32, timestamp: i64) -> Result<i64, anyhow::Error> {
        let query = "SELECT min(offset) as offset FROM kafka_messages WHERE topic = ? AND partition = ? AND ts >= fromUnixTimestamp64Milli(?)";
        let result: Option<i64> = self.client.query(query)
            .bind(topic)
            .bind(partition)
            .bind(timestamp)
            .fetch_optional()
            .await?;
        
        Ok(result.unwrap_or(0))
    }
} 