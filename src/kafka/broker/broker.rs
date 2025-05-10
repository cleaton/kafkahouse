use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::messages::produce_response::{TopicProduceResponse, PartitionProduceResponse, LeaderIdAndEpoch};
use clickhouse::{Client, Row};
use kafka_protocol::records::{RecordBatchDecoder, Compression};
use log::{info, debug, error};
use ractor::Actor;
use serde::Serialize;
use tokio::net::TcpListener;
use chrono::Utc;

use crate::kafka::client::actor::Args;
use crate::kafka::client::ClientActor;
use super::types::TopicPartitions;
use crate::kafka::consumer::actor::ConsumerGroupsApi;

#[derive(Row, Serialize)]
struct KafkaMessageInsert {
    topic: String,
    key: String,
    value: String,
}

#[derive(Row, Serialize)]
struct ConsumerOffsetInsert {
    group_id: String,
    topic: String,
    partition: i32,
    offset: i64,
    metadata: String,
    member_id: String,
    client_id: String,
    client_host: String,
    commit_time: chrono::DateTime<Utc>,
}

struct ClickHouseClients {
    clients: Vec<Client>,
    next_idx: AtomicUsize,
}

impl ClickHouseClients {
    fn new(urls: &str) -> Self {
        let clients = urls.split(',')
            .map(|url| Client::default()
                .with_url(url.trim())
                .with_option("async_insert", "1")
                .with_option("wait_for_async_insert", "1"))
            .collect();
        Self { 
            clients,
            next_idx: AtomicUsize::new(0),
        }
    }

    fn get(&self) -> &Client {
        if self.clients.is_empty() {
            panic!("No ClickHouse clients available");
        }
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }
}

pub struct Broker {
    clients: ClickHouseClients,
    consumer_groups_api: Arc<ConsumerGroupsApi>,
    listen_host: String,
    listen_port: i32,
}

impl Broker {
    pub async fn new(clickhouse_url: String, listen_host: String, listen_port: i32) -> Result<Arc<Self>, anyhow::Error> {
        let clients = ClickHouseClients::new(&clickhouse_url);
        let client = clients.get().clone();
        
        let broker = Self {
            clients,
            consumer_groups_api: Arc::new(ConsumerGroupsApi::new(client).await),
            listen_host,
            listen_port,
        };
        
        Ok(Arc::new(broker))
    }

    pub fn get_consumer_groups_api(&self) -> Arc<ConsumerGroupsApi> {
        self.consumer_groups_api.clone()
    }

    pub fn get_client(&self) -> &Client {
        self.clients.get()
    }

    pub fn get_listen_host(&self) -> &str {
        &self.listen_host
    }

    pub fn get_listen_port(&self) -> i32 {
        self.listen_port
    }

    pub async fn produce(&self, mut req: ProduceRequest) -> Result<ProduceResponse, anyhow::Error> {
        let mut insert = self.clients.get().insert("kafka.kafka_messages_ingest")?;
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
        let addr = format!("{}:{}", self.listen_host, self.listen_port);
        let listener = TcpListener::bind(&addr).await?;
        info!("Kafka broker listening on {}", addr);
    
        loop {
            // Accept new connections
            let (socket, _addr) = listener.accept().await?;

            // Clone the Arc for this client
            let broker_clone = Arc::clone(self);

            let (_actor, _handle) = Actor::spawn(None, ClientActor, Args{
                broker: broker_clone,
                tcp_stream: socket
            }).await?;
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
        let mut query = String::from("SELECT topic, partition, key, value, offset FROM kafka.kafka_messages WHERE ");
        
        let mut conditions = Vec::new();
        
        // Create conditions for each topic and partition
        for _ in 0..total_partitions {
            conditions.push("(topic = ? AND partition = ? AND offset >= ? AND snowflakeIDToDateTime64(toUInt64(offset), 1735689600000) <= now64() - interval 5 seconds)");
        }
        
        query.push_str(&conditions.join(" OR "));
        query.push_str(" ORDER BY topic, partition, offset LIMIT 5000"); // Limit to prevent huge responses
        
        debug!("Executing ClickHouse query: {}", query);
        
        // Execute the query
        let mut query_builder = self.clients.get().query(&query);
        
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
        let query = "SELECT max(offset) as max_offset FROM kafka.kafka_messages WHERE topic = ? AND partition = ?";
        let result: Option<i64> = self.clients.get().query(query)
            .bind(topic)
            .bind(partition)
            .fetch_optional()
            .await?;
        
        Ok(result.unwrap_or(0))
    }

    pub async fn get_earliest_offset(&self, topic: &str, partition: i32) -> Result<i64, anyhow::Error> {
        let query = "SELECT min(offset) as min_offset FROM kafka.kafka_messages WHERE topic = ? AND partition = ?";
        let result: Option<i64> = self.clients.get().query(query)
            .bind(topic)
            .bind(partition)
            .fetch_optional()
            .await?;
        
        Ok(result.unwrap_or(0))
    }

    pub async fn get_offset_at_time(&self, topic: &str, partition: i32, timestamp: i64) -> Result<i64, anyhow::Error> {
        let query = "SELECT min(offset) as offset FROM kafka.kafka_messages WHERE topic = ? AND partition = ? AND ts >= fromUnixTimestamp64Milli(?)";
        let result: Option<i64> = self.clients.get().query(query)
            .bind(topic)
            .bind(partition)
            .bind(timestamp)
            .fetch_optional()
            .await?;
        
        Ok(result.unwrap_or(0))
    }

    pub async fn commit_offsets(
        &self,
        group_id: String,
        member_id: String,
        client_id: String,
        client_host: String,
        offsets: Vec<(String, i32, i64, String)>, // (topic, partition, offset, metadata)
    ) -> Result<Vec<(String, i32, i16)>, anyhow::Error> { // Returns (topic, partition, error_code)
        let mut results = Vec::new();
        let now = Utc::now();

        // Prepare batch insert with async settings
        let mut insert = self.clients.get()
            .insert("consumer_offsets")?
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        // Process each offset
        for (topic, partition, offset, metadata) in offsets {
            // Insert into ClickHouse
            let row = ConsumerOffsetInsert {
                group_id: group_id.clone(),
                topic: topic.clone(),
                partition,
                offset,
                metadata,
                member_id: member_id.clone(),
                client_id: client_id.clone(),
                client_host: client_host.clone(),
                commit_time: now,
            };

            if let Err(e) = insert.write(&row).await {
                error!("Failed to write offset commit: {}", e);
                results.push((topic, partition, 1)); // Error code 1 for general errors
                continue;
            }

            results.push((topic, partition, 0)); // Success
        }

        // Commit the batch
        if let Err(e) = insert.end().await {
            error!("Failed to commit offset batch: {}", e);
            // Mark all remaining offsets as failed
            for result in results.iter_mut() {
                if result.2 == 0 {
                    result.2 = 1;
                }
            }
        }

        Ok(results)
    }

    pub async fn fetch_offsets(
        &self,
        group_id: &str,
        topics: &[(String, Vec<i32>)], // (topic, partition_indexes)
    ) -> Result<Vec<(String, i32, i64, String)>, anyhow::Error> { // Returns (topic, partition, offset, metadata)
        let mut results = Vec::new();
        
        // Build query to get latest offsets for each topic-partition
        let query = r#"
            WITH latest_offsets AS (
                SELECT
                    topic,
                    partition,
                    argMax(offset, commit_time) as offset,
                    argMax(metadata, commit_time) as metadata
                FROM consumer_offsets
                WHERE group_id = ? 
                    AND commit_time >= now() - INTERVAL 7 DAY
                    AND (topic, partition) IN (
                        SELECT topic, partition
                        FROM (
                            SELECT arrayJoin(?) as topic, arrayJoin(?) as partition
                        )
                    )
                GROUP BY topic, partition
            )
            SELECT
                topic,
                partition,
                offset,
                metadata
            FROM latest_offsets
            ORDER BY topic, partition
        "#;

        // Prepare parameters
        let mut topic_names = Vec::new();
        let mut partition_ids = Vec::new();
        
        for (topic, partitions) in topics {
            for &partition in partitions {
                topic_names.push(topic.clone());
                partition_ids.push(partition);
            }
        }

        // Execute query
        let rows = self.clients.get()
            .query(query)
            .bind(group_id)
            .bind(&topic_names)
            .bind(&partition_ids)
            .fetch_all()
            .await?;

        for (topic, partition, offset, metadata) in rows {
            results.push((topic, partition, offset, metadata));
        }

        Ok(results)
    }
} 