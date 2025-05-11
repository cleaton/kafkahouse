use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use kafka_protocol::messages::produce_response::{TopicProduceResponse, PartitionProduceResponse, LeaderIdAndEpoch};
use clickhouse::{Client, Row};
use kafka_protocol::records::{RecordBatchDecoder, Compression};
use log::{info, debug, error};
use ractor::Actor;
use serde::{Serialize, Deserialize};
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

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct ConsumerOffsetInsert {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub commit_time: chrono::DateTime<Utc>,
}

struct ClickHouseClients {
    clients: Vec<Client>,
    next_idx: AtomicUsize,
}

impl ClickHouseClients {
    fn new(urls: &str) -> Self {
        let clients = urls.split(',')
            .map(|url| Client::default()
                .with_url(url.trim()))
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
        let mut insert = self.clients.get()
            .insert("kafka.kafka_messages_ingest")?
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");
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
        query.push_str(" ORDER BY topic, partition, offset ASC LIMIT 100 BY topic, partition LIMIT 5000"); // Limit to prevent huge responses
        
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

        info!("Committing offsets for group_id={}, member_id={}, client_id={}, offsets.len={}",
              group_id, member_id, client_id, offsets.len());

        // Process each offset individually instead of batch
        for (topic, partition, offset, metadata) in offsets {
            debug!("Inserting offset: topic={}, partition={}, offset={}", topic, partition, offset);
            
            // Create insert for just one row with async settings
            let mut insert = self.clients.get()
                .insert("kafka.consumer_offsets")?
                .with_option("async_insert", "1")
                .with_option("wait_for_async_insert", "0");
            
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

            match insert.write(&row).await {
                Ok(_) => {
                    // Commit this single insert
                    match insert.end().await {
                        Ok(_) => {
                            debug!("Successfully committed offset for topic={}, partition={}, offset={}", 
                                  topic, partition, offset);
                            results.push((topic, partition, 0)); // Success
                        },
                        Err(e) => {
                            error!("Failed to commit offset for topic={}, partition={}: {}", 
                                  topic, partition, e);
                            results.push((topic, partition, 1)); // Error code 1 for general errors
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to write offset commit for topic={}, partition={}: {}", 
                          topic, partition, e);
                    results.push((topic, partition, 1)); // Error code 1 for general errors
                }
            }
        }

        info!("Completed offset commits: {} successful, {} failed", 
             results.iter().filter(|r| r.2 == 0).count(),
             results.iter().filter(|r| r.2 != 0).count());

        Ok(results)
    }

    pub async fn list_offsets(
        &self,
        topics: &[(String, Vec<i32>)], // (topic, partition_indexes)
    ) -> Result<Vec<(String, i32, i64)>, anyhow::Error> { // Returns (topic, partition, offset)
        let mut results = Vec::new();
        
        // Transform topics into format suitable for IN clause: [(topic, partition), ...]
        let mut topic_partition_pairs = Vec::new();
        for (topic, partitions) in topics {
            for &partition in partitions {
                topic_partition_pairs.push((topic.clone(), partition));
            }
        }
        
        // Build query to get latest offsets for each topic-partition
        let query = r#"
            SELECT topic, partition, max(offset) as offset FROM kafka.kafka_messages
            WHERE (topic, partition) IN ? AND ts > now() - interval 7 day
            GROUP BY topic, partition
        "#;

        // Execute query
        let rows = self.clients.get()
            .query(query)
            .bind(&topic_partition_pairs)
            .fetch_all()
            .await?;

        for (topic, partition, offset) in rows {
            results.push((topic, partition, offset));
        }

        Ok(results)
    }

    pub async fn offset_fetch(
        &self,
        group_id: String,
        topics: &[(String, Vec<i32>)], // (topic, partition_indexes)
    ) -> Result<Vec<(String, i32, i64)>, anyhow::Error> { // Returns (topic, partition, offset)
        let mut results = Vec::new();
        
        // Transform topics into format suitable for IN clause: [(topic, partition), ...]
        let mut topic_partition_pairs = Vec::new();
        for (topic, partitions) in topics {
            for &partition in partitions {
                topic_partition_pairs.push((topic.clone(), partition));
            }
        }
        
        // Build query to get latest offsets for each topic-partition
        let query = r#"
            SELECT topic, partition, offset FROM kafka.consumer_offsets
            WHERE group_id = ?
            AND (topic, partition) IN ?
            ORDER BY group_id, topic, partition, commit_time DESC
            LIMIT 1 BY group_id, topic, partition
        "#;

        // Execute query
        let rows = self.clients.get()
            .query(query)
            .bind(&group_id)
            .bind(&topic_partition_pairs)
            .fetch_all()
            .await?;

        for (topic, partition, offset) in rows {
            results.push((topic, partition, offset));
        }

        Ok(results)
    }

    pub async fn get_partitions_offsets(
        &self,
        topic_partitions: &[(String, Vec<i32>)],
    ) -> Result<Vec<(String, i32, i64)>, anyhow::Error> { 
        // Returns (topic, partition, offset)
        if topic_partitions.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare topic and partition arrays with pre-joined format for binding
        let mut topics = Vec::new();
        let mut partitions = Vec::new();
        
        for (topic, parts) in topic_partitions {
            for &partition in parts {
                topics.push(topic.clone());
                partitions.push(partition);
            }
        }
        
        // Build query using parameters for security
        let query = r#"
            -- Manually create a table of unique topic-partition pairs
            WITH requested_pairs AS (
                SELECT 
                    topic,
                    partition
                FROM (
                    SELECT 
                        arrayJoin(?) AS topic,
                        CAST(arrayJoin(?) AS Int32) AS partition
                    -- Using array index ensures topics and partitions stay aligned
                    FROM (
                        SELECT 
                            number 
                        FROM 
                            numbers(1)
                    )
                )
            )
            -- Query results with left join to ensure all pairs are included
            SELECT 
                rp.topic, 
                rp.partition, 
                COALESCE(max(km.offset), 0) AS offset
            FROM 
                requested_pairs rp
            LEFT JOIN 
                kafka.kafka_messages km 
            ON 
                km.topic = rp.topic AND 
                km.partition = rp.partition AND
                km.ts > now() - INTERVAL 7 DAY
            GROUP BY 
                rp.topic, 
                rp.partition
            ORDER BY 
                rp.topic, 
                rp.partition
        "#;

        // Execute query with binding for security
        let rows: Vec<(String, i32, i64)> = self.clients.get()
            .query(query)
            .bind(&topics)
            .bind(&partitions)
            .fetch_all()
            .await?;

        info!("Retrieved offsets for {} topic-partitions", rows.len());
        
        Ok(rows)
    }
} 