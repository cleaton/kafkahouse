use anyhow::Result;
use clickhouse::{Client, Row};
use serde::Serialize;
use tracing::{info, warn};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use std::thread;

#[derive(Row, Serialize, serde::Deserialize)]
pub struct Message {
    pub topic: String,
    pub partition: u32,
    pub key: String,
    pub value: String,
}

type MessageBatch = (String, u32, Vec<(String, String)>);

struct InsertRequest {
    batches: Vec<MessageBatch>,
    response: oneshot::Sender<Result<u64>>,
}

pub struct MessageFetchTopicPartitionRequest {
    pub topic: String,
    pub partition: u32,
    pub start_offset: i64,
    pub max_bytes: i32,
}

pub struct MessageFetchRequest {
    pub topics: Vec<MessageFetchTopicPartitionRequest>,
    pub max_bytes: i32,
}

#[derive(Row, serde::Deserialize)]
struct MessageRow {
    topic: String,
    partition: u32,
    offset: i64,
    timestamp: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct MessageFetchTopicPartitionData {
    pub key: String,
    pub value: String,
    pub offset: i64,
    pub timestamp: i64,
}

pub struct MessageFetchTopicPartitionResponse {
    pub topic: String,
    pub partition: u32,
    pub data: Vec<MessageFetchTopicPartitionData>,
    pub high_watermark: i64,
    pub log_start_offset: i64,
}

pub struct MessageFetchResponse {
    pub topics: Vec<MessageFetchTopicPartitionResponse>,
}

impl Default for MessageFetchResponse {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
        }
    }
}

pub struct MessageStore {
    sender: mpsc::Sender<InsertRequest>,
    client: Client,
}

impl MessageStore {
    pub fn new(url: &str) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<InsertRequest>(1000);
        let client = Client::default()
            .with_url(url)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");
        let client_clone = client.clone();

        // Spawn the worker task
        tokio::spawn(async move {
            let mut pending_requests = Vec::new();
            
            while let Some(first_request) = receiver.recv().await {
                // Got first request, now collect all pending requests without waiting
                pending_requests.push(first_request);
                
                // Drain the channel
                while let Ok(request) = receiver.try_recv() {
                    pending_requests.push(request);
                }

                let start = Instant::now();
                let request_count = pending_requests.len();

                // Collect all batches into a single vec
                let all_batches: Vec<MessageBatch> = pending_requests
                    .iter()
                    .flat_map(|req| req.batches.iter().cloned())
                    .collect();

                // Process all batches in one insert
                let result = Self::process_insert(&client, all_batches).await;

                // Send result to all waiting requests
                if let Ok(count) = result.as_ref() {
                    let duration = start.elapsed();
                    info!(
                        thread_id = ?thread::current().id(),
                        request_count = request_count,
                        "Inserted {} messages in {:?}, {} messages/sec, {} requests",
                        count,
                        duration,
                        *count as f64 / duration.as_secs_f64(),
                        request_count
                    );
                }

                // Send responses
                for request in pending_requests.drain(..) {
                    let individual_count = request.batches.iter()
                        .map(|(_, _, msgs)| msgs.len() as u64)
                        .sum();
                    let _ = request.response.send(result.as_ref().map(|_| individual_count).map_err(|e| anyhow::anyhow!(e.to_string())));
                }
            }
        });

        Ok(Self { sender, client: client_clone })
    }

    async fn process_insert(client: &Client, batches: Vec<MessageBatch>) -> Result<u64> {
        let total_size: usize = batches.iter().map(|(_, _, msgs)| msgs.len()).sum();
        let mut messages = Vec::with_capacity(total_size);

        // Convert batches into Message structs
        for (topic, partition, batch_messages) in batches {
            for (key, value) in batch_messages {
                messages.push(Message {
                    topic: topic.clone(),
                    partition,
                    key,
                    value
                });
            }
        }

        // Create a single insert operation and write all messages
        let mut insert = client.insert("messages")?;
        for msg in messages {
            insert.write(&msg).await?;
        }
        insert.end().await?;

        Ok(total_size as u64)
    }

    pub async fn append_message_batches(&self, batches: Vec<MessageBatch>) -> Result<u64> {
        let (response_tx, response_rx) = oneshot::channel();
        
        self.sender.send(InsertRequest {
            batches,
            response: response_tx,
        }).await.map_err(|_| anyhow::anyhow!("Worker task terminated"))?;

        response_rx.await.map_err(|_| anyhow::anyhow!("Worker task terminated"))?
    }

    pub async fn get_offsets(&self, topic: &str, partitions: &[u32], timestamp: i64) -> Result<Vec<(u32, i64)>> {
        // Convert partitions to comma-separated string
        let partition_list = partitions.iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let query = if timestamp == -1 {
            format!(
                "SELECT partition, COALESCE(MAX(offset), -1) as next_offset \
                 FROM messages \
                 WHERE topic = '{}' AND partition IN ({}) \
                 GROUP BY partition",
                topic, partition_list
            )
        } else if timestamp == -2 {  // Earliest offset
            format!(
                "SELECT partition, COALESCE(MIN(offset), 0) as next_offset \
                 FROM messages \
                 WHERE topic = '{}' AND partition IN ({}) \
                 GROUP BY partition",
                topic, partition_list
            )
        } else {
            format!(
                "SELECT partition, COALESCE(MAX(offset), -1) as next_offset \
                 FROM messages \
                 WHERE topic = '{}' AND partition IN ({}) \
                 AND ts <= {} \
                 GROUP BY partition",
                topic, partition_list, timestamp
            )
        };

        #[derive(Row, serde::Deserialize)]
        struct PartitionOffset {
            partition: u32,
            next_offset: i64,
        }

        let rows: Vec<PartitionOffset> = self.client.query(&query).fetch_all().await?;
        let mut partition_offsets = Vec::new();
        
        for row in rows {
            // For latest offset (-1), if we got -1 back it means no messages exist
            // In this case we should return 0 as both the earliest and latest offset
            let offset = if timestamp == -1 && row.next_offset == -1 {
                0
            } else {
                row.next_offset
            };
            partition_offsets.push((row.partition, offset));
        }

        // Fill in missing partitions with offset 0
        let mut result = Vec::with_capacity(partitions.len());
        for &partition in partitions {
            match partition_offsets.iter().find(|(p, _)| *p == partition) {
                Some(&(_, offset)) => result.push((partition, offset)),
                None => result.push((partition, 0)),
            }
        }

        Ok(result)
    }

    pub async fn get_earliest_offset(&self, topic: &str, partition: u32) -> Result<i64> {
        let result = self.get_offsets(topic, &[partition], 0).await?;
        Ok(result.first().map(|&(_, offset)| offset).unwrap_or(0))
    }

    pub async fn get_latest_offset(&self, topic: &str, partition: u32) -> Result<i64> {
        let result = self.get_offsets(topic, &[partition], -1).await?;
        Ok(result.first().map(|&(_, offset)| offset).unwrap_or(0))
    }

    pub async fn get_messages(&self, request: MessageFetchRequest) -> Result<MessageFetchResponse> {
        warn!(
            "Starting get_messages for {} topic requests",
            request.topics.len()
        );

        let mut response = MessageFetchResponse::default();

        if request.topics.is_empty() {
            return Ok(response);
        }

        // Build the query with parameter placeholders
        let mut watermark_conditions = Vec::new();
        let mut message_conditions = Vec::new();
        
        for _ in &request.topics {
            watermark_conditions.push("(topic = ? AND partition = ?)");
            message_conditions.push("(topic = ? AND partition = ? AND offset >= ?)");
        }

        let query = format!(
            "WITH watermarks AS (
                SELECT 
                    topic,
                    partition,
                    COALESCE(MIN(offset), 0) as log_start_offset,
                    COALESCE(MAX(offset), -1) as high_watermark
                FROM messages
                WHERE ts >= now() - INTERVAL 7 DAYS AND ({}) 
                GROUP BY topic, partition
            )
            SELECT 
                m.topic as topic,
                m.partition as partition,
                m.offset as offset,
                m.ts as timestamp,
                m.key as key,
                m.value as value,
                w.log_start_offset as log_start_offset,
                w.high_watermark as high_watermark
            FROM messages m
            JOIN watermarks w ON m.topic = w.topic AND m.partition = w.partition
            WHERE timestamp >= now() - INTERVAL 7 DAYS AND ({})
            ORDER BY m.topic, m.partition, m.offset
            LIMIT ?",
            watermark_conditions.join(" OR "),
            message_conditions.join(" OR ")
        );

        // Build the query with bindings
        let mut query_builder = self.client.query(&query);

        // Bind parameters for watermark conditions
        for req in &request.topics {
            query_builder = query_builder.bind(&req.topic);
            query_builder = query_builder.bind(req.partition);
        }

        // Bind parameters for message conditions
        for req in &request.topics {
            query_builder = query_builder.bind(&req.topic);
            query_builder = query_builder.bind(req.partition);
            query_builder = query_builder.bind(req.start_offset);
        }

        // Bind the LIMIT parameter
        query_builder = query_builder.bind(10000);

        warn!("About to execute query in get_messages");

        #[derive(Row, serde::Deserialize)]
        struct MessageRowWithWatermarks {
            topic: String,
            partition: u32,
            offset: i64,
            timestamp: i64,
            key: String,
            value: String,
            log_start_offset: i64,
            high_watermark: i64,
        }

        // Execute query
        let rows: Vec<MessageRowWithWatermarks> = query_builder.fetch_all().await?;

        // Group messages by topic and partition
        let mut messages_by_topic_partition: std::collections::HashMap<(String, u32), (Vec<MessageFetchTopicPartitionData>, i64, i64)> = 
            std::collections::HashMap::new();

        warn!("Got {} rows from ClickHouse query", rows.len());

        for row in rows {
            let message_data = MessageFetchTopicPartitionData {
                key: row.key,
                value: row.value,
                offset: row.offset,
                timestamp: row.timestamp,
            };

            messages_by_topic_partition
                .entry((row.topic.clone(), row.partition))
                .or_insert((Vec::new(), row.log_start_offset, row.high_watermark))
                .0.push(message_data);
        }

        warn!("Grouped messages into {} topic-partitions", messages_by_topic_partition.len());

        // Build response for each requested topic partition
        for topic_request in &request.topics {
            let (messages, log_start_offset, high_watermark) = messages_by_topic_partition
                .get(&(topic_request.topic.clone(), topic_request.partition))
                .map(|(msgs, low, high)| (msgs.clone(), *low, *high))
                .unwrap_or_default();

            warn!(
                "get_messages response for topic={} partition={}: requested_offset={}, log_start_offset={}, high_watermark={}, messages_count={}, message_offsets={:?}",
                topic_request.topic,
                topic_request.partition,
                topic_request.start_offset,
                log_start_offset,
                if high_watermark == -1 { 0 } else { high_watermark + 1 },
                messages.len(),
                messages.iter().map(|m| m.offset).collect::<Vec<_>>()
            );

            response.topics.push(MessageFetchTopicPartitionResponse {
                topic: topic_request.topic.clone(),
                partition: topic_request.partition,
                data: messages,
                high_watermark: if high_watermark == -1 { 0 } else { high_watermark + 1 },
                log_start_offset,
            });
        }

        Ok(response)
    }
} 