use anyhow::Result;
use clickhouse::{Client, Row};
use serde::Serialize;
use std::time::Duration;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::rt::TokioExecutor;
use futures::future::join_all;

const INSERT_BATCH_SIZE: usize = 10_000;
const MAX_EXECUTION_TIME: u64 = 60;
const POOL_MAX_IDLE: usize = 32;
const POOL_IDLE_TIMEOUT_MS: u64 = 2_500;

#[derive(Row, Serialize)]
struct Message {
    topic: String,
    partition: u32,
    data: String,
    offset: u64,
}

// TODO: Implement message storage in ClickHouse
pub struct MessageStore {
    client: Client,
}

impl MessageStore {
    pub fn new(url: &str) -> Result<Self> {
        // Configure HTTP client with connection pooling
        let connector = HttpConnector::new();
        let hyper_client = HyperClient::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_millis(POOL_IDLE_TIMEOUT_MS))
            .pool_max_idle_per_host(POOL_MAX_IDLE)
            .build(connector);

        // Configure ClickHouse client
        let client = Client::with_http_client(hyper_client)
            .with_url(url)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0")
            .with_option("async_insert_busy_timeout_ms", "500")
            .with_option("max_insert_block_size", "10000")
            .with_option("min_insert_block_size_rows", "10000")
            .with_option("min_insert_block_size_bytes", "100000")
            .with_option("max_execution_time", MAX_EXECUTION_TIME.to_string())
            .with_option("max_threads", "8")
            .with_option("max_insert_threads", "8")
            .with_option("max_compress_block_size", "1048576");

        Ok(Self { client })
    }

    pub async fn append_message(&self, topic: &str, partition: u32, data: &str) -> Result<u64> {
        let mut insert = self.client.insert("messages")?;
        
        insert.write(&Message { 
            topic: topic.to_string(), 
            partition, 
            data: data.to_string(),
            offset: 0,
        }).await?;
        
        insert.end().await?;
        Ok(0)
    }

    pub async fn append_messages(&self, topic: &str, partition: u32, data: Vec<String>) -> Result<u64> {
        let mut insert = self.client.insert("messages")?;
        
        for msg in data {
            insert.write(&Message { 
                topic: topic.to_string(), 
                partition, 
                data: msg,
                offset: 0,
            }).await?;
        }
        
        insert.end().await?;
        Ok(0)
    }

    pub async fn append_message_batches(&self, batches: Vec<(String, u32, Vec<String>)>) -> Result<u64> {
        let mut total_messages = 0;
        let mut insert = self.client.insert("messages")?;
        
        for (topic, partition, messages) in batches {
            let topic = topic.clone(); // Clone once per batch instead of per message
            for msg in messages {
                insert.write(&Message {
                    topic: topic.clone(),
                    partition,
                    data: msg,
                    offset: total_messages,
                }).await?;
                total_messages += 1;
            }
        }
        
        insert.end().await?;
        Ok(total_messages as u64)
    }

    pub async fn get_message(&self, topic: &str, partition: u32, offset: u64) -> Result<Option<String>> {
        let mut cursor = self.client
            .query("SELECT data FROM messages WHERE topic = ? AND partition = ? AND offset = ?")
            .bind(topic)
            .bind(partition)
            .bind(offset)
            .fetch::<String>()?;

        Ok(cursor.next().await?)
    }
} 