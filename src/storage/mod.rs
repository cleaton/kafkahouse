use anyhow::Result;
use clickhouse::{Client, Row};
use serde::Serialize;

#[derive(Row, Serialize)]
struct Message {
    topic: String,
    partition: u32,
    data: String,
}

// TODO: Implement message storage in ClickHouse
pub struct MessageStore {
    client: Client,
}

impl MessageStore {
    pub fn new(url: &str) -> Result<Self> {
        let client = Client::default()
            .with_url(url)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");
        Ok(Self { client })
    }

    pub async fn append_message(&self, topic: &str, partition: u32, data: &str) -> Result<u64> {
        let mut insert = self.client.insert("messages")?;
        
        insert.write(&Message { 
            topic: topic.to_string(), 
            partition, 
            data: data.to_string(),
        }).await?;
        
        insert.end().await?;
        Ok(0)
    }

    // NEW METHOD: append a batch of messages as one record batch
    pub async fn append_messages(&self, topic: &str, partition: u32, data: Vec<String>) -> Result<u64> {
        let mut insert = self.client.insert("messages")?;
        
        for msg in data {
            insert.write(&Message { 
                topic: topic.to_string(), 
                partition, 
                data: msg,
            }).await?;
        }
        
        insert.end().await?;
        Ok(0)
    }

    pub async fn append_message_batches(&self, batches: Vec<(String, u32, Vec<String>)>) -> Result<u64> {
        let mut insert = self.client.insert("messages")?;
        
        for (topic, partition, messages) in batches {
            for msg in messages {
                insert.write(&Message {
                    topic: topic.clone(),
                    partition,
                    data: msg,
                }).await?;
            }
        }
        
        insert.end().await?;
        Ok(0)
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