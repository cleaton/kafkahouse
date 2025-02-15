use anyhow::Result;
use clickhouse::{Client, Row};
use serde::Serialize;
use tracing::info;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use std::thread;

#[derive(Row, Serialize)]
struct Message {
    topic: String,
    partition: u32,
    data: String,
}

type MessageBatch = (String, u32, Vec<String>);

struct InsertRequest {
    batches: Vec<MessageBatch>,
    response: oneshot::Sender<Result<u64>>,
}

pub struct MessageStore {
    sender: mpsc::Sender<InsertRequest>,
}

impl MessageStore {
    pub fn new(url: &str) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<InsertRequest>(1000);
        let client = Client::default()
            .with_url(url)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "1");

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

        Ok(Self { sender })
    }

    async fn process_insert(client: &Client, batches: Vec<MessageBatch>) -> Result<u64> {
        let total_size: usize = batches.iter().map(|(_, _, msgs)| msgs.len()).sum();
        let mut messages = Vec::with_capacity(total_size);

        // Convert batches into Message structs
        for (topic, partition, batch_messages) in batches {
            for msg in batch_messages {
                messages.push(Message {
                    topic: topic.clone(),
                    partition,
                    data: msg,
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
} 