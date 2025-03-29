use kafka_protocol::messages::api_versions_response::ApiVersion;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use bytes::BytesMut;
use std::collections::HashMap;
use kafka_protocol::messages::*;
use std::sync::{Arc, OnceLock};
use anyhow;
use log::{debug, info, error};

const MAX_MESSAGE_SIZE: i32 = 10_485_760; // 10MB


use super::client_handlers::{
    handle_api_versions, handle_fetch, handle_find_coordinator, handle_heartbeat, 
    handle_join_group, handle_leave_group, handle_list_offsets, handle_metadata, 
    handle_offset_commit, handle_offset_fetch, handle_produce, handle_sync_group
};
use super::protocol::{
    KafkaRequestMessage,
    KafkaResponseMessage,
};
use super::Broker;
use super::consumer_group::SharedConsumerGroupCache;

static SUPPORTED_API_VERSIONS: OnceLock<Vec<ApiVersion>> = OnceLock::new();

// Initialize API versions at module load
fn init_api_versions() -> &'static Vec<ApiVersion> {
    SUPPORTED_API_VERSIONS.get_or_init(|| {
        vec![
            ApiVersion::default()
                .with_api_key(ApiKey::ApiVersions as i16)
                .with_min_version(0)
                .with_max_version(3),
            ApiVersion::default()
                .with_api_key(ApiKey::Metadata as i16)
                .with_min_version(0)
                .with_max_version(12),
            ApiVersion::default()
                .with_api_key(ApiKey::Produce as i16)
                .with_min_version(0)
                .with_max_version(9),
            ApiVersion::default()
                .with_api_key(ApiKey::FindCoordinator as i16)
                .with_min_version(0)
                .with_max_version(4),
            ApiVersion::default()
                .with_api_key(ApiKey::ListOffsets as i16)
                .with_min_version(0)
                .with_max_version(7),
            ApiVersion::default()
                .with_api_key(ApiKey::Fetch as i16)
                .with_min_version(0)
                .with_max_version(13),
            ApiVersion::default()
                .with_api_key(ApiKey::JoinGroup as i16)
                .with_min_version(0)
                .with_max_version(9),
            // Add more essential APIs
            ApiVersion::default()
                .with_api_key(ApiKey::InitProducerId as i16)
                .with_min_version(0)
                .with_max_version(4),
            ApiVersion::default()
                .with_api_key(ApiKey::AddPartitionsToTxn as i16)
                .with_min_version(0)
                .with_max_version(3),
            ApiVersion::default()
                .with_api_key(ApiKey::AddOffsetsToTxn as i16)
                .with_min_version(0)
                .with_max_version(3),
            ApiVersion::default()
                .with_api_key(ApiKey::Heartbeat as i16)
                .with_min_version(0)
                .with_max_version(4),
            ApiVersion::default()
                .with_api_key(ApiKey::SyncGroup as i16)
                .with_min_version(0)
                .with_max_version(5),
            ApiVersion::default()
                .with_api_key(ApiKey::LeaveGroup as i16)
                .with_min_version(0)
                .with_max_version(4),
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetCommit as i16)
                .with_min_version(0)
                .with_max_version(8),
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetFetch as i16)
                .with_min_version(0)
                .with_max_version(7),
            ApiVersion::default()
                .with_api_key(ApiKey::SaslHandshake as i16)
                .with_min_version(0)
                .with_max_version(1),
        ]
    })
}

pub struct KafkaClient {
    // Basic broker information
    pub(crate) broker_id: i32,
    pub(crate) cluster_id: String,
    pub(crate) controller_id: Option<i32>,
    
    // Topic metadata
    pub(crate) topics: HashMap<String, Vec<TopicPartition>>, // topic name -> partitions
    
    // Consumer group information
    pub(crate) consumer_groups: HashMap<String, HashMap<String, i32>>, // group_id -> (member_id -> generation_id)
    
    // Consumer group cache
    pub(crate) consumer_group_cache: Arc<SharedConsumerGroupCache>,
    
    // Subscribed topics for each consumer group member
    pub(crate) member_subscriptions: HashMap<String, Vec<String>>, // member_id -> [topic]
    
    // Committed offsets
    pub(crate) committed_offsets: HashMap<String, HashMap<String, HashMap<i32, i64>>>, // group_id -> (topic -> (partition -> offset))
    
    // Supported API versions
    pub(crate) supported_api_versions: &'static Vec<ApiVersion>,
    pub(crate) broker: Arc<Broker>,
}

pub(crate) struct TopicPartition {
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

async fn read_loop(request_tx: mpsc::Sender<KafkaRequestMessage>, tcp_reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<(), String> {
    let mut read_buffer = BytesMut::with_capacity(4096);
    loop {
        debug!("Waiting for message size...");
        let message_size = tcp_reader.read_i32().await.map_err(|e| e.to_string())?;
        
        if message_size <= 0 || message_size > MAX_MESSAGE_SIZE {
            return Err(format!("Invalid message size: {}", message_size));
        }
        let message_size = message_size as usize;

        if message_size > read_buffer.len() {
            read_buffer.resize(message_size, 0);
        }

        debug!("Reading message payload of size {}", message_size);
        match tcp_reader.read_exact(&mut read_buffer[..message_size]).await {
            Ok(_) => debug!("Successfully read payload"),
            Err(e) => return Err(format!("Failed to read payload: {}", e)),
        }

        debug!("Read buffer length: {:?}", read_buffer.len());

        // Process the message;
        match KafkaRequestMessage::decode(&mut read_buffer.split_to(message_size).freeze()) {
            Ok(request) => {
                info!("Decoded request: ApiKey={:?}, Version={}, CorrelationId={}", 
                    request.api_key, 
                    request.header.request_api_version,
                    request.header.correlation_id);
                request_tx.send(request).await.map_err(|e| e.to_string())?;
                // Only clear the buffer if decoding was successful
                read_buffer.clear();
            }
            Err(e) => {
                error!("Failed to decode message: {}", e);
                // Log the buffer content for debugging
                debug!("Buffer content that failed to decode: {:?}", read_buffer);
                return Err(format!("Failed to decode message: {}", e));
            }
        }
    }
}

impl KafkaClient {
    pub fn new(broker: Arc<Broker>) -> Self {
        // Use the broker's shared consumer group cache
        let consumer_group_cache = broker.consumer_group_cache();
        
        Self {
            broker_id: 0,
            cluster_id: "test-cluster".to_string(),
            controller_id: Some(0),
            topics: HashMap::new(),
            consumer_groups: HashMap::new(),
            consumer_group_cache,
            member_subscriptions: HashMap::new(),
            committed_offsets: HashMap::new(),
            supported_api_versions: init_api_versions(),
            broker,
        }
    }

    pub async fn run(&mut self, tcp_stream: TcpStream) {
        let (mut tcp_reader, mut tcp_writer) = tcp_stream.into_split();
        let (request_tx, mut request_rx) = mpsc::channel::<KafkaRequestMessage>(100);
        tokio::spawn(async move {
            if let Err(e) = read_loop(request_tx.clone(), &mut tcp_reader).await {
                error!("Error in read loop: {:?}", e);
            }
            drop(request_tx);
        });
        let mut response_buffer = BytesMut::with_capacity(4096);
        while let Some(request) = request_rx.recv().await {
            let response = match self.handle_request(request).await {
                Ok(response) => response,
                Err(e) => {
                    error!("Error handling request: {:?}", e);
                    continue;
                }
            };

            response_buffer.clear();
            
            
            // Encode response after size field
            if let Err(e) = response.encode(&mut response_buffer).map_err(|e| e.to_string()) {
                error!("Error encoding response: {:?}", e);
                continue;
            }

            // After encoding the response
            debug!("Response encoded successfully. Size: {}, First 20 bytes: {:?}", 
                response_buffer.len(),
                &response_buffer[0..std::cmp::min(20, response_buffer.len())]);

            // Write entire message atomically
            if let Err(e) = tcp_writer.write_all(&response_buffer).await {
                error!("Error writing response: {:?}", e);
                break;
            }
            // Add flush to ensure the response is sent
            if let Err(e) = tcp_writer.flush().await {
                error!("Error flushing response: {:?}", e);
                break;
            }
        }
        
        drop(tcp_writer); // Close the writer to signal the reader loop to stop
        request_rx.close();
        ()
    }

    async fn handle_request(&mut self, request: KafkaRequestMessage) -> Result<KafkaResponseMessage, anyhow::Error> {
        let api_version = request.header.request_api_version;
        let (response, response_size) = match &request.request {
            RequestKind::ApiVersions(req) => handle_api_versions(self, req, api_version),
            RequestKind::Metadata(req) => handle_metadata(self, req, api_version),
            RequestKind::Produce(req) => handle_produce(self, req, api_version).await,
            RequestKind::Fetch(req) => handle_fetch(self, req, api_version).await,
            RequestKind::FindCoordinator(req) => handle_find_coordinator(self, req, api_version),
            RequestKind::ListOffsets(req) => handle_list_offsets(self, req, api_version).await,
            RequestKind::JoinGroup(req) => handle_join_group(self, req, api_version).await,
            RequestKind::SyncGroup(req) => handle_sync_group(self, req, api_version).await,
            RequestKind::Heartbeat(req) => handle_heartbeat(self, req, api_version).await,
            RequestKind::LeaveGroup(req) => handle_leave_group(self, req, api_version).await,
            RequestKind::OffsetCommit(req) => handle_offset_commit(self, req, api_version).await,
            RequestKind::OffsetFetch(req) => handle_offset_fetch(self, req, api_version).await,
            // Add more handlers as needed
            _ => {
                return Err(anyhow::anyhow!("Unsupported request type: {:?}", request.api_key));
            }
        }?;

        Ok(KafkaResponseMessage {
            request_header: request.header,
            api_key: request.api_key,
            response,
            response_size,
        })
    }
}