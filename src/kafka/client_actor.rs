use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use anyhow;
use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::protocol::Encodable;
use log::{debug, error, info};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream};

use super::Broker;
use super::client_handlers::*;
use super::client_types::*;
use super::consumer_group::ConsumerGroups;
use super::protocol::{KafkaRequestMessage, KafkaResponseMessage};

// Constants
const MAX_MESSAGE_SIZE: i32 = 10_485_760; // 10MB

// Initialize API versions using OnceLock
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

pub struct ClientActor;

pub struct ClientState {
    pub broker: Arc<Broker>,
    pub broker_id: i32,
    pub cluster_id: String,
    pub controller_id: Option<i32>,
    pub client_id: String,
    pub client_host: String,
    tcp_reader: OwnedReadHalf,
    tcp_writer: OwnedWriteHalf,
    
    // Topic metadata
    pub topics: HashMap<String, Vec<TopicPartition>>, // topic name -> partitions
    
    // Group membership
    pub joined_groups: JoinedGroups,
    
    // Consumer group cache
    pub consumer_group_cache: Arc<ConsumerGroups>,
    
    // Committed offsets
    pub committed_offsets: HashMap<String, HashMap<String, HashMap<i32, i64>>>, // group_id -> (topic -> (partition -> offset))
    
    // Supported API versions
    pub supported_api_versions: &'static Vec<ApiVersion>,
    
    // Message buffers
    read_buffer: BytesMut,
    response_buffer: BytesMut,
}

#[derive(Debug, Clone)]
pub(crate) struct TopicPartition {
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

pub enum Message {
    ReadRequest,
    ProcessRequest(KafkaRequestMessage),
    SendResponse(KafkaResponseMessage),
}

pub struct Args {
    pub tcp_stream: TcpStream,
    pub broker: Arc<Broker>,
}

impl Actor for ClientActor {
    type Msg = Message;
    type State = ClientState;
    type Arguments = Args;
    
    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        let (tcp_reader, tcp_writer) = args.tcp_stream.into_split();
        let client_host = tcp_writer.peer_addr().map_or_else(
            |_| "".to_string(), 
            |peer| peer.ip().to_string() + ":" + &peer.port().to_string()
        );
        
        let consumer_group_cache = args.broker.consumer_group_cache();
        
        // Create initialized state
        let state = ClientState {
            broker: args.broker,
            broker_id: 0,
            cluster_id: "test-cluster".to_string(),
            controller_id: Some(0),
            client_id: "".to_string(),
            client_host,
            tcp_reader,
            tcp_writer,
            topics: HashMap::new(),
            joined_groups: HashMap::new(),
            consumer_group_cache,
            committed_offsets: HashMap::new(),
            supported_api_versions: init_api_versions(),
            read_buffer: BytesMut::with_capacity(4096),
            response_buffer: BytesMut::with_capacity(4096),
        };
        
        // Begin reading from TCP stream
        let _ = myself.send_message(Message::ReadRequest);
        
        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            Message::ReadRequest => {
                // Read message size
                match state.tcp_reader.read_i32().await {
                    Ok(message_size) => {
                        if message_size <= 0 || message_size > MAX_MESSAGE_SIZE {
                            error!("Invalid message size: {}", message_size);
                            return Ok(());
                        }
                        
                        let message_size = message_size as usize;
                        debug!("Reading message payload of size {}", message_size);
                        
                        // Ensure buffer has enough capacity
                        if message_size > state.read_buffer.len() {
                            state.read_buffer.resize(message_size, 0);
                        }
                        
                        // Read message payload
                        match state.tcp_reader.read_exact(&mut state.read_buffer[..message_size]).await {
                            Ok(_) => {
                                debug!("Successfully read payload");
                                
                                // Decode message
                                match KafkaRequestMessage::decode(&mut state.read_buffer.split_to(message_size).freeze()) {
                                    Ok(request) => {
                                        info!("Decoded request: ApiKey={:?}, Version={}, CorrelationId={}", 
                                            request.api_key, 
                                            request.header.request_api_version,
                                            request.header.correlation_id);
                                        
                                        // Process the request
                                        let _ = myself.send_message(Message::ProcessRequest(request));
                                    }
                                    Err(e) => {
                                        error!("Failed to decode message: {}", e);
                                        debug!("Buffer content that failed to decode: {:?}", state.read_buffer);
                                        
                                        // Continue reading despite decode error
                                        state.read_buffer.clear();
                                        let _ = myself.send_message(Message::ReadRequest);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to read payload: {}", e);
                                return Ok(());
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error reading message size: {:?}", e);
                        return Ok(());
                    }
                }
            },
            
            Message::ProcessRequest(request) => {
                // Update client_id if available
                if state.client_id.is_empty() && request.header.client_id.is_some() {
                    state.client_id = request.header.client_id.clone().unwrap().to_string();
                }
                
                // Process the request
                match process_request(state, request).await {
                    Ok(response) => {
                        let _ = myself.send_message(Message::SendResponse(response));
                    }
                    Err(e) => {
                        error!("Error handling request: {:?}", e);
                        // Continue reading despite processing error
                        let _ = myself.send_message(Message::ReadRequest);
                    }
                }
            },
            
            Message::SendResponse(response) => {
                state.response_buffer.clear();
                
                // Encode response
                if let Err(e) = response.encode(&mut state.response_buffer).map_err(|e| e.to_string()) {
                    error!("Error encoding response: {:?}", e);
                    // Continue reading despite encoding error
                    let _ = myself.send_message(Message::ReadRequest);
                    return Ok(());
                }

                debug!("Response encoded successfully. Size: {}", state.response_buffer.len());

                // Write response to client
                if let Err(e) = state.tcp_writer.write_all(&state.response_buffer).await {
                    error!("Error writing response: {:?}", e);
                    return Ok(());
                }
                
                // Ensure data is sent
                if let Err(e) = state.tcp_writer.flush().await {
                    error!("Error flushing response: {:?}", e);
                    return Ok(());
                }
                
                // Schedule next read
                let _ = myself.send_message(Message::ReadRequest);
            }
        }
        
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Clean up resources if needed
        Ok(())
    }
}

// Process a Kafka request and produce a response
async fn process_request(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Handle API versions requests directly
    let (response, response_size) = if request.api_key == ApiKey::ApiVersions {
        // Manually create the ApiVersionsResponse
        let mut response = api_versions_response::ApiVersionsResponse::default();
        response.error_code = 0;
        response.api_keys = state.supported_api_versions.to_vec();
        response.throttle_time_ms = 0;
        
        // Additional fields for v3+
        if api_version >= 3 {
            response.finalized_features_epoch = 0;
            response.finalized_features = vec![];
            response.supported_features = vec![];
            response.zk_migration_ready = false;
        }
        
        let response_size = response.compute_size(api_version)? as i32;
        (ResponseKind::ApiVersions(response), response_size)
    } else {
        match &request.request {
            RequestKind::Metadata(req) => handle_metadata(state, req, api_version)?,
            RequestKind::Produce(req) => handle_produce(state, req, api_version).await?,
            RequestKind::Fetch(req) => handle_fetch(state, req, api_version).await?,
            RequestKind::FindCoordinator(req) => handle_find_coordinator(state, req, api_version)?,
            RequestKind::ListOffsets(req) => handle_list_offsets(state, req, api_version).await?,
            RequestKind::JoinGroup(req) => handle_join_group(state, req, api_version).await?,
            RequestKind::SyncGroup(req) => handle_sync_group(state, req, api_version).await?,
            RequestKind::Heartbeat(req) => handle_heartbeat(state, req, api_version).await?,
            RequestKind::LeaveGroup(req) => handle_leave_group(state, req, api_version).await?,
            RequestKind::OffsetCommit(req) => handle_offset_commit(state, req, api_version).await?,
            RequestKind::OffsetFetch(req) => handle_offset_fetch(state, req, api_version).await?,
            _ => {
                return Err(anyhow::anyhow!("Unsupported request type: {:?}", request.api_key));
            }
        }
    };

    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response,
        response_size,
    })
}