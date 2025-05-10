use std::sync::{Arc, OnceLock};

use anyhow;
use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use log::{debug, error};
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::kafka::broker::Broker;
use super::handlers::*;
use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};
use super::types::ClientState;

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
        
        // Create initialized state
        let state = ClientState {
            broker: args.broker.clone(),
            broker_id: 0,
            cluster_id: "test-cluster".to_string(),
            controller_id: Some(0),
            client_id: "".to_string(),
            client_host,
            tcp_reader,
            tcp_writer,
            topics: std::collections::HashMap::new(),
            joined_groups: std::collections::HashMap::new(),
            committed_offsets: std::collections::HashMap::new(),
            supported_api_versions: init_api_versions(),
            read_buffer: BytesMut::with_capacity(4096),
            response_buffer: BytesMut::with_capacity(4096),
            consumer_groups_api: args.broker.get_consumer_groups_api(),
            active_groups: std::collections::HashMap::new(),
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
                let message_size = match state.tcp_reader.read_i32().await {
                    Ok(size) if size <= 0 || size > MAX_MESSAGE_SIZE => {
                        error!("Invalid message size: {}", size);
                        return Ok(());
                    }
                    Ok(size) => size,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(e) => {
                        error!("Error reading message size: {:?}", e);
                        return Ok(());
                    }
                };

                // Reuse read_buffer, clear and ensure capacity
                state.read_buffer.clear();
                state.read_buffer.reserve(message_size as usize);
                state.read_buffer.resize(message_size as usize, 0);

                if let Err(e) = state.tcp_reader.read_exact(&mut state.read_buffer).await {
                    error!("Error reading message body: {:?}", e);
                    return Ok(());
                }

                // TODO: avoid clone...
                if let Ok(request) = KafkaRequestMessage::decode(&mut state.read_buffer.clone().freeze()) {
                    debug!("Received request with correlation_id: {}", request.header.correlation_id);
                    if let Some(client_id) = &request.header.client_id {
                        if state.client_id.is_empty() {
                            state.client_id = client_id.to_string();
                            debug!("Client connected: {} from {}", state.client_id, state.client_host);
                        }
                    }
                    
                    // Process request immediately
                    match process_request(state, request).await {
                        Ok(response) => {
                            // Send response immediately
                            if let Err(e) = Self::send_response(state, response).await {
                                error!("Failed to send response: {:?}", e);
                            }
                        }
                        Err(e) => {
                            error!("Error processing request: {:?}", e);
                        }
                    }
                }

                // Continue reading next request
                if let Err(e) = myself.send_message(Message::ReadRequest) {
                    error!("Failed to schedule next read: {:?}", e);
                    return Err(anyhow::anyhow!("Failed to schedule next read: {}", e).into());
                }
            }
            
            Message::ProcessRequest(_) => {
                // This message type is no longer used since we process inline
                debug!("Ignoring ProcessRequest message as we now process inline");
            }
            
            Message::SendResponse(_) => {
                // This message type is no longer used since we send responses inline
                debug!("Ignoring SendResponse message as we now send inline");
            }
        }
        
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        debug!("Client disconnected: {}", _state.client_id);
        Ok(())
    }
}

async fn process_request(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    debug!("Processing request: api_key={}, api_version={}, correlation_id={}", 
           request.header.request_api_key, request.header.request_api_version, 
           request.header.correlation_id);
    
    let api_key = ApiKey::try_from(request.header.request_api_key)
        .map_err(|_| anyhow::anyhow!("Invalid API key: {}", request.header.request_api_key))?;
    
    match api_key {
        ApiKey::ApiVersions => handle_api_versions(state, request).await,
        ApiKey::Metadata => handle_metadata(state, request).await,
        ApiKey::Fetch => handle_fetch(state, request).await,
        ApiKey::FindCoordinator => handle_find_coordinator(state, request).await,
        ApiKey::JoinGroup => handle_join_group(state, request).await,
        ApiKey::SyncGroup => handle_sync_group(state, request).await,
        ApiKey::Heartbeat  => handle_heartbeat(state, request).await,
        ApiKey::LeaveGroup  => handle_leave_group(state, request).await,
        ApiKey::ListOffsets => handle_list_offsets(state, request).await,
        ApiKey::OffsetCommit  => handle_offset_commit(state, request).await,
        ApiKey::OffsetFetch  => handle_offset_fetch(state, request).await,
        ApiKey::Produce => handle_produce(state, request).await,
        _ => Err(anyhow::anyhow!("Unsupported API key: {}", request.header.request_api_key))
    }
}

impl ClientActor {
    async fn send_response(state: &mut ClientState, response: KafkaResponseMessage) -> Result<(), std::io::Error> {
        // Reuse response_buffer
        state.response_buffer.clear();

        // Encode the response
        if let Err(e) = response.encode(&mut state.response_buffer) {
            error!("Failed to encode response: {:?}", e);
            return Ok(());
        }

        debug!("Sending response: total_buffer_len={}, first_bytes={:?}", 
            state.response_buffer.len(),
            &state.response_buffer[..std::cmp::min(20, state.response_buffer.len())]);

        // Write everything in one go
        let mut buffer = &state.response_buffer[..];
        while !buffer.is_empty() {
            match state.tcp_writer.write(buffer).await {
                Ok(n) => {
                    buffer = &buffer[n..];
                    debug!("Wrote {} bytes, {} remaining", n, buffer.len());
                }
                Err(e) => return Err(e),
            }
        }
        debug!("Wrote all response bytes");
        state.tcp_writer.flush().await?;
        debug!("Flushed response");
        Ok(())
    }
} 