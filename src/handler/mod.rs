use anyhow::Result;
use bytes::Bytes;
use tokio::net::TcpStream;
use tracing::{info, warn, debug, trace};
use kafka_protocol::messages::{
    ProduceRequest, ProduceResponse, 
    ApiVersionsResponse,
    MetadataRequest, MetadataResponse,
    ApiKey, BrokerId, TopicName,
};
use kafka_protocol::messages::api_versions_response::{ApiVersion, FinalizedFeatureKey};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseTopic, 
    MetadataResponseBroker,
    MetadataResponsePartition,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse,
    TopicProduceResponse,
    LeaderIdAndEpoch,
};
use kafka_protocol::protocol::Decodable;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::protocol::{read_request, write_response, write_api_versions_response};
use crate::storage::MessageStore;

#[derive(Clone)]
pub struct Handler {
    message_store: Arc<MessageStore>,
}

impl Handler {
    pub fn new(message_store: MessageStore) -> Self {
        Self {
            message_store: Arc::new(message_store)
        }
    }

    pub async fn handle_connection(&self, mut socket: TcpStream) -> Result<()> {
        // Get peer info for logging
        let peer = socket.peer_addr()?;
        info!("New connection from {}", peer);

        loop {
            let request = match read_request(&mut socket).await {
                Ok(req) => req,
                Err(e) => {
                    info!("Client {} disconnected with error: {}", peer, e);
                    break;
                }
            };
            debug!("Received request: api_key={:?}, api_version={}, correlation_id={}", request.api_key, request.api_version, request.correlation_id);
            
            match request.api_key {
                ApiKey::ApiVersions => {
                    info!("Received ApiVersions request from client {:?}", request.client_id);
                    
                    // Create response with supported versions
                    let mut response = ApiVersionsResponse::default();
                    response.error_code = 0; // NONE
                    response.throttle_time_ms = 0;
                    
                    response.api_keys = vec![
                        create_api_version(ApiKey::Produce, 0, 2),
                        create_api_version(ApiKey::Metadata, 0, 1),
                        create_api_version(ApiKey::ApiVersions, 0, 3),
                    ];
                    
                    if request.api_version >= 3 {
                        response.finalized_features_epoch = 0;
                        response.finalized_features = Vec::new();
                        response.unknown_tagged_fields = BTreeMap::new();
                    }

                    debug!("ApiVersions response: error_code={}, throttle_time={}, api_keys={:?}", 
                          response.error_code, response.throttle_time_ms, response.api_keys);
                    
                    write_api_versions_response(&mut socket, &response, request.api_version, request.correlation_id).await?;
                }
                ApiKey::Metadata => {
                    info!("Received metadata request from client {:?}", request.client_id);
                    
                    let mut payload = Bytes::from(request.payload);
                    let metadata_request = MetadataRequest::decode(&mut payload, request.api_version)?;
                    
                    trace!("Metadata request: {:?}", metadata_request);
                    
                    let mut response = MetadataResponse::default();
                    
                    response.brokers = vec![{
                        let mut broker = MetadataResponseBroker::default();
                        broker.node_id = BrokerId(0);
                        broker.host = StrBytes::from_string("localhost".to_string());
                        broker.port = 9092;
                        if request.api_version >= 1 {
                            broker.rack = None;
                        }
                        broker
                    }];
                    
                    response.topics = vec![{
                        let mut topic = MetadataResponseTopic::default();
                        topic.error_code = 0;
                        topic.name = Some(TopicName(StrBytes::from_string("test_topic".to_string())));
                        if request.api_version >= 1 {
                            topic.is_internal = false;
                        }
                        
                        topic.partitions = vec![{
                            let mut partition = MetadataResponsePartition::default();
                            partition.error_code = 0;
                            partition.partition_index = 0;
                            partition.leader_id = BrokerId(0);
                            partition.replica_nodes = vec![BrokerId(0)];
                            partition.isr_nodes = vec![BrokerId(0)];
                            if request.api_version >= 1 {
                                partition.offline_replicas = Vec::new();
                            }
                            partition
                        }];
                        
                        topic
                    }];
                    
                    debug!("Writing Metadata v{} response: brokers={:?}, topics={:?}", request.api_version, response.brokers, response.topics);
                    write_response(&mut socket, &response, request.api_version, request.correlation_id).await?;
                }
                ApiKey::Produce => {
                    info!("Received produce request from client {:?}", request.client_id);
                    
                    let produce_request = ProduceRequest::decode(
                        &mut Bytes::from(request.payload),
                        request.api_version
                    )?;
                    
                    trace!("Produce request: {:?}", produce_request);
                    
                    let mut response = ProduceResponse::default();
                    
                    if request.api_version >= 2 {
                        response.throttle_time_ms = 0;
                    }

                    // Pre-allocate responses for all topics/partitions
                    let mut topic_responses = Vec::new();
                    let mut clickhouse_batches = Vec::new();

                    // First pass: collect all messages into batches
                    for topic_data in &produce_request.topic_data {
                        let mut partition_responses = Vec::new();
                        let mut current_offset: i64 = 0;

                        for partition_data in &topic_data.partition_data {
                            let mut partition_response = PartitionProduceResponse::default();
                            partition_response.index = partition_data.index;
                            partition_response.error_code = 0;  // Success initially
                            
                            if let Some(ref records) = partition_data.records {
                                let mut records_buf = Bytes::from(records.to_vec());
                                match RecordBatchDecoder::decode::<_, fn(&mut Bytes, _) -> _>(&mut records_buf) {
                                    Ok(decoded_records) => {
                                        let messages: Vec<String> = decoded_records.into_iter().map(|record| {
                                            record.value.map(|v| String::from_utf8_lossy(&v).into_owned()).unwrap_or_default()
                                        }).collect();
                                        
                                        let num_messages = messages.len() as i64;
                                        partition_response.base_offset = current_offset;
                                        current_offset += num_messages;

                                        // Store batch info for bulk insert
                                        clickhouse_batches.push((
                                            topic_data.name.0.to_string(),
                                            partition_data.index as u32,
                                            messages
                                        ));
                                    }
                                    Err(e) => {
                                        warn!("Failed to decode record batch: {}", e);
                                        partition_response.error_code = 1;  // Unknown Server Error
                                    }
                                }
                            }

                            if request.api_version >= 2 {
                                partition_response.log_append_time_ms = -1; // Use create time
                                partition_response.log_start_offset = 0;
                                partition_response.record_errors = Vec::new();
                                partition_response.error_message = None;
                            }
                            partition_responses.push(partition_response);
                        }

                        let mut topic_response = TopicProduceResponse::default();
                        topic_response.name = topic_data.name.clone();
                        topic_response.partition_responses = partition_responses;
                        topic_responses.push(topic_response);
                    }

                    // Bulk insert all messages
                    if let Err(e) = self.message_store.append_message_batches(clickhouse_batches).await {
                        warn!("Failed to store message batches: {}", e);
                        // Mark all partitions as failed
                        for topic_response in &mut topic_responses {
                            for partition_response in &mut topic_response.partition_responses {
                                partition_response.error_code = 1; // Unknown Server Error
                            }
                        }
                    }

                    response.responses = topic_responses;
                    
                    debug!("Writing Produce v{} response", request.api_version);
                    write_response(&mut socket, &response, request.api_version, request.correlation_id).await?;
                }
                _ => {
                    warn!("Received unsupported request type: {:?}", request.api_key);
                    // TODO: Send UNSUPPORTED_VERSION or INVALID_REQUEST error response
                }
            }
        }

        Ok(())
    }
}

fn create_api_version(api_key: ApiKey, min_version: i16, max_version: i16) -> ApiVersion {
    let mut v = ApiVersion::default();
    v.api_key = api_key as i16;
    v.min_version = min_version;
    v.max_version = max_version;
    v.unknown_tagged_fields = BTreeMap::new();
    v
}

fn handle_disconnect(_client_id: Option<String>, peer: &std::net::SocketAddr) {
    info!("Client {} disconnected", peer);
} 