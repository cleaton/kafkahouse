use anyhow::Result;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncWrite, split};
use tokio::sync::mpsc;
use tracing::{info, warn, debug, trace};
use futures::{StreamExt, stream};
use kafka_protocol::messages::{
    ProduceRequest, ProduceResponse, 
    ApiVersionsResponse,
    MetadataRequest, MetadataResponse,
    ApiKey, BrokerId, TopicName,
};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseTopic, 
    MetadataResponseBroker,
    MetadataResponsePartition,
};
use kafka_protocol::messages::produce_response::{
    PartitionProduceResponse,
    TopicProduceResponse,
};
use kafka_protocol::protocol::Decodable;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use crate::protocol::{read_request, write_response, write_api_versions_response};
use crate::storage::MessageStore;

const MAX_CONCURRENT_MESSAGES: usize = 1024;
const MAX_CONCURRENT_PARTITIONS: usize = 100;
const BATCH_TIMEOUT_MS: u64 = 10;
const CHANNEL_SIZE: usize = 10_000;

#[derive(Debug)]
enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
    Produce(ProduceResponse),
    Metadata(MetadataResponse),
}

// Response type that includes correlation ID for ordering
#[derive(Debug)]
struct ResponseBatch {
    responses: Vec<(KafkaResponse, i16, i32)>, // (response, api_version, correlation_id)
}

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

    async fn handle_produce_request(
        &self,
        request: ProduceRequest,
        api_version: i16,
        _correlation_id: i32,  // Prefix with underscore since it's unused
    ) -> Result<ProduceResponse> {
        let mut response = ProduceResponse::default();
        if api_version >= 2 {
            response.throttle_time_ms = 0;
        }

        info!("Produce request config: acks={}, timeout_ms={}", 
              request.acks, request.timeout_ms);

        let mut topic_responses = Vec::new();
        let mut all_batches = Vec::new();
        let current_offset: i64 = 0;  // Removed mut since it's not modified

        // Count total messages across all topics/partitions
        let mut total_messages = 0;
        for topic_data in &request.topic_data {
            for partition_data in &topic_data.partition_data {
                if let Some(ref records) = partition_data.records {
                    let mut records_buf = Bytes::from(records.to_vec());
                    if let Ok(decoded_records) = RecordBatchDecoder::decode::<_, fn(&mut Bytes, _) -> _>(&mut records_buf) {
                        total_messages += decoded_records.len();
                    }
                }
            }
        }
        info!("Received produce request with {} messages across {} topics", 
              total_messages, request.topic_data.len());

        // Process each topic
        for topic_data in request.topic_data {
            let topic_name_str = topic_data.name.0.to_string();
            let partition_count = topic_data.partition_data.len();
            
            // Create a stream of partition processing tasks
            let partition_stream = stream::iter(topic_data.partition_data.into_iter().enumerate())
                .map(|(idx, partition_data)| {
                    let message_store = Arc::clone(&self.message_store);
                    let topic_name = topic_name_str.clone();
                    
                    async move {
                        let result = process_partition_batch(
                            message_store,
                            topic_name,
                            &partition_data,
                            current_offset,
                        ).await;
                        (idx, result)
                    }
                })
                .buffered(MAX_CONCURRENT_PARTITIONS);
            
            // Collect results in order
            let mut partition_responses = vec![None; partition_count];
            tokio::pin!(partition_stream);
            
            while let Some((idx, (partition_response, batch_info))) = partition_stream.next().await {
                if let Some(batch) = batch_info {
                    all_batches.push(batch);
                }
                partition_responses[idx] = Some(partition_response);
            }

            let mut topic_response = TopicProduceResponse::default();
            topic_response.name = topic_data.name;
            topic_response.partition_responses = partition_responses.into_iter()
                .map(|r| r.unwrap_or_else(|| {
                    let mut error_response = PartitionProduceResponse::default();
                    error_response.error_code = 1;
                    error_response
                }))
                .collect();
            
            topic_responses.push(topic_response);
        }

        // Bulk insert all collected batches
        if let Err(e) = self.message_store.append_message_batches(all_batches).await {
            warn!("Failed to store message batches: {}", e);
            // Mark all partitions as failed
            for topic_response in &mut topic_responses {
                for partition_response in &mut topic_response.partition_responses {
                    partition_response.error_code = 1;
                }
            }
        }

        response.responses = topic_responses;
        Ok(response)
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        let peer = socket.peer_addr()?;
        info!("New connection from {}", peer);

        let (reader, writer) = split(socket);
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);

        // Spawn separate tasks for reading and writing
        let read_task = tokio::spawn(self.clone().handle_reads(reader, tx));
        let write_task = tokio::spawn(self.clone().handle_writes(writer, rx));

        // Wait for either task to complete (or error)
        tokio::select! {
            read_result = read_task => {
                if let Err(e) = read_result {
                    warn!("Read task failed: {}", e);
                }
            }
            write_result = write_task => {
                if let Err(e) = write_result {
                    warn!("Write task failed: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn handle_reads<R>(self, mut reader: R, tx: mpsc::Sender<ResponseBatch>) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        loop {
            match read_request(&mut reader).await {
                Ok(request) => {
                    let correlation_id = request.correlation_id;
                    let api_version = request.api_version;
                    let api_key = request.api_key;
                    
                    debug!("Processing request: api_key={:?}, api_version={}, correlation_id={}", 
                          api_key, api_version, correlation_id);

                    let result = match api_key {
                        ApiKey::ApiVersions => {
                            info!("Received ApiVersions request from client {:?}", request.client_id);
                            
                            let mut response = ApiVersionsResponse::default();
                            response.error_code = 0;
                            response.throttle_time_ms = 0;
                            
                            response.api_keys = vec![
                                create_api_version(ApiKey::Produce, 0, 2),
                                create_api_version(ApiKey::Metadata, 0, 1),
                                create_api_version(ApiKey::ApiVersions, 0, 3),
                            ];
                            
                            if api_version >= 3 {
                                response.finalized_features_epoch = 0;
                                response.finalized_features = Vec::new();
                                response.unknown_tagged_fields = BTreeMap::new();
                            }
                            
                            Ok((KafkaResponse::ApiVersions(response), api_version, correlation_id))
                        }
                        ApiKey::Metadata => {
                            info!("Received metadata request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let metadata_request = MetadataRequest::decode(&mut payload, api_version)?;
                            
                            trace!("Metadata request: {:?}", metadata_request);
                            
                            let mut response = MetadataResponse::default();
                            
                            response.brokers = vec![{
                                let mut broker = MetadataResponseBroker::default();
                                broker.node_id = BrokerId(0);
                                broker.host = StrBytes::from_string("localhost".to_string());
                                broker.port = 9092;
                                if api_version >= 1 {
                                    broker.rack = None;
                                }
                                broker
                            }];
                            
                            response.topics = vec![{
                                let mut topic = MetadataResponseTopic::default();
                                topic.error_code = 0;
                                topic.name = Some(TopicName(StrBytes::from_string("test_topic".to_string())));
                                if api_version >= 1 {
                                    topic.is_internal = false;
                                }
                                
                                topic.partitions = (0..100).map(|partition_idx| {
                                    let mut partition = MetadataResponsePartition::default();
                                    partition.error_code = 0;
                                    partition.partition_index = partition_idx;
                                    partition.leader_id = BrokerId(0);
                                    partition.replica_nodes = vec![BrokerId(0)];
                                    partition.isr_nodes = vec![BrokerId(0)];
                                    if api_version >= 1 {
                                        partition.offline_replicas = Vec::new();
                                    }
                                    partition
                                }).collect();
                                
                                topic
                            }];
                            
                            Ok((KafkaResponse::Metadata(response), api_version, correlation_id))
                        }
                        ApiKey::Produce => {
                            info!("Received produce request from client {:?}", request.client_id);
                            
                            let produce_request = ProduceRequest::decode(
                                &mut Bytes::from(request.payload),
                                api_version
                            )?;
                            
                            self.handle_produce_request(produce_request, api_version, correlation_id)
                                .await
                                .map(|resp| (KafkaResponse::Produce(resp), api_version, correlation_id))
                        }
                        _ => {
                            warn!("Received unsupported request type: {:?}", api_key);
                            // Return an error response with UNSUPPORTED_VERSION error code
                            let mut error_response = match api_key {
                                ApiKey::Metadata => {
                                    let mut resp = MetadataResponse::default();
                                    resp.topics = vec![];
                                    resp.brokers = vec![];
                                    KafkaResponse::Metadata(resp)
                                }
                                ApiKey::Produce => {
                                    let mut resp = ProduceResponse::default();
                                    resp.responses = vec![];
                                    KafkaResponse::Produce(resp)
                                }
                                _ => {
                                    let mut resp = ApiVersionsResponse::default();
                                    resp.error_code = 35; // UNSUPPORTED_VERSION
                                    resp.api_keys = vec![];
                                    KafkaResponse::ApiVersions(resp)
                                }
                            };
                            Ok((error_response, api_version, correlation_id))
                        }
                    };

                    match result {
                        Ok(response) => {
                            // Send single response to writer task
                            if let Err(e) = tx.send(ResponseBatch { responses: vec![response] }).await {
                                warn!("Failed to send response to writer: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("Error processing request: {}", e);
                            // Send error response
                            let mut error_response = ApiVersionsResponse::default();
                            error_response.error_code = 1; // UNKNOWN_SERVER_ERROR
                            if let Err(e) = tx.send(ResponseBatch { 
                                responses: vec![(KafkaResponse::ApiVersions(error_response), 0, 0)] 
                            }).await {
                                warn!("Failed to send error response to writer: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Read error: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_writes<W>(self, mut writer: W, mut rx: mpsc::Receiver<ResponseBatch>) -> Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        while let Some(batch) = rx.recv().await {
            for (response, api_version, correlation_id) in batch.responses {
                match response {
                    KafkaResponse::ApiVersions(resp) => {
                        write_api_versions_response(&mut writer, &resp, api_version, correlation_id).await?;
                    }
                    KafkaResponse::Metadata(resp) => {
                        write_response(&mut writer, &resp, api_version, correlation_id).await?;
                    }
                    KafkaResponse::Produce(resp) => {
                        write_response(&mut writer, &resp, api_version, correlation_id).await?;
                    }
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

async fn process_partition_batch(
    message_store: Arc<MessageStore>,
    topic_name: String,
    partition_data: &kafka_protocol::messages::produce_request::PartitionProduceData,
    current_offset: i64,
) -> (PartitionProduceResponse, Option<(String, u32, Vec<(String, String)>)>) {
    let mut partition_response = PartitionProduceResponse::default();
    partition_response.index = partition_data.index;
    partition_response.error_code = 0;
    partition_response.base_offset = current_offset;
    
    let batch_info = if let Some(ref records) = partition_data.records {
        let mut records_buf = Bytes::from(records.to_vec());
        info!("Processing record batch: size={} bytes for topic={} partition={}", 
              records.len(), topic_name, partition_data.index);
        
        match RecordBatchDecoder::decode::<_, fn(&mut Bytes, _) -> _>(&mut records_buf) {
            Ok(decoded_records) => {
                let messages: Vec<(String, String)> = decoded_records.into_iter()
                    .map(|record| {
                        let key = record.key
                            .map(|k| String::from_utf8_lossy(&k).into_owned())
                            .unwrap_or_default();
                        let value = record.value
                            .map(|v| String::from_utf8_lossy(&v).into_owned())
                            .unwrap_or_default();
                        (key, value)
                    })
                    .collect();
                
                info!("Decoded {} messages from record batch for topic={} partition={}", 
                      messages.len(), topic_name, partition_data.index);
                
                Some((
                    topic_name,
                    partition_data.index as u32,
                    messages
                ))
            }
            Err(e) => {
                warn!("Failed to decode record batch: {} (size={} bytes)", e, records.len());
                partition_response.error_code = 1;
                None
            }
        }
    } else {
        info!("No records in batch for topic={} partition={}", topic_name, partition_data.index);
        None
    };

    partition_response.log_append_time_ms = -1;
    partition_response.log_start_offset = 0;
    partition_response.record_errors = Vec::new();
    partition_response.error_message = None;

    (partition_response, batch_info)
} 