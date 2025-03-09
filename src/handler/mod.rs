use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncWrite, split};
use tokio::sync::mpsc;
use tracing::{info, warn, debug, trace};
use futures::{StreamExt, stream};
use kafka_protocol::messages::{
    ProduceRequest, ProduceResponse, 
    ApiVersionsResponse,
    MetadataRequest, MetadataResponse,
    FetchRequest, FetchResponse,
    JoinGroupRequest, JoinGroupResponse,
    ApiKey, BrokerId, TopicName,
    ListOffsetsRequest, ListOffsetsResponse,
    FindCoordinatorRequest, FindCoordinatorResponse,
    SyncGroupRequest, SyncGroupResponse,
    HeartbeatRequest, HeartbeatResponse,
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
use kafka_protocol::records::{
    RecordBatchDecoder, RecordBatchEncoder, Record,
    Compression, TimestampType, RecordEncodeOptions,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_ID,
    NO_PRODUCER_EPOCH, NO_SEQUENCE,
};
use kafka_protocol::protocol::buf::ByteBuf;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use indexmap::IndexMap;

use crate::protocol::{read_request, write_response, write_api_versions_response};
use crate::storage::{MessageStore, Message, MessageFetchRequest, MessageFetchTopicPartitionRequest, MessageFetchResponse, MessageFetchTopicPartitionResponse, MessageFetchTopicPartitionData};

const MAX_CONCURRENT_MESSAGES: usize = 1024;
const MAX_CONCURRENT_PARTITIONS: usize = 100;
const BATCH_TIMEOUT_MS: u64 = 10;
const CHANNEL_SIZE: usize = 10_000;

#[derive(Debug)]
enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
    Produce(ProduceResponse),
    Metadata(MetadataResponse),
    Fetch(FetchResponse),
    JoinGroup(JoinGroupResponse),
    ListOffsets(ListOffsetsResponse),
    FindCoordinator(FindCoordinatorResponse),
    SyncGroup(SyncGroupResponse),
    Heartbeat(HeartbeatResponse),
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
        let (reader, writer) = split(socket);
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        
        let handler_clone = self.clone();
        let read_task = tokio::spawn(async move {
            handler_clone.handle_reads(reader, tx).await
        });

        let handler_clone = self.clone();
        let write_task = tokio::spawn(async move {
            handler_clone.handle_writes(writer, rx).await
        });

        // Wait for either task to complete
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
                                create_api_version(ApiKey::Produce, 0, 7),
                                create_api_version(ApiKey::Metadata, 0, 8),
                                create_api_version(ApiKey::ApiVersions, 0, 3),
                                create_api_version(ApiKey::ListOffsets, 0, 5),
                                create_api_version(ApiKey::Fetch, 0, 11),
                                create_api_version(ApiKey::FindCoordinator, 0, 3),
                                create_api_version(ApiKey::JoinGroup, 0, 5),
                                create_api_version(ApiKey::SyncGroup, 0, 5),
                                create_api_version(ApiKey::Heartbeat, 0, 4),
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
                        ApiKey::JoinGroup => {
                            info!("Received join group request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let join_group_request = JoinGroupRequest::decode(&mut payload, api_version)?;
                            
                            trace!("JoinGroup request: group_id={:?}, member_id={:?}", 
                                  join_group_request.group_id,
                                  join_group_request.member_id);
                            
                            let mut response = JoinGroupResponse::default();
                            response.error_code = 0;
                            if api_version >= 2 {
                                response.throttle_time_ms = 0;
                            }
                            
                            // Make this member the leader since we're a single-broker setup
                            response.generation_id = 1;
                            response.protocol_type = Some(join_group_request.protocol_type);
                            response.protocol_name = Some(join_group_request.protocols.first()
                                .map(|p| p.name.clone())
                                .unwrap_or_else(|| StrBytes::from_string("consumer".to_string())));
                            response.leader = join_group_request.member_id.clone();
                            response.member_id = join_group_request.member_id;
                            response.members = vec![];  // No other members in the group
                            
                            if api_version >= 7 {
                                response.unknown_tagged_fields = BTreeMap::new();
                            }
                            
                            Ok((KafkaResponse::JoinGroup(response), api_version, correlation_id))
                        }
                        ApiKey::ListOffsets => {
                            info!("Received ListOffsets request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let list_offsets_request = ListOffsetsRequest::decode(&mut payload, api_version)?;
                            
                            trace!("ListOffsets request: {:?}", list_offsets_request);
                            
                            let mut response = ListOffsetsResponse::default();
                            if api_version >= 1 {
                                response.throttle_time_ms = 0;
                            }
                            
                            // Process each topic
                            let mut topic_responses = Vec::new();
                            for topic_request in &list_offsets_request.topics {
                                let mut topic_response = kafka_protocol::messages::list_offsets_response::ListOffsetsTopicResponse::default();
                                topic_response.name = topic_request.name.clone();
                                
                                // Collect all partition indices
                                let partitions: Vec<u32> = topic_request.partitions.iter()
                                    .map(|p| p.partition_index as u32)
                                    .collect();
                                
                                // Query all partitions in one go
                                let partition_offsets = match self.message_store.get_offsets(
                                    &topic_request.name.0,
                                    &partitions,
                                    topic_request.partitions[0].timestamp // Assume same timestamp for all partitions
                                ).await {
                                    Ok(offsets) => offsets,
                                    Err(e) => {
                                        warn!("Failed to get offsets: {}", e);
                                        // On error, return 0 for all partitions
                                        partitions.into_iter().map(|p| (p, 0)).collect()
                                    }
                                };
                                
                                // Create partition responses
                                let mut partition_responses = Vec::new();
                                for partition_request in &topic_request.partitions {
                                    let mut partition_response = kafka_protocol::messages::list_offsets_response::ListOffsetsPartitionResponse::default();
                                    partition_response.partition_index = partition_request.partition_index;
                                    
                                    // Find the offset for this partition
                                    let offset = partition_offsets.iter()
                                        .find(|(p, _)| *p == partition_request.partition_index as u32)
                                        .map(|(_, o)| *o)
                                        .unwrap_or(0);
                                    
                                    partition_response.error_code = 0;
                                    if api_version == 0 {
                                        partition_response.old_style_offsets = vec![offset];
                                    } else {
                                        partition_response.offset = offset;
                                        partition_response.timestamp = partition_request.timestamp;
                                    }
                                    
                                    partition_responses.push(partition_response);
                                }
                                
                                topic_response.partitions = partition_responses;
                                topic_responses.push(topic_response);
                            }
                            
                            response.topics = topic_responses;
                            Ok((KafkaResponse::ListOffsets(response), api_version, correlation_id))
                        }
                        ApiKey::Fetch => {
                            info!("Received fetch request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let fetch_request = FetchRequest::decode(&mut payload, api_version)?;
                            
                            trace!("Fetch request: {:?}", fetch_request);
                            
                            let mut response = FetchResponse::default();
                            if api_version >= 1 {
                                response.throttle_time_ms = 0;
                            }
                            if api_version >= 7 {
                                response.error_code = 0;
                                response.session_id = 0;
                            }
                            if api_version >= 11 {
                                response.node_endpoints = Vec::new();
                                response.unknown_tagged_fields = BTreeMap::new();
                            }
                            
                            // Process each topic
                            let mut responses = Vec::new();
                            for topic_request in &fetch_request.topics {
                                let mut topic_response = kafka_protocol::messages::fetch_response::FetchableTopicResponse::default();
                                topic_response.topic = topic_request.topic.clone();
                                if api_version >= 11 {
                                    topic_response.unknown_tagged_fields = BTreeMap::new();
                                }
                                
                                // Create MessageFetchRequest
                                let fetch_req = MessageFetchRequest {
                                    topics: topic_request.partitions.iter().map(|partition_request| {
                                        MessageFetchTopicPartitionRequest {
                                            topic: topic_request.topic.0.to_string(),
                                            partition: partition_request.partition as u32,
                                            start_offset: partition_request.fetch_offset,
                                            max_bytes: fetch_request.max_bytes,
                                        }
                                    }).collect(),
                                    max_bytes: fetch_request.max_bytes,
                                };

                                warn!(
                                    "About to call get_messages with {} topic partition requests",
                                    fetch_req.topics.len()
                                );

                                // Fetch messages from ClickHouse
                                match self.message_store.get_messages(fetch_req).await {
                                    Ok(fetch_response) => {
                                        let mut partitions = Vec::new();
                                        
                                        // Convert each topic partition response
                                        for topic_partition in fetch_response.topics {
                                            let mut partition_response = kafka_protocol::messages::fetch_response::PartitionData::default();
                                            partition_response.partition_index = topic_partition.partition as i32;
                                            partition_response.error_code = 0;
                                            partition_response.high_watermark = topic_partition.high_watermark;
                                            partition_response.last_stable_offset = topic_partition.high_watermark;
                                            partition_response.log_start_offset = topic_partition.log_start_offset;
                                            partition_response.aborted_transactions = None;
                                            partition_response.preferred_read_replica = BrokerId(-1);
                                            if api_version >= 11 {
                                                partition_response.unknown_tagged_fields = BTreeMap::new();
                                            }

                                            warn!(
                                                "Constructing fetch response for topic={} partition={}: \
                                                partition_index={}, error_code={}, high_watermark={}, \
                                                last_stable_offset={}, log_start_offset={}, \
                                                data_count={}, data_offsets={:?}",
                                                topic_partition.topic,
                                                topic_partition.partition,
                                                partition_response.partition_index,
                                                partition_response.error_code,
                                                partition_response.high_watermark,
                                                partition_response.last_stable_offset,
                                                partition_response.log_start_offset,
                                                topic_partition.data.len(),
                                                topic_partition.data.iter().map(|m| m.offset).collect::<Vec<_>>()
                                            );

                                            if !topic_partition.data.is_empty() {
                                                // Convert our messages to Kafka Record format
                                                let records: Vec<Record> = topic_partition.data.iter().map(|msg| {
                                                    let record = Record {
                                                        transactional: false,
                                                        control: false,
                                                        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                                                        producer_id: NO_PRODUCER_ID,
                                                        producer_epoch: NO_PRODUCER_EPOCH,
                                                        timestamp_type: TimestampType::Creation,
                                                        offset: msg.offset,
                                                        sequence: NO_SEQUENCE,
                                                        timestamp: msg.timestamp,
                                                        key: Some(Bytes::from(msg.key.clone().into_bytes())),
                                                        value: Some(Bytes::from(msg.value.clone().into_bytes())),
                                                        headers: IndexMap::new(),
                                                    };
                                                    debug!("Sending record: offset={}, key={:?}, value={:?}", 
                                                          msg.offset, msg.key, msg.value);
                                                    record
                                                }).collect();

                                                debug!("Encoding {} records for topic={} partition={}", 
                                                      records.len(), topic_partition.topic, topic_partition.partition);

                                                let mut buf = Vec::new();
                                                let options = RecordEncodeOptions {
                                                    version: 2,  // Use the latest version
                                                    compression: Compression::None,
                                                };

                                                debug!("Attempting to encode {} records with version={}, compression={:?}", 
                                                      records.len(), options.version, options.compression);

                                                // Encode the records using Kafka's encoder
                                                match RecordBatchEncoder::encode::<Vec<u8>, &[Record], fn(&mut BytesMut, &mut Vec<u8>, Compression) -> Result<()>>(
                                                    &mut buf,
                                                    records.as_slice(),
                                                    &options,
                                                ) {
                                                    Ok(_) => {
                                                        debug!("Successfully encoded {} bytes of records into batch", buf.len());
                                                        partition_response.records = Some(Bytes::from(buf));
                                                    }
                                                    Err(e) => {
                                                        warn!("Failed to encode record batch: {}", e);
                                                        partition_response.error_code = 1;
                                                    }
                                                }

                                                debug!("Partition response: error_code={}, high_watermark={}, last_stable_offset={}, log_start_offset={}, records_size={:?}", 
                                                      partition_response.error_code,
                                                      partition_response.high_watermark,
                                                      partition_response.last_stable_offset,
                                                      partition_response.log_start_offset,
                                                      partition_response.records.as_ref().map(|r| r.len()));
                                            }

                                            partitions.push(partition_response);
                                        }

                                        topic_response.partitions = partitions;
                                    }
                                    Err(e) => {
                                        warn!("Failed to fetch messages: {}", e);
                                        // On error, create error responses for all partitions
                                        topic_response.partitions = topic_request.partitions.iter().map(|p| {
                                            let mut partition_response = kafka_protocol::messages::fetch_response::PartitionData::default();
                                            partition_response.partition_index = p.partition;
                                            partition_response.error_code = 1;
                                            partition_response
                                        }).collect();
                                    }
                                }
                                
                                responses.push(topic_response);
                            }
                            
                            response.responses = responses;
                            Ok((KafkaResponse::Fetch(response), api_version, correlation_id))
                        }
                        ApiKey::FindCoordinator => {
                            info!("Received FindCoordinator request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let find_coordinator_request = FindCoordinatorRequest::decode(&mut payload, api_version)?;
                            
                            trace!("FindCoordinator request: key_type={:?}, key={}", 
                                  find_coordinator_request.key_type,
                                  find_coordinator_request.key);
                            
                            let mut response = FindCoordinatorResponse::default();
                            if api_version >= 1 {
                                response.throttle_time_ms = 0;
                            }
                            response.error_code = 0;
                            
                            // Always return our single broker as the coordinator
                            response.node_id = BrokerId(0);
                            response.host = StrBytes::from_string("127.0.0.1".to_string());
                            response.port = 9092;
                            
                            if api_version >= 1 {
                                response.error_message = None;
                            }

                            if api_version >= 3 {
                                response.unknown_tagged_fields = BTreeMap::new();
                            }
                            
                            Ok((KafkaResponse::FindCoordinator(response), api_version, correlation_id))
                        }
                        ApiKey::SyncGroup => {
                            info!("Received SyncGroup request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let sync_group_request = SyncGroupRequest::decode(&mut payload, api_version)?;
                            
                            trace!("SyncGroup request: group_id={:?}, member_id={:?}, generation_id={}", 
                                  sync_group_request.group_id,
                                  sync_group_request.member_id,
                                  sync_group_request.generation_id);
                            
                            // Create a default response and set the fields according to the documentation
                            let mut response = SyncGroupResponse::default();
                            response.error_code = 0; // Success
                            
                            if api_version >= 1 {
                                response.throttle_time_ms = 0;
                            }
                            
                            // Create a simple assignment with a single partition
                            let mut buf = Vec::new();
                            // Version (2 bytes)
                            buf.extend_from_slice(&[0, 0]);
                            // Number of topics (2 bytes)
                            buf.extend_from_slice(&[0, 1]);
                            // Topic name length (2 bytes)
                            let topic_name = b"test_topic";
                            buf.extend_from_slice(&[(topic_name.len() >> 8) as u8, topic_name.len() as u8]);
                            // Topic name
                            buf.extend_from_slice(topic_name);
                            // Number of partitions (4 bytes)
                            buf.extend_from_slice(&[0, 0, 0, 1]);
                            // Partition ID (4 bytes)
                            buf.extend_from_slice(&[0, 0, 0, 0]);
                            
                            response.assignment = Bytes::from(buf);
                            
                            if api_version >= 5 {
                                response.protocol_type = None;
                                response.protocol_name = None;
                                response.unknown_tagged_fields = BTreeMap::new();
                            }
                            
                            Ok((KafkaResponse::SyncGroup(response), api_version, correlation_id))
                        }
                        ApiKey::Heartbeat => {
                            info!("Received Heartbeat request from client {:?}", request.client_id);
                            
                            let mut payload = Bytes::from(request.payload);
                            let heartbeat_request = HeartbeatRequest::decode(&mut payload, api_version)?;
                            
                            trace!("Heartbeat request: group_id={:?}, member_id={:?}, generation_id={}", 
                                  heartbeat_request.group_id,
                                  heartbeat_request.member_id,
                                  heartbeat_request.generation_id);
                            
                            let mut response = HeartbeatResponse::default();
                            response.error_code = 0;
                            if api_version >= 1 {
                                response.throttle_time_ms = 0;
                            }
                            if api_version >= 4 {
                                response.unknown_tagged_fields = BTreeMap::new();
                            }
                            
                            Ok((KafkaResponse::Heartbeat(response), api_version, correlation_id))
                        }
                        _ => {
                            warn!("Received unsupported request type: {:?}", api_key);
                            // Return an error response with UNSUPPORTED_VERSION error code
                            let error_response = match api_key {
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
                    KafkaResponse::ApiVersions(response) => {
                        write_api_versions_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::Metadata(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::Produce(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::Fetch(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::JoinGroup(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::ListOffsets(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::FindCoordinator(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::SyncGroup(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
                    }
                    KafkaResponse::Heartbeat(response) => {
                        write_response(&mut writer, &response, api_version, correlation_id).await?;
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