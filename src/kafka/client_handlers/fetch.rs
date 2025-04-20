use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData};
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::{debug, info};

use crate::kafka::client_actor::ClientState;

pub(crate) async fn handle_fetch(
    client: &mut ClientState,
    request: &FetchRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling Fetch request: replica_id={:?}, min_bytes={}, max_wait_ms={}",
          request.replica_id, request.min_bytes, request.max_wait_ms);
    
    let mut response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0);
    
    let mut response_topics = Vec::new();
    
    // Process each topic in the request
    for topic in &request.topics {
        let topic_name = topic.topic.to_string();
        let mut response_partitions = Vec::new();
        
        // Process each partition in the topic
        for partition in &topic.partitions {
            let partition_id = partition.partition;
            
            // Create the partition response with empty records
            let response_partition = PartitionData::default()
                .with_partition_index(partition_id)
                .with_error_code(0) // SUCCESS
                .with_high_watermark(partition.fetch_offset + 10) // Example high watermark
                .with_last_stable_offset(partition.fetch_offset + 10) // Example last stable offset
                .with_log_start_offset(0); // Example log start offset
            
            response_partitions.push(response_partition);
        }
        
        // Create the topic response
        let response_topic = FetchableTopicResponse::default()
            .with_topic(topic.topic.clone())
            .with_partitions(response_partitions);
        
        response_topics.push(response_topic);
    }
    
    // Add response topics
    response.responses = response_topics;
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    debug!("Fetch response size: {} bytes", response_size);
    Ok((ResponseKind::Fetch(response), response_size))
}