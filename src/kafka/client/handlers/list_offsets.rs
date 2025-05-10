use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse};
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap};

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_list_offsets(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the ListOffsetsRequest from the request
    let typed_request = if let RequestKind::ListOffsets(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected ListOffsets request"));
    };
    
    info!("Handling ListOffsets request for {} topics", typed_request.topics.len());
    
    // Extract topic-partition pairs from request
    let topic_partitions: Vec<(String, Vec<i32>)> = typed_request.topics
        .iter()
        .map(|topic| {
            let topic_name = topic.name.to_string();
            let partitions = topic.partitions.iter()
                .map(|p| p.partition_index)
                .collect();
            (topic_name, partitions)
        })
        .collect();
    
    // Get all partition offsets in a single query
    let partition_offsets = state.broker.get_partitions_offsets(&topic_partitions).await?;
    
    // Create a map for easy lookup: topic -> partition -> offset
    let mut offset_map: HashMap<String, HashMap<i32, i64>> = HashMap::new();
    for (topic, partition, offset) in partition_offsets {
        offset_map
            .entry(topic)
            .or_insert_with(HashMap::new)
            .insert(partition, offset);
    }
    
    // Build the response
    let mut response_topics = Vec::new();
    
    for topic_request in &typed_request.topics {
        let topic_name = topic_request.name.to_string();
        let mut partitions = Vec::new();
        
        // Process each requested partition
        for partition_request in &topic_request.partitions {
            let partition_id = partition_request.partition_index;
            
            // Get offset for this partition (default to 0 if not found)
            let offset = offset_map
                .get(&topic_name)
                .and_then(|partitions| partitions.get(&partition_id))
                .copied()
                .unwrap_or(0);
            
            // Handle different timestamp specifications
            let response_offset = match partition_request.timestamp {
                -1 => offset,         // Latest offset
                -2 => 0,              // Earliest offset (we're assuming 0 for all partitions)
                _ => 0,               // Timestamp-based offset (not fully implemented)
            };
            
            let error_code = if response_offset == 0 && partition_request.timestamp == -1 {
                // Check if partition exists in topic metadata
                let partition_exists = state.topics
                    .get(&topic_name)
                    .map(|partitions| partitions.iter().any(|p| p.partition_id == partition_id))
                    .unwrap_or(false);
                
                if !partition_exists {
                    3 // UNKNOWN_TOPIC_OR_PARTITION
                } else {
                    0 // SUCCESS
                }
            } else {
                0 // SUCCESS
            };
            
            let partition_response = ListOffsetsPartitionResponse::default()
            .with_partition_index(partition_id)
            .with_error_code(error_code)
            .with_timestamp(partition_request.timestamp)
            .with_offset(response_offset)
            .with_leader_epoch(-1);
            
            partitions.push(partition_response);
        }
        
        // Add topic to response
        let topic_response = ListOffsetsTopicResponse::default()
            .with_name(topic_request.name.clone())
            .with_partitions(partitions);
            
        response_topics.push(topic_response);
    }
    
    // Build the final response
    let response = ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(response_topics);
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::ListOffsets(response),
        response_size
    })
}
