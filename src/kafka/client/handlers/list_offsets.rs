use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse};
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use std::collections::HashMap;

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

    // Collect topics and partitions to query
    let mut topics_partitions = Vec::new();
    for topic in &typed_request.topics {
        let mut partition_ids = Vec::new();
        for partition in &topic.partitions {
            partition_ids.push(partition.partition_index);
        }
        
        if !partition_ids.is_empty() {
            topics_partitions.push((topic.name.to_string(), partition_ids));
        }
    }
    
    // Get the latest offsets from the broker
    let offsets = state.broker.list_offsets(&topics_partitions).await?;
    
    // Build the response
    let mut topic_responses = Vec::new();
    let mut topic_offsets_map: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
    
    // Organize offsets by topic
    for (topic, partition, offset) in offsets {
        topic_offsets_map
            .entry(topic)
            .or_insert_with(Vec::new)
            .push((partition, offset));
    }
    
    // Create response for each topic in the request
    for topic in &typed_request.topics {
        let topic_name = topic.name.to_string();
        let mut partition_responses = Vec::new();
        
        for partition_req in &topic.partitions {
            let partition_id = partition_req.partition_index;
            let offset = topic_offsets_map
                .get(&topic_name)
                .and_then(|partitions| partitions.iter().find(|(p, _)| *p == partition_id))
                .map(|(_, o)| *o)
                .unwrap_or(0); // Default to 0 if offset not found
            
            let partition_response = ListOffsetsPartitionResponse::default()
                .with_partition_index(partition_id)
                .with_error_code(0) // Success
                .with_timestamp(partition_req.timestamp) // Echo back the timestamp
                .with_offset(offset);
                
            partition_responses.push(partition_response);
        }
        
        let topic_response = ListOffsetsTopicResponse::default()
            .with_name(topic.name.clone())
            .with_partitions(partition_responses);
            
        topic_responses.push(topic_response);
    }
    
    let response = ListOffsetsResponse::default()
        .with_topics(topic_responses)
        .with_throttle_time_ms(0);
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::ListOffsets(response),
        response_size
    })
}
