use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::list_offsets_response::{ListOffsetsResponse, ListOffsetsTopicResponse, ListOffsetsPartitionResponse};
use kafka_protocol::protocol::Encodable;
use log::info;

use crate::kafka::client_actor::ClientState;

pub(crate) async fn handle_list_offsets(
    client: &mut ClientState,
    request: &ListOffsetsRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling ListOffsets request: isolation_level={:?}, topics_count={}", 
           request.isolation_level, request.topics.len());
    
    // Create response
    let mut response_topics = Vec::new();
    
    // Process each topic in the request
    for topic in &request.topics {
        let topic_name = topic.name.0.to_string();
        let mut response_partitions = Vec::new();
        
        // Process each partition in the topic
        for partition in &topic.partitions {
            let partition_id = partition.partition_index;
            
            // In a real implementation, we would compute the actual offsets
            // Here, we'll just return some example values
            let offset = match partition.timestamp {
                // -1 means latest, -2 means earliest
                -1 => 100, // Latest
                -2 => 0,   // Earliest
                _ => 50,   // Some offset close to the requested timestamp
            };
            
            // Create the partition response
            let response_partition = ListOffsetsPartitionResponse::default()
                .with_partition_index(partition_id)
                .with_error_code(0) // SUCCESS
                .with_offset(offset);
            
            response_partitions.push(response_partition);
        }
        
        // Create the topic response
        let response_topic = ListOffsetsTopicResponse::default()
            .with_name(topic.name.clone())
            .with_partitions(response_partitions);
        
        response_topics.push(response_topic);
    }
    
    // Create the full response
    let response = ListOffsetsResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(response_topics);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::ListOffsets(response), response_size))
}
