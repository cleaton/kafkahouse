use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseTopic, OffsetFetchResponsePartition};
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::info;

use crate::kafka::client_actor::ClientState;

pub(crate) async fn handle_offset_fetch(
    client: &mut ClientState,
    request: &OffsetFetchRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling OffsetFetch request: group_id={:?}", request.group_id);
    
    let group_id = request.group_id.0.to_string();
    let mut response_topics = Vec::new();
    
    // Check if we have any committed offsets for this group
    if let Some(group_offsets) = client.committed_offsets.get(&group_id) {
        // Process each topic in the request if topics is Some
        if let Some(topics) = &request.topics {
            for topic in topics {
                let topic_name = topic.name.0.to_string();
                let mut response_partitions = Vec::new();
                
                // Get the offsets for this topic
                if let Some(topic_offsets) = group_offsets.get(&topic_name) {
                    // Process each partition in the topic
                    for partition_index in &topic.partition_indexes {
                        let partition_id = *partition_index;
                        
                        // Get the committed offset for this partition
                        let (offset, metadata, error_code) = if let Some(&offset) = topic_offsets.get(&partition_id) {
                            (offset, StrBytes::from_string("".to_string()), 0) // SUCCESS
                        } else {
                            (-1, StrBytes::from_string("".to_string()), 0) // No offset found, not an error
                        };
                        
                        // Add partition to response
                        let response_partition = OffsetFetchResponsePartition::default()
                            .with_partition_index(partition_id)
                            .with_committed_offset(offset)
                            .with_metadata(Some(metadata))
                            .with_error_code(error_code);
                        
                        response_partitions.push(response_partition);
                    }
                } else {
                    // No offsets for this topic, return empty partitions
                    for partition_index in &topic.partition_indexes {
                        let response_partition = OffsetFetchResponsePartition::default()
                            .with_partition_index(*partition_index)
                            .with_committed_offset(-1)
                            .with_metadata(Some(StrBytes::from_string("".to_string())))
                            .with_error_code(0);
                        
                        response_partitions.push(response_partition);
                    }
                }
                
                // Add topic to response
                let response_topic = OffsetFetchResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(response_partitions);
                
                response_topics.push(response_topic);
            }
        }
    } else if let Some(topics) = &request.topics {
        // No offsets for this group, return empty topics
        for topic in topics {
            let mut response_partitions = Vec::new();
            
            for partition_index in &topic.partition_indexes {
                let response_partition = OffsetFetchResponsePartition::default()
                    .with_partition_index(*partition_index)
                    .with_committed_offset(-1)
                    .with_metadata(Some(StrBytes::from_string("".to_string())))
                    .with_error_code(0);
                
                response_partitions.push(response_partition);
            }
            
            let response_topic = OffsetFetchResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(response_partitions);
            
            response_topics.push(response_topic);
        }
    }
    
    // Create response
    let response = OffsetFetchResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(response_topics)
        .with_error_code(0);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::OffsetFetch(response), response_size))
} 