use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::offset_commit_response::{OffsetCommitResponse, OffsetCommitResponseTopic, OffsetCommitResponsePartition};
use kafka_protocol::protocol::Encodable;
use log::{debug, info};
use std::collections::HashMap;

use crate::kafka::client_actor::ClientState;

pub(crate) async fn handle_offset_commit(
    client: &mut ClientState,
    request: &OffsetCommitRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling OffsetCommit request: group_id={:?}, member_id={:?}, generation={:?}",
          request.group_id, request.member_id, request.generation_id_or_member_epoch);
    
    let group_id = request.group_id.0.to_string();
    
    // Check if the group and member exist
    let valid_member = true;
    
    // Create response topics
    let mut response_topics = Vec::new();
    
    // Process each topic in the request
    for topic in &request.topics {
        let mut response_partitions = Vec::new();
        
        // Process each partition in the topic
        for partition in &topic.partitions {
            let error_code = if !valid_member {
                if request.generation_id_or_member_epoch >= 0 {
                    22 // ILLEGAL_GENERATION
                } else {
                    15 // UNKNOWN_MEMBER_ID
                }
            } else {
                // Store the committed offset
                let topic_name = topic.name.to_string();
                let partition_id = partition.partition_index;
                let offset = partition.committed_offset;
                
                // In a real implementation, we would store these offsets in a persistent store
                // For now, we'll just store them in memory
                client.committed_offsets
                    .entry(group_id.clone())
                    .or_insert_with(HashMap::new)
                    .entry(topic_name.clone())
                    .or_insert_with(HashMap::new)
                    .insert(partition_id, offset);
                
                debug!("Committed offset {} for group={}, topic={}, partition={}",
                       offset, group_id, topic_name, partition_id);
                
                0 // SUCCESS
            };
            
            // Add partition to response
            let response_partition = OffsetCommitResponsePartition::default()
                .with_partition_index(partition.partition_index)
                .with_error_code(error_code);
            
            response_partitions.push(response_partition);
        }
        
        // Add topic to response
        let response_topic = OffsetCommitResponseTopic::default()
            .with_name(topic.name.clone())
            .with_partitions(response_partitions);
        
        response_topics.push(response_topic);
    }
    
    // Create response
    let response = OffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(response_topics);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::OffsetCommit(response), response_size))
} 