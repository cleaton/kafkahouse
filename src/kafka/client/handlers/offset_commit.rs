use anyhow::Result;
use log::info;
use kafka_protocol::messages::*;
use kafka_protocol::messages::offset_commit_response::{OffsetCommitResponse, OffsetCommitResponseTopic, OffsetCommitResponsePartition};
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::messages::TopicName;
use crate::kafka::client::types::ClientState;
use std::collections::BTreeMap;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_offset_commit(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the OffsetCommitRequest from the request
    let typed_request = if let RequestKind::OffsetCommit(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected OffsetCommit request"));
    };
    
    info!("Handling OffsetCommit request: group_id={:?}, member_id={:?}, generation={:?}",
          typed_request.group_id, typed_request.member_id, typed_request.generation_id_or_member_epoch);
    
    let group_id = typed_request.group_id.to_string();
    let member_id = typed_request.member_id.to_string();
    
    // Check if the group exists and member is valid
    let valid_member = state.active_groups.contains_key(&group_id);
    
    // Collect all offsets to commit
    let mut offsets = Vec::new();
    for topic in &typed_request.topics {
        for partition in &topic.partitions {
            offsets.push((
                topic.name.to_string(),
                partition.partition_index,
                partition.committed_offset,
                partition.committed_metadata.clone().unwrap_or_default().to_string(),
            ));
        }
    }

    // Commit offsets and get results
    let commit_results = if valid_member {
        state.broker.commit_offsets(
            group_id.clone(),
            member_id.clone(),
            state.client_id.clone(),
            state.client_host.clone(),
            offsets,
        ).await?
    } else {
        // If member is not valid, mark all offsets with appropriate error
        offsets.into_iter().map(|(topic, partition, _, _)| {
            let error_code = if typed_request.generation_id_or_member_epoch >= 0 {
                22 // ILLEGAL_GENERATION
            } else {
                15 // UNKNOWN_MEMBER_ID
            };
            (topic, partition, error_code)
        }).collect()
    };

    // Build response topics
    let mut response_topics = Vec::new();
    let mut current_topic = String::new();
    let mut current_partitions = Vec::new();

    // Group results by topic
    for (topic, partition, error_code) in commit_results {
        if topic != current_topic && !current_topic.is_empty() {
            // Add completed topic to response
            response_topics.push(
                OffsetCommitResponseTopic::default()
                    .with_name(TopicName(StrBytes::from_string(current_topic.clone())))
                    .with_partitions(current_partitions.clone())
            );
            current_partitions.clear();
        }
        
        current_topic = topic;
        current_partitions.push(
            OffsetCommitResponsePartition::default()
                .with_partition_index(partition)
                .with_error_code(error_code)
        );
    }

    // Add last topic if any
    if !current_topic.is_empty() {
        response_topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(TopicName(StrBytes::from_string(current_topic)))
                .with_partitions(current_partitions)
        );
    }
    
    // Create response
    let response = OffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(response_topics)
        .with_unknown_tagged_fields(BTreeMap::new());
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::OffsetCommit(response),
        response_size,
    })
}
