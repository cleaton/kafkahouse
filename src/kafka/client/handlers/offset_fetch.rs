use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseTopic, OffsetFetchResponsePartition};
use kafka_protocol::protocol::{Encodable, StrBytes};

use crate::kafka::client::types::ClientState;
use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_offset_fetch(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the OffsetFetchRequest from the request
    let typed_request = if let RequestKind::OffsetFetch(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected OffsetFetch request"));
    };

    // Extract group ID
    let group_id = typed_request.group_id.to_string();
    
    // Collect topics and partitions to query
    let mut topics_partitions = Vec::new();
    
    // Process topics if they exist
    if let Some(topics) = &typed_request.topics {
        for topic in topics {
            let topic_name = topic.name.to_string();
            let partition_ids = topic.partition_indexes.clone();
            
            if !partition_ids.is_empty() {
                topics_partitions.push((topic_name, partition_ids));
            }
        }
    }
    
    // Get the stored offsets from the broker
    let offsets = state.broker.offset_fetch(group_id, &topics_partitions).await?;
    
    // Build the response
    let mut response_topics = Vec::new();
    let mut topic_offsets_map: std::collections::HashMap<String, Vec<(i32, i64)>> = std::collections::HashMap::new();
    
    // Organize offsets by topic
    for (topic, partition, offset) in offsets {
        topic_offsets_map
            .entry(topic)
            .or_insert_with(Vec::new)
            .push((partition, offset));
    }
    
    // Create response for each topic in the request
    if let Some(topics) = &typed_request.topics {
        for topic in topics {
            let topic_name = topic.name.to_string();
            let mut partition_responses = Vec::new();
            
            for &partition_id in &topic.partition_indexes {
                // Find the offset for this partition or use default values
                let offset = topic_offsets_map
                    .get(&topic_name)
                    .and_then(|partitions| partitions.iter().find(|(p, _)| *p == partition_id))
                    .map(|(_, o)| *o)
                    .unwrap_or(-1); // -1 is usually used to indicate no offset
                
                let partition_response = OffsetFetchResponsePartition::default()
                    .with_partition_index(partition_id)
                    .with_committed_offset(offset)
                    .with_committed_leader_epoch(-1) // Not supported in our implementation
                    .with_metadata(Some(StrBytes::from(""))) // Wrap in Some() as it expects an Option
                    .with_error_code(0); // Success
                    
                partition_responses.push(partition_response);
            }
            
            let topic_response = OffsetFetchResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(partition_responses);
                
            response_topics.push(topic_response);
        }
    }
    
    let response = OffsetFetchResponse::default()
        .with_topics(response_topics)
        .with_throttle_time_ms(0)
        .with_error_code(0); // Success
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::OffsetFetch(response),
        response_size,
    })
}
