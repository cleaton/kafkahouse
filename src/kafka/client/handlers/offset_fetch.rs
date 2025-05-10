use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseTopic, OffsetFetchResponsePartition};
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::info;
use std::collections::BTreeMap;

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
    
    info!("Handling OffsetFetch request: group_id={:?}", typed_request.group_id);
    
    let group_id = typed_request.group_id.0.to_string();
    let mut response_topics = Vec::new();
    
    // Process topics if present
    if let Some(topics) = typed_request.topics {
        // Prepare topics and partitions for query
        let topics_partitions: Vec<(String, Vec<i32>)> = topics.iter()
            .map(|topic| (
                topic.name.0.to_string(),
                topic.partition_indexes.clone()
            ))
            .collect();

        // Fetch offsets from ClickHouse
        let offsets = state.broker.fetch_offsets(&group_id, &topics_partitions).await?;
        
        // Create a map for easy lookup
        let mut offset_map: BTreeMap<(String, i32), (i64, String)> = BTreeMap::new();
        for (topic, partition, offset, metadata) in offsets {
            offset_map.insert((topic, partition), (offset, metadata));
        }
        
        // Build response
        for topic in topics {
            let topic_name = topic.name.0.to_string();
            let mut response_partitions = Vec::new();
            
            for &partition_id in &topic.partition_indexes {
                let (offset, metadata) = offset_map
                    .get(&(topic_name.clone(), partition_id))
                    .map(|(o, m)| (*o, m.clone()))
                    .unwrap_or((-1, String::new())); // No offset found
                
                let response_partition = OffsetFetchResponsePartition::default()
                    .with_partition_index(partition_id)
                    .with_committed_offset(offset)
                    .with_metadata(Some(StrBytes::from_string(metadata)))
                    .with_error_code(0);
                
                response_partitions.push(response_partition);
            }
            
            let response_topic = OffsetFetchResponseTopic::default()
                .with_name(topic.name)
                .with_partitions(response_partitions);
            
            response_topics.push(response_topic);
        }
    }
    
    // Create response
    let response = OffsetFetchResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(response_topics)
        .with_error_code(0)
        .with_unknown_tagged_fields(BTreeMap::new());
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::OffsetFetch(response),
        response_size,
    })
}
