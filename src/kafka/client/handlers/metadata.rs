use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::metadata_response::{MetadataResponse, MetadataResponseBroker, MetadataResponseTopic};
use kafka_protocol::protocol::{Encodable, StrBytes};
use crate::kafka::client::types::{ClientState, TopicPartition};
use log::{debug, info};
use std::collections::HashMap;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_metadata(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the MetadataRequest from the request
    let metadata_request = if let RequestKind::Metadata(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected Metadata request"));
    };
    
    debug!("Handling metadata request: api_version={}, {:?}", api_version, metadata_request);
    
    // Create a minimal response based on your previous working code
    let mut response = MetadataResponse::default();
    
    // Add broker information - minimal fields that work across versions
    response.brokers = vec![
        MetadataResponseBroker::default()
            .with_node_id(BrokerId(state.broker_id))
            .with_host(StrBytes::from_string("127.0.0.1".to_string()))
            .with_port(9092)
    ];

    // Auto-create topics if they don't exist
    let mut created_topics = HashMap::new();
    if let Some(topics) = &metadata_request.topics {
        for topic in topics {
            if let Some(topic_name) = &topic.name {
                let name = topic_name.0.to_string();
                if !state.topics.contains_key(&name) && !name.is_empty() {
                    info!("Auto-creating topic: {}", name);
                    // Create a topic with 100 partitions
                    let mut partitions = Vec::with_capacity(100);
                    for i in 0..100 {
                        partitions.push(TopicPartition {
                            partition_id: i,
                            leader: state.broker_id,
                            replicas: vec![state.broker_id],
                            isr: vec![state.broker_id],
                        });
                    }
                    state.topics.insert(name.clone(), partitions);
                    created_topics.insert(name, true);
                }
            }
        }
    }

    // Add topic metadata
    response.topics = metadata_request.topics.as_ref().map_or_else(
        || {
            debug!("No topics specified in request, returning all topics: {:?}", state.topics.keys().collect::<Vec<_>>());
            state.topics.iter().map(|(name, partitions)| 
                create_topic_metadata(name, partitions)).collect()
        },
        |topics| {
            debug!("Topics requested: {:?}", topics.iter().filter_map(|t| t.name.as_ref().map(|n| n.0.to_string())).collect::<Vec<_>>());
            topics.iter()
                .filter_map(|topic| {
                    let name = topic.name.as_ref().map(|t| t.0.to_string()).unwrap_or_default();
                    if name.is_empty() {
                        debug!("Empty topic name in request");
                        return None;
                    }
                    
                    let result = state.topics.get(&name)
                        .map(|partitions| create_topic_metadata(&name, partitions));
                    
                    if result.is_none() {
                        debug!("Topic '{}' not found in broker", name);
                    } else {
                        if created_topics.contains_key(&name) {
                            info!("Returning newly created topic '{}' in response", name);
                        } else {
                            debug!("Found topic '{}' with {} partitions", name, state.topics.get(&name).map_or(0, |p| p.len()));
                        }
                    }
                    
                    result
                })
                .collect()
        }
    );

    // If we have no topics at all, create a default test_topic
    if state.topics.is_empty() {
        info!("No topics exist, creating default test_topic");
        let partition = TopicPartition {
            partition_id: 0,
            leader: state.broker_id,
            replicas: vec![state.broker_id],
            isr: vec![state.broker_id],
        };
        state.topics.insert("test_topic".to_string(), vec![partition]);
    }

    // Check if we have any topics in the response
    if response.topics.is_empty() {
        debug!("No topics in response. Client topics: {:?}", state.topics.keys().collect::<Vec<_>>());
    } else {
        debug!("Response contains {} topics: {:?}", 
               response.topics.len(), 
               response.topics.iter().filter_map(|t| t.name.as_ref().map(|n| n.0.to_string())).collect::<Vec<_>>());
    }

    let response_size = response.compute_size(api_version)? as i32;
    debug!("Metadata response size: {} bytes", response_size);
    debug!("Metadata response: {:?}", response);

    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::Metadata(response),
        response_size
    })
}

fn create_topic_metadata(name: &str, partitions: &[TopicPartition]) -> MetadataResponseTopic {
    let topic_metadata = MetadataResponseTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(name.to_string()))))
        .with_partitions(partitions.iter().map(|p| {
            metadata_response::MetadataResponsePartition::default()
                .with_partition_index(p.partition_id)
                .with_leader_id(BrokerId(p.leader))
                .with_replica_nodes(p.replicas.iter().map(|&id| BrokerId(id)).collect())
                .with_isr_nodes(p.isr.iter().map(|&id| BrokerId(id)).collect())
        }).collect())
        .with_error_code(0);
    
    debug!("Created topic metadata for '{}' with {} partitions", name, partitions.len());
    topic_metadata
} 