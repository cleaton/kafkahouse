use anyhow::Result;
use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;
use log::{debug, info};
use std::collections::BTreeMap;

use crate::kafka::client::KafkaClient;

/// Represents a topic and its partitions for consumer group assignment
struct TopicPartitions {
    /// The name of the topic
    topic: String,
    /// The list of partition IDs assigned to this topic
    partitions: Vec<i32>,
}

impl TopicPartitions {
    /// Creates a new TopicPartitions instance
    fn new(topic: String, partitions: Vec<i32>) -> Self {
        Self { topic, partitions }
    }
}

/// Creates a consumer group assignment for the given topics and their partitions.
/// 
/// The assignment format follows the Kafka consumer protocol:
/// - Version (int16): 0
/// - Topic count (int32): number of topics
/// - For each topic:
///   - Topic name (string): topic name
///   - Partition count (int32): number of partitions for this topic
///   - Partition IDs (int32 array): array of partition IDs for this topic
/// - User data length (int32): 0
fn create_consumer_assignment(topics: &[TopicPartitions]) -> BytesMut {
    let mut assignment_buf = BytesMut::new();
    
    // Version: 0 (int16)
    assignment_buf.extend_from_slice(&0i16.to_be_bytes());
    
    // Topic count (int32)
    let topic_count = topics.len() as i32;
    assignment_buf.extend_from_slice(&topic_count.to_be_bytes());
    
    // For each topic
    let mut total_partitions = 0;
    for topic_partitions in topics {
        // Topic name (string)
        let topic_bytes = topic_partitions.topic.as_bytes();
        assignment_buf.extend_from_slice(&(topic_bytes.len() as i16).to_be_bytes());
        assignment_buf.extend_from_slice(topic_bytes);
        
        // Partition count (int32)
        let partition_count = topic_partitions.partitions.len() as i32;
        assignment_buf.extend_from_slice(&partition_count.to_be_bytes());
        total_partitions += partition_count;
        
        // Partition IDs (int32 array)
        for &partition_id in &topic_partitions.partitions {
            assignment_buf.extend_from_slice(&partition_id.to_be_bytes());
        }
    }
    
    // User data length (int32): 0
    assignment_buf.extend_from_slice(&0i32.to_be_bytes());
    
    debug!("Created assignment with {} topics and {} total partitions", 
           topic_count, total_partitions);
    
    assignment_buf
}

pub(crate) async fn handle_sync_group(
    client: &mut KafkaClient,
    request: &SyncGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling SyncGroup request: group_id={:?}, member_id={:?}, generation_id={}",
          request.group_id, request.member_id, request.generation_id);
    
    // Get the member ID
    let member_id = request.member_id.to_string();
    
    // Get the subscribed topics for this member
    let subscribed_topics = client.member_subscriptions.get(&member_id)
        .cloned()
        .unwrap_or_else(|| vec!["testing_clickhouse_broker".to_string()]);
    
    debug!("Member {} is subscribed to topics: {:?}", member_id, subscribed_topics);
    
    // Create topic partitions for each subscribed topic
    let mut topic_partitions = Vec::new();
    for topic in subscribed_topics {
        // For simplicity, we'll assign all partitions (0-99) for each topic
        // In a real implementation, we would distribute partitions among group members
        topic_partitions.push(TopicPartitions::new(topic, (0..100).collect()));
    }
    
    // Create the consumer assignment
    let assignment_buf = create_consumer_assignment(&topic_partitions);
    
    // Create response
    let assignment_size = assignment_buf.len();
    let response = SyncGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_protocol_type(Some(StrBytes::from_string("consumer".to_string())))
        .with_protocol_name(Some(StrBytes::from_string("range".to_string())))
        .with_assignment(assignment_buf.freeze())
        .with_unknown_tagged_fields(BTreeMap::new());
    
    debug!("SyncGroup response sent with assignment of size {} bytes", assignment_size);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::SyncGroup(response), response_size))
}