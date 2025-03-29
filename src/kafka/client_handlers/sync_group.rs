use anyhow::Result;
use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap};
use std::collections::HashSet;

use crate::kafka::client::KafkaClient;
use crate::kafka::consumer_group::GroupMember;

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
    let group_id = request.group_id.to_string();
    let member_id = request.member_id.to_string();
    let generation_id = request.generation_id;
    
    info!("Handling SyncGroup request: group_id={}, member_id={}, generation_id={}",
          group_id, member_id, generation_id);
    
    // Check if this member is the leader
    let is_leader = if let Some(leader) = client.consumer_group_cache.get_group_leader(&group_id) {
        leader.member_id == member_id
    } else {
        // If no group exists yet, the first member becomes leader
        true
    };
    
    // Get all subscribed topics for all members in the group
    let mut all_topics: HashSet<String> = HashSet::new();
    let mut member_topics: HashMap<String, Vec<String>> = HashMap::new();
    
    // Add this member's subscribed topics
    if let Some(topics) = client.member_subscriptions.get(&member_id) {
        all_topics.extend(topics.clone());
        member_topics.insert(member_id.clone(), topics.clone());
    }
    
    // If leader, gather member assignments from all sync group data
    if is_leader {
        debug!("Member {} is the leader of group {}", member_id, group_id);
        
        // Process leader's assignments for all members
        for assignment_data in &request.assignments {
            let member_id = assignment_data.member_id.to_string();
            
            // Store member assignment
            if let Some(topics) = client.member_subscriptions.get(&member_id) {
                all_topics.extend(topics.clone());
                member_topics.insert(member_id, topics.clone());
            }
        }
        
        // Get partition count information for each topic
        // For simplicity, assume 10 partitions per topic
        let partition_counts: HashMap<String, i32> = all_topics.iter()
            .map(|topic| (topic.clone(), 10))
            .collect();
        
        // Convert topic set to vector for assignment
        let topics_vec: Vec<String> = all_topics.into_iter().collect();
        
        // Get all members from the client's local state
        let all_members: Vec<GroupMember> = if let Some(group) = client.consumer_group_cache.get_group(&group_id) {
            group.members
        } else {
            Vec::new()
        };
            
        // Create assignments using GroupMember's built-in function
        let assignments = GroupMember::create_assignments(
            &group_id,
            &all_members,
            &topics_vec,
            &partition_counts
        );
        
        debug!("Leader created assignments for group {}: {:?}", group_id, assignments);
        
        // TODO: Store assignments in ClickHouse via the broker
        // This would happen in a full implementation
    }
    
    // Get individual member's assignment (different for leader vs member)
    let mut member_assignment = HashMap::new();
    if let Some(group) = client.consumer_group_cache.get_group(&group_id) {
        for group_member in &group.members {
            if group_member.member_id == member_id {
                // Found our member, extract topic-partitions
                member_assignment = group_member.assignments.clone();
                break;
            }
        }
    }
    
    // Convert our member's assignment to TopicPartitions format for response
    let mut topic_partitions = Vec::new();
    for (topic, partitions) in &member_assignment {
        topic_partitions.push(TopicPartitions::new(topic.clone(), partitions.clone()));
    }
    
    // If we have no assignments yet, just assign all subscribed topics
    if topic_partitions.is_empty() {
        if let Some(topics) = client.member_subscriptions.get(&member_id) {
            for topic in topics {
                // For simplicity, assign all partitions 0-9 to this member
                topic_partitions.push(TopicPartitions::new(topic.clone(), (0..10).collect()));
            }
        }
    }
    
    // Create the consumer assignment for this member
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
    
    debug!("SyncGroup response sent to {} with assignment of size {} bytes", member_id, assignment_size);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::SyncGroup(response), response_size))
}