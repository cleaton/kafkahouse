use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::{debug, info};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::kafka::client::KafkaClient;
use crate::kafka::consumer_group::{GroupMember, TopicAssignments};

/// Extract subscribed topics from the member metadata
fn extract_subscribed_topics(protocol_metadata: &Bytes) -> Vec<String> {
    // Try to decode the consumer protocol metadata
    // Format:
    // - Version (int16)
    // - Topics count (int32)
    // - Topic names (string array)
    // - User data (bytes)
    if protocol_metadata.len() < 6 {
        return vec!["testing_clickhouse_broker".to_string()]; // Default topic if metadata is too short
    }

    let mut topics = Vec::new();
    let mut cursor = 0;

    // Skip version (int16)
    cursor += 2;

    // Read topics count (int32)
    if cursor + 4 > protocol_metadata.len() {
        return vec!["testing_clickhouse_broker".to_string()]; // Default topic if metadata is too short
    }
    let topics_count = i32::from_be_bytes([
        protocol_metadata[cursor],
        protocol_metadata[cursor + 1],
        protocol_metadata[cursor + 2],
        protocol_metadata[cursor + 3],
    ]);
    cursor += 4;

    // Read each topic name
    for _ in 0..topics_count {
        if cursor + 2 > protocol_metadata.len() {
            break;
        }
        
        // Read topic name length (int16)
        let topic_len = i16::from_be_bytes([
            protocol_metadata[cursor],
            protocol_metadata[cursor + 1],
        ]) as usize;
        cursor += 2;
        
        if cursor + topic_len > protocol_metadata.len() {
            break;
        }
        
        // Read topic name
        let topic_name = String::from_utf8_lossy(&protocol_metadata[cursor..cursor + topic_len]).to_string();
        topics.push(topic_name);
        cursor += topic_len;
    }

    // If no topics were found, use the default topic
    if topics.is_empty() {
        topics.push("testing_clickhouse_broker".to_string());
    }

    topics
}

pub(crate) async fn handle_join_group(
    client: &mut KafkaClient,
    request: &JoinGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling JoinGroup request: group_id={:?}, member_id={:?}, protocol_type={:?}",
          request.group_id, request.member_id, request.protocol_type);
    
    let group_id = request.group_id.to_string();
    
    // Generate a member ID if one wasn't provided
    let member_id = if request.member_id.is_empty() {
        // Create a unique member ID using timestamp and a counter
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        
        format!("{}_{}", group_id, timestamp)
    } else {
        request.member_id.to_string()
    };
    
    debug!("Assigned member ID: {}", member_id);
    
    // Extract subscribed topics
    let mut subscribed_topics = Vec::new();
    
    if let Some(protocol) = request.protocols.first() {
        subscribed_topics = extract_subscribed_topics(&protocol.metadata);
        debug!("Member {} subscribed to topics: {:?}", member_id, subscribed_topics);
        
        // Store the subscribed topics in the client for later use
        client.member_subscriptions.insert(member_id.clone(), subscribed_topics.clone());
    }
    
    // Check existing group state from cache
    let group_exists = client.consumer_group_cache.get_group(&group_id).is_some();
    let generation_id = client.consumer_group_cache.get_group_generation(&group_id).unwrap_or(1);
    
    // For now, set initial generation to 1 if it's a new group
    let generation_id = if group_exists { generation_id } else { 1 };
    
    // Store in local client map for quick reference
    client.consumer_groups.entry(group_id.clone())
        .or_insert_with(HashMap::new)
        .insert(member_id.clone(), generation_id);
    
    // Get leader from shared cache or set this member as leader if it's a new group
    let leader_id = if let Some(leader) = client.consumer_group_cache.get_group_leader(&group_id) {
        leader.member_id
    } else {
        // If no leader exists, this member becomes the leader
        member_id.clone()
    };
    
    debug!("JoinGroup response for {}: leader={}, member_id={}, generation_id={}",
           group_id, leader_id, member_id, generation_id);
    
    // Build the response with members list if this is the leader
    let members = Vec::new(); // We'll populate this in the sync phase
    
    // Create the response
    let response = JoinGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_generation_id(generation_id)
        .with_protocol_type(Some(request.protocol_type.clone()))
        .with_protocol_name(request.protocols.first().map(|p| p.name.clone()))
        .with_leader(StrBytes::from_string(leader_id))
        .with_member_id(StrBytes::from_string(member_id))
        .with_members(members);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::JoinGroup(response), response_size))
} 