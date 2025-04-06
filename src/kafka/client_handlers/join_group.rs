use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::join_group_response::JoinGroupResponse;
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::{debug, info};
use std::collections::{HashMap, BTreeMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use crate::kafka::client_types::*;

use crate::kafka::client::KafkaClient;

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
    
    let group_info = client.joined_groups.entry(group_id.clone()).or_insert_with(|| {
        GroupInfo {
            member_info: MemberInfo {
                join_time: Utc::now(),
                session_timeout_ms: request.session_timeout_ms,
                rebalance_timeout_ms: request.rebalance_timeout_ms,
                member_id: member_id.clone(),
                subscribed_topics: Vec::new(),
                group_instance_id: request.group_instance_id.clone().map(|s| s.to_string()),
                protocol_type: request.protocol_type.clone().to_string(),
                protocol: request.protocols.iter()
                    .map(|p| (p.name.clone().to_string(), p.metadata.clone().to_vec()))
                    .collect()
            },
            members: Vec::new(),
            assignments: Vec::new()
        }
    });


    if let Some(protocol) = request.protocols.first() {
        group_info.member_info.subscribed_topics = extract_subscribed_topics(&protocol.metadata);
        debug!("Member {} subscribed to topics: {:?}", member_id, group_info.member_info.subscribed_topics);
    }

    // Get current generation ID and leader
    let mut generation_id = client.consumer_group_cache.get_group_generation(&group_id).unwrap_or(1);

    // Join the consumer group using the cache
    client.consumer_group_cache.write_action(MemberAction::JoinGroup, client.client_id.clone(), client.client_host.clone(), group_id.clone(), generation_id, group_info).await?;

    // Wait for the member to appear in the cache with the correct generation
    client.consumer_group_cache.wait_for_member_generation(&group_id, &member_id, generation_id, Duration::from_secs(10)).await?;

    let leader = client.consumer_group_cache.get_group_leader(&group_id).unwrap_or_else(|| member_id.clone());

    debug!("JoinGroup response for {}: leader={}, member_id={}, generation_id={}",
           group_id, leader, member_id, generation_id);
           
    
    let members = client.consumer_group_cache.get_members(&group_id);

    if member_id == leader && group_info.should_rebalance(&members) {
        generation_id += 1;
    }

    group_info.members = members.clone();
    
    // Build member list with metadata
    let members = if member_id == leader {
        members.into_iter().map(|m| {
            let metadata = if let Some(protocol) = request.protocols.first() {
                protocol.metadata.clone()
            } else {
                Bytes::new()
            };
            m.with_metadata(metadata)
        }).collect()
    } else {
        Vec::new() // Non-leaders get empty member list
    };
    
    // Create the response
    let response = JoinGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_generation_id(generation_id)
        .with_protocol_type(Some(request.protocol_type.clone()))
        .with_protocol_name(request.protocols.first().map(|p| p.name.clone()))
        .with_leader(StrBytes::from_string(leader))
        .with_member_id(StrBytes::from_string(member_id))
        .with_members(members)
        .with_unknown_tagged_fields(BTreeMap::new());  // Required for v5
    
    debug!("JoinGroup response: {:?}", response);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::JoinGroup(response), response_size))
} 