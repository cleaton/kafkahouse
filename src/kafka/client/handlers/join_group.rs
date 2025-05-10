use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::protocol::{Encodable, StrBytes};
use crate::kafka::client::types::ClientState;
use crate::kafka::consumer::actor::{GroupInfo, MemberAction};
use log::{debug, info};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use chrono::Utc;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

/// Extract subscribed topics from the member metadata
fn extract_subscribed_topics(protocol_metadata: &[u8]) -> Vec<String> {
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

pub(crate) async fn handle_join_group(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the JoinGroupRequest from the request
    let typed_request = if let RequestKind::JoinGroup(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected JoinGroup request"));
    };
    
    info!("Handling JoinGroup request: group_id={:?}, member_id={:?}, protocol_type={:?}",
          typed_request.group_id, typed_request.member_id, typed_request.protocol_type);

    let group_id = typed_request.group_id.to_string();
    
    // Generate a member ID if one wasn't provided
    let member_id = if typed_request.member_id.is_empty() {
        // Create a unique member ID using timestamp and a counter
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        
        format!("{}_{}", group_id, timestamp)
    } else {
        typed_request.member_id.to_string()
    };

    // Extract protocols and subscribed topics
    let protocols: Vec<(String, Vec<u8>)> = typed_request.protocols
        .iter()
        .map(|p| (p.name.to_string(), p.metadata.to_vec()))
        .collect();

    let subscribed_topics = if let Some(protocol) = typed_request.protocols.first() {
        extract_subscribed_topics(&protocol.metadata)
    } else {
        vec!["testing_clickhouse_broker".to_string()]
    };

    debug!("Member {} subscribed to topics: {:?}", member_id, subscribed_topics);

    // Get existing group info or create new one
    let group_info = state.active_groups.entry(group_id.clone())
        .or_insert_with(|| GroupInfo {
            member_id: member_id.clone(),
            protocol_type: typed_request.protocol_type.to_string(),
            protocols: protocols.clone(),
            subscribed_topics: subscribed_topics.clone(),
            last_generation_change: None,
            join_time: Utc::now(),
            assignments: Vec::new(),
        });

    // Get current generation and leader
    let (leader, mut generation, last_change) = state.consumer_groups_api
        .get_group_info(&group_id)
        .unwrap_or_else(|| (member_id.clone(), 0, Instant::now()));

    // Write join action to ClickHouse
    state.consumer_groups_api.write_action(
        MemberAction::JoinGroup,
        state.client_id.clone(),
        state.client_host.clone(),
        group_id.clone(),
        generation,
        &group_info,
    ).await?;

    // Wait for the member to appear in the cache
    state.consumer_groups_api.wait_join(group_id.clone(), member_id.clone()).await;

    // Get the latest group members from cache
    let members = if member_id == leader {
        debug!("Member {} is the leader, fetching all members", member_id);
        let members = state.consumer_groups_api
            .get_group_members(&group_id)
            .unwrap_or_default();
        
        debug!("Found {} members in group {}", members.len(), group_id);
        
        members.into_iter()
            .map(|m| {
                let protocol_metadata = m.protocol.first()
                    .map(|(_, metadata)| metadata.clone())
                    .unwrap_or_default();
                
                debug!("Adding member {} with {}B metadata", m.member_id, protocol_metadata.len());
                
                JoinGroupResponseMember::default()
                    .with_member_id(StrBytes::from_string(m.member_id))
                    .with_metadata(protocol_metadata.into())
            })
            .collect()
    } else {
        debug!("Member {} is not the leader, empty member list", member_id);
        Vec::new()
    };

    debug!("JoinGroup response: {} members for {}", members.len(), member_id);

        // If we're the leader and there's been a change since our last generation change,
    // increment the generation
    if member_id == leader && group_info.last_generation_change.is_none_or(|i| i != last_change) {
        generation += 1;
        group_info.last_generation_change = Some(last_change);
        debug!("Leader incrementing generation from {} to {}", generation - 1, generation);
    }

    // Create response
    let response = JoinGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_generation_id(generation)
        .with_protocol_type(Some(typed_request.protocol_type))
        .with_protocol_name(typed_request.protocols.first().map(|p| p.name.clone()))
        .with_leader(StrBytes::from_string(leader))
        .with_member_id(StrBytes::from_string(member_id))
        .with_members(members)
        .with_unknown_tagged_fields(BTreeMap::new());

    debug!("JoinGroup response: {:?}", response);

    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;

    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::JoinGroup(response),
        response_size,
    })
}
