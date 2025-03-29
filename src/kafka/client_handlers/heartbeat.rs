use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::protocol::Encodable;
use log::{debug, info};
use std::collections::HashSet;

use crate::kafka::client::KafkaClient;

pub(crate) async fn handle_heartbeat(
    client: &mut KafkaClient,
    request: &HeartbeatRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    let group_id = request.group_id.to_string();
    let member_id = request.member_id.to_string();
    let generation_id = request.generation_id;
    
    info!("Handling Heartbeat request: group_id={}, member_id={}, generation_id={}",
          group_id, member_id, generation_id);
    
    // Check if member should rejoin based on shared cache state
    let should_rejoin = client.consumer_group_cache.should_member_rejoin(&group_id, &member_id, generation_id);
    
    // Check if this member is the leader
    let is_leader = if let Some(_group) = client.consumer_group_cache.get_group(&group_id) {
        if let Some(leader) = client.consumer_group_cache.get_group_leader(&group_id) {
            leader.member_id == member_id
        } else {
            false
        }
    } else {
        false
    };
    
    // Leader-specific logic - check if rebalance needed
    if is_leader {
        debug!("Member {} is the leader of group {}", member_id, group_id);
        
        // Check if expected members match current members
        if let Some(group_members) = client.consumer_groups.get(&group_id) {
            // Get current members from local state
            let expected_members: HashSet<String> = group_members.keys().cloned().collect();
            
            // Check if rebalance is needed
            let should_rebalance = client.consumer_group_cache.should_leader_rebalance(&group_id, &expected_members);
            
            if should_rebalance {
                debug!("Leader detected membership changes. Triggering rebalance for group {}", group_id);
                // Return REBALANCE_IN_PROGRESS to trigger rejoin
                let response = HeartbeatResponse::default()
                    .with_throttle_time_ms(0)
                    .with_error_code(27); // REBALANCE_IN_PROGRESS
                
                let response_size = response.compute_size(api_version)? as i32;
                return Ok((ResponseKind::Heartbeat(response), response_size));
            }
        }
    }
    
    // Determine error code based on whether member should rejoin
    let error_code = if should_rejoin {
        debug!("Member {} should rejoin group {}", member_id, group_id);
        if client.consumer_group_cache.get_group(&group_id).is_some() {
            // Group exists but member needs to rejoin - likely generation change
            22 // ILLEGAL_GENERATION
        } else {
            // Group doesn't exist
            25 // UNKNOWN_MEMBER_ID
        }
    } else {
        // Valid heartbeat
        0
    };
    
    // Create response
    let response = HeartbeatResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::Heartbeat(response), response_size))
} 