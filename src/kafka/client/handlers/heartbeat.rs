use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use log::{debug, error, info};
use crate::kafka::consumer::actor::MemberAction;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_heartbeat(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the HeartbeatRequest from the request
    let typed_request = if let RequestKind::Heartbeat(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected Heartbeat request"));
    };
    
    let group_id = typed_request.group_id.to_string();
    let member_id = typed_request.member_id.to_string();
    let generation_id = typed_request.generation_id;
    
    debug!("Handling Heartbeat request: group_id={}, member_id={}, generation_id={}",
          group_id, member_id, generation_id);

    // Check if the group is active
    if !state.active_groups.contains_key(&group_id) {
        debug!("Group {} is not active", group_id);
        let response = HeartbeatResponse::default()
            .with_error_code(25) // UNKNOWN_MEMBER_ID
            .with_throttle_time_ms(0);
        
        let response_size = response.compute_size(api_version)? as i32;
        return Ok(KafkaResponseMessage {
            request_header: request.header,
            api_key: request.api_key,
            response: ResponseKind::Heartbeat(response),
            response_size
        });
    }

    // Get mutable reference to group info from active groups
    let group_info = state.active_groups.get_mut(&group_id).unwrap();

    // Check group info and determine error code
    let mut error_code = 0; // SUCCESS by default
    
    if let Some(group_api_info) = state.consumer_groups_api.get_group_info(&group_id) {
        let (leader, current_generation, last_change) = group_api_info;
        
        // Check for generation mismatch
        if current_generation != generation_id {
            debug!("Member {} has outdated generation {} (current is {}), requesting rejoin",
                member_id, generation_id, current_generation);
            
            // Store the current last_change timestamp to avoid continuous rebalancing
            if member_id == leader {
                group_info.last_generation_change = Some(last_change);
                debug!("Leader updated timestamp to avoid continuous rebalancing");
            }
            
            error_code = 22; // REBALANCE_IN_PROGRESS
        }
        
        // If this member is the leader, check for group membership changes
        else if member_id == leader {
            // Check if the last_change timestamp has changed since our last known timestamp
            // This indicates membership changes (joins/leaves) happened
            let is_membership_changed = match group_info.last_generation_change {
                Some(stored_change) => {
                    let changed = stored_change != last_change;
                    if changed {
                        info!("Leader detected timestamp change for group {}: stored={:?}, current={:?}", 
                              group_id, stored_change, last_change);
                    } else {
                        debug!("No timestamp change for group {}: stored={:?}, current={:?}",
                               group_id, stored_change, last_change);
                    }
                    changed
                },
                None => {
                    info!("Leader has no stored timestamp for group {}, initializing with {:?}", 
                          group_id, last_change);
                    true
                }
            };
            
            if is_membership_changed {
                info!("Leader detected membership change in group {}, {:?} initiating rebalance", 
                      group_id, last_change);
                error_code = 22; // REBALANCE_IN_PROGRESS
            }
        }
    }

    // Record the heartbeat
    if let Err(e) = state.consumer_groups_api.write_action(
        MemberAction::Heartbeat,
        state.client_id.clone(),
        state.client_host.clone(),
        group_id,
        generation_id,
        &group_info,
    ).await {
        error!("Failed to record heartbeat: {:?}", e);
        error_code = 15; // GROUP_COORDINATOR_NOT_AVAILABLE
    }
    
    // Create response
    let response = HeartbeatResponse::default()
        .with_error_code(error_code)
        .with_throttle_time_ms(0);
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::Heartbeat(response),
        response_size
    })
}
