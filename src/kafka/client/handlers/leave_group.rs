use anyhow::Result;
use log::{info, error, debug};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use crate::kafka::consumer::actor::MemberAction;
use std::time::Instant;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_leave_group(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the LeaveGroupRequest from the request
    let typed_request = if let RequestKind::LeaveGroup(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected LeaveGroup request"));
    };
    
    let group_id = typed_request.group_id.to_string();
    info!("Handling LeaveGroup request: group_id={}, api_version={}", group_id, api_version);

    // Get current group info
    let _base_group_info = state.active_groups.get(&group_id)
        .ok_or_else(|| anyhow::anyhow!("Group not found"))?;

    // Handle both newer format (members array) and older format (single member_id)
    if !typed_request.members.is_empty() {
        // Process each member in the request (newer protocol version)
        info!("Processing leave request with {} members in the array", typed_request.members.len());
        for member in typed_request.members {
            process_member_leave(
                state, 
                &group_id, 
                member.member_id.to_string()
            ).await;
        }
    } else {
        // Process the single member_id (older protocol version)
        let member_id = typed_request.member_id.to_string();
        info!("Processing leave request with single member_id: {}", member_id);
        
        process_member_leave(
            state, 
            &group_id, 
            member_id
        ).await;
    }

    // Create a super minimal response with just error_code, like in the working example
    let response = LeaveGroupResponse::default()
        .with_error_code(0);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::LeaveGroup(response),
        response_size,
    })
}

// Helper function to process a member leaving
async fn process_member_leave(
    state: &mut ClientState,
    group_id: &str,
    member_id: String
) {
    info!("Processing leave request for member: {}", member_id);

    // Get existing group info for this member if it exists
    if let Some(group_info) = state.active_groups.get(group_id) {
        if group_info.member_id == member_id {
            // Get current generation from consumer groups API
            let (_, generation, _) = state.consumer_groups_api
                .get_group_info(group_id)
                .unwrap_or(("".to_string(), 0, Instant::now()));

            info!("Member {} is leaving group {} with generation {}", 
                   member_id, group_id, generation);

            // Write leave action to ClickHouse with current generation
            match state.consumer_groups_api.write_action(
                MemberAction::LeaveGroup,
                state.client_id.clone(),
                state.client_host.clone(),
                group_id.to_string(),
                generation,
                group_info,
            ).await {
                Ok(_) => {
                    info!("Successfully recorded leave action for member {}", member_id);
                    
                    // Remove from active groups
                    state.active_groups.remove(group_id);
                }
                Err(e) => {
                    error!("Failed to process leave for member {}: {}", member_id, e);
                }
            }
        } else {
            debug!("Member {} not found in group {}, but still responding with success", 
                   member_id, group_id);
        }
    } else {
        debug!("Group {} not found for member {}, but still responding with success", 
               group_id, member_id);
    }
}
