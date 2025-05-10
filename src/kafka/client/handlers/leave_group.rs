use anyhow::Result;
use log::{info, error};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Encodable, StrBytes};
use crate::kafka::client::types::ClientState;
use crate::kafka::consumer::actor::{GroupInfo, MemberAction};
use std::collections::BTreeMap;
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
    info!("Handling LeaveGroup request: group_id={}", group_id);

    // Get current group info
    let base_group_info = state.active_groups.get(&group_id)
        .ok_or_else(|| anyhow::anyhow!("Group not found"))?;

    // Process each member in the request
    let mut member_responses = Vec::new();
    for member in typed_request.members {
        let member_id = member.member_id.to_string();
        info!("Processing leave request for member: {}", member_id);

        // Get existing group info for this member if it exists
        if let Some(group_info) = state.active_groups.get(&group_id) {
            if group_info.member_id == member_id {
                // Get current generation from consumer groups API
                let (_, generation, _) = state.consumer_groups_api
                    .get_group_info(&group_id)
                    .unwrap_or(("".to_string(), 0, Instant::now()));

                // Write leave action to ClickHouse with current generation
                match state.consumer_groups_api.write_action(
                    MemberAction::LeaveGroup,
                    state.client_id.clone(),
                    state.client_host.clone(),
                    group_id.clone(),
                    generation,
                    group_info,
                ).await {
                    Ok(_) => {
                        // Remove from active groups
                        state.active_groups.remove(&group_id);
                        member_responses.push(leave_group_response::MemberResponse::default()
                            .with_member_id(StrBytes::from_string(member_id))
                            .with_group_instance_id(member.group_instance_id)
                            .with_error_code(0)); // 0 = success
                    }
                    Err(e) => {
                        error!("Failed to process leave for member {}: {}", member_id, e);
                        member_responses.push(leave_group_response::MemberResponse::default()
                            .with_member_id(StrBytes::from_string(member_id))
                            .with_group_instance_id(member.group_instance_id)
                            .with_error_code(1)); // 1 = error
                    }
                }
            } else {
                // Member not found in active groups but still respond with success
                member_responses.push(leave_group_response::MemberResponse::default()
                    .with_member_id(StrBytes::from_string(member_id))
                    .with_group_instance_id(member.group_instance_id)
                    .with_error_code(0));
            }
        } else {
            // Group not found but still respond with success
            member_responses.push(leave_group_response::MemberResponse::default()
                .with_member_id(StrBytes::from_string(member_id))
                .with_group_instance_id(member.group_instance_id)
                .with_error_code(0));
        }
    }

    // Create response with member-specific error codes
    let response = LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0) // Overall success, individual errors in members list
        .with_members(member_responses)
        .with_unknown_tagged_fields(BTreeMap::new());
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::LeaveGroup(response),
        response_size,
    })
}
