use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
use kafka_protocol::protocol::Encodable;
use log::{debug, info};

use crate::kafka::client::KafkaClient;

pub(crate) async fn handle_leave_group(
    client: &mut KafkaClient,
    request: &LeaveGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    let group_id = request.group_id.to_string();
    info!("Handling LeaveGroup request: group_id={}", group_id);
    
    let mut error_code = 0;
    
    // Process all members that are leaving
    if let Some(group_members) = client.consumer_groups.get_mut(&group_id) {
        // In newer versions, we have a list of members
        if !request.members.is_empty() {
            for member in &request.members {
                let member_id = member.member_id.to_string();
                debug!("Removing member {} from group {}", member_id, group_id);
                group_members.remove(&member_id);
                
                // Remove from subscriptions as well
                client.member_subscriptions.remove(&member_id);
            }
        } else if !request.member_id.is_empty() {
            // In older versions, we have a single member_id
            let member_id = request.member_id.to_string();
            debug!("Removing member {} from group {}", member_id, group_id);
            group_members.remove(&member_id);
            
            // Remove from subscriptions as well
            client.member_subscriptions.remove(&member_id);
        }
        
        // If the group is now empty, remove it
        if group_members.is_empty() {
            debug!("Group {} is now empty, removing", group_id);
            client.consumer_groups.remove(&group_id);
        }
    } else {
        // Group not found
        debug!("Group {} not found", group_id);
        error_code = 15; // UNKNOWN_MEMBER_ID (used for unknown group too)
    }
    
    // Note: In a complete implementation, we would also update the ClickHouse database
    // to remove the members, but this is handled by the cache worker which will
    // detect that members are missing from the local state when it does the next update.
    
    // Create response
    let response = LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::LeaveGroup(response), response_size))
} 