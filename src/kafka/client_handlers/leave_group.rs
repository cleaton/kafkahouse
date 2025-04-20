use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
use kafka_protocol::protocol::Encodable;
use log::info;

use crate::kafka::client_actor::ClientState;

pub(crate) async fn handle_leave_group(
    client: &mut ClientState,
    request: &LeaveGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling LeaveGroup request: group_id={:?}", request.group_id);
    
    let group_id = request.group_id.0.to_string();
    
    // Remove the group from our joined_groups
    let group_existed = client.joined_groups.remove(&group_id).is_some();
    
    // Create response
    let response = LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(if group_existed { 0 } else { 16 }); // SUCCESS or UNKNOWN_MEMBER_ID
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::LeaveGroup(response), response_size))
} 