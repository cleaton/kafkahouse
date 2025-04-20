use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::protocol::Encodable;
use log::info;

use crate::kafka::client_actor::ClientState;

pub(crate) async fn handle_heartbeat(
    client: &mut ClientState,
    request: &HeartbeatRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling Heartbeat request: group_id={:?}, member_id={:?}, generation={:?}",
          request.group_id, request.member_id, request.generation_id);
    
    let group_id = request.group_id.0.to_string();
    let member_id = request.member_id.to_string();
    
    // Check if the group exists
    let exists = client.joined_groups.contains_key(&group_id);
    
    // Create response with appropriate error code
    let mut response = HeartbeatResponse::default()
        .with_throttle_time_ms(0);
    
    if !exists {
        // Group does not exist
        response = response.with_error_code(16); // UNKNOWN_MEMBER_ID
    } else {
        // Group exists, everything is fine
        response = response.with_error_code(0); // SUCCESS
    }
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::Heartbeat(response), response_size))
} 