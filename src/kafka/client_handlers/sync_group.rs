use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::protocol::Encodable;
use log::info;

use crate::kafka::client_actor::ClientState;


pub(crate) async fn handle_sync_group(
    client: &mut ClientState,
    request: &SyncGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling SyncGroup request: group_id={:?}, member_id={:?}, generation={:?}",
          request.group_id, request.member_id, request.generation_id);
    
    let group_id = request.group_id.0.to_string();
    
    // Get assignment data from leader
    let assignment_data = if !request.assignments.is_empty() {
        // This is the leader's assignment
        request.assignments[0].assignment.clone()
    } else {
        // Default empty assignment
        bytes::Bytes::new()
    };
    
    // Create response
    let response = SyncGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0) // SUCCESS
        .with_assignment(assignment_data);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::SyncGroup(response), response_size))
}