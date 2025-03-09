use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::protocol::Encodable;
use log::{debug, info};

use crate::kafka::client::KafkaClient;

pub(crate) async fn handle_heartbeat(
    client: &mut KafkaClient,
    request: &HeartbeatRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling Heartbeat request: group_id={:?}, member_id={:?}, generation_id={}",
          request.group_id, request.member_id, request.generation_id);
    
    // Check if the group and member exist
    let group_id = request.group_id.0.to_string();
    let member_id = request.member_id.to_string();
    
    let error_code = if let Some(group_members) = client.consumer_groups.get(&group_id) {
        if let Some(&stored_generation) = group_members.get(&member_id) {
            if stored_generation == request.generation_id {
                // Valid heartbeat
                0
            } else {
                // Generation mismatch
                debug!("Generation mismatch: expected {}, got {}", 
                       stored_generation, request.generation_id);
                22 // ILLEGAL_GENERATION
            }
        } else {
            // Member not found
            debug!("Member {} not found in group {}", member_id, group_id);
            25 // UNKNOWN_MEMBER_ID
        }
    } else {
        // Group not found
        debug!("Group {} not found", group_id);
        15 // UNKNOWN_MEMBER_ID (used for unknown group too)
    };
    
    // Create response
    let response = HeartbeatResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::Heartbeat(response), response_size))
} 