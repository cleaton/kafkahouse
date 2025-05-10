use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use log::{debug, error};
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
    
    debug!("Handling Heartbeat request: ${:?}", typed_request);

    let group_id = typed_request.group_id.to_string();

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

    // Check if we need to rejoin due to generation change
    if let Some(group_info) = state.consumer_groups_api.get_group_info(&typed_request.group_id) {
        let (_, current_generation, _) = group_info;
        if current_generation != typed_request.generation_id {
            debug!("Generation mismatch for group {:?}: client={}, server={}", 
                typed_request.group_id, typed_request.generation_id, current_generation);
            let response = HeartbeatResponse::default()
                .with_error_code(22) // REBALANCE_IN_PROGRESS
                .with_throttle_time_ms(0);
            
            let response_size = response.compute_size(api_version)? as i32;
            return Ok(KafkaResponseMessage {
                request_header: request.header,
                api_key: request.api_key,
                response: ResponseKind::Heartbeat(response),
                response_size
            });
        }
    }

    // Get the group info from active groups
    let group_info = state.active_groups.get(&group_id).unwrap();

    // Record the heartbeat
    if let Err(e) = state.consumer_groups_api.write_action(
        MemberAction::Heartbeat,
        state.client_id.clone(),
        state.client_host.clone(),
        group_id,
        typed_request.generation_id,
        &group_info,
    ).await {
        error!("Failed to record heartbeat: {:?}", e);
        let response = HeartbeatResponse::default()
            .with_error_code(15) // GROUP_COORDINATOR_NOT_AVAILABLE
            .with_throttle_time_ms(0);
        
        let response_size = response.compute_size(api_version)? as i32;
        return Ok(KafkaResponseMessage {
            request_header: request.header,
            api_key: request.api_key,
            response: ResponseKind::Heartbeat(response),
            response_size
        });
    }
    
    // Success response
    let response = HeartbeatResponse::default()
        .with_error_code(0) // No error
        .with_throttle_time_ms(0);
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::Heartbeat(response),
        response_size
    })
}
