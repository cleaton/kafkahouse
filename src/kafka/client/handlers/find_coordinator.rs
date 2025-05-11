use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Encodable, StrBytes};
use crate::kafka::client::types::ClientState;
use log::debug;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_find_coordinator(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the FindCoordinatorRequest from the request
    let typed_request = if let RequestKind::FindCoordinator(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected FindCoordinator request"));
    };
    
    debug!("Handling FindCoordinator request: api_version={}", api_version);
    
    // Create response with current broker as coordinator, handling version differences
    let mut response = FindCoordinatorResponse::default()
        .with_error_code(0);
    
    // Handle different versions appropriately
    if api_version <= 3 {
        // For older versions (v0-v3), we use individual fields
        response = response
            .with_node_id(BrokerId(state.broker_id))
            .with_host(StrBytes::from_string("127.0.0.1".to_string()))
            .with_port(9092);
    } else {
        // For v4+, we need to use the coordinators field
        let coordinator_key = if typed_request.coordinator_keys.is_empty() {
            typed_request.key.to_string()
        } else {
            typed_request.coordinator_keys[0].to_string()
        };
        
        let coordinator = find_coordinator_response::Coordinator::default()
            .with_key(StrBytes::from_string(coordinator_key))
            .with_node_id(BrokerId(state.broker_id))
            .with_host(StrBytes::from_string("127.0.0.1".to_string()))
            .with_port(9092)
            .with_error_code(0)
            .with_error_message(None);
        
        response = response.with_coordinators(vec![coordinator]);
    }
    
    // Add throttle_time_ms only for version 1+
    if api_version >= 1 {
        response = response.with_throttle_time_ms(0);
    }
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::FindCoordinator(response),
        response_size
    })
}
