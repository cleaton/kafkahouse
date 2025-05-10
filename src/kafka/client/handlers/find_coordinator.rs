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
    
    debug!("Handling FindCoordinator request: ${:?}", typed_request);
    
    // Create response with current broker as coordinator
    let response = FindCoordinatorResponse::default()
        .with_error_code(0) // No error
        .with_error_message(None)
        .with_node_id(BrokerId(state.broker_id))
        .with_host(StrBytes::from_string(state.broker.get_listen_host().to_string()))
        .with_port(state.broker.get_listen_port());
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::FindCoordinator(response),
        response_size
    })
}
