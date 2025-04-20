use kafka_protocol::messages::*;
use kafka_protocol::messages::find_coordinator_response::{Coordinator, FindCoordinatorResponse};
use kafka_protocol::protocol::{Encodable, StrBytes};
use log::info;

use crate::kafka::client_actor::ClientState;

pub(crate) fn handle_find_coordinator(client: &ClientState, request: &FindCoordinatorRequest, api_version: i16) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling FindCoordinator request: key={:?}, key_type={:?}", request.key, request.key_type);
    
    // For simplicity, we'll return this broker as the coordinator for all keys
    let coordinator = Coordinator::default()
        .with_node_id(BrokerId(client.broker_id))
        .with_host(StrBytes::from_string("127.0.0.1".to_string()))
        .with_port(9092);
    
    let mut response = FindCoordinatorResponse::default();
    
    // Kafka API v >= 1 can return multiple coordinators
    if api_version >= 1 {
        response.coordinators = vec![coordinator];
    } else {
        response.node_id = BrokerId(client.broker_id);
        response.host = StrBytes::from_string("127.0.0.1".to_string());
        response.port = 9092;
    }
    
    response.error_code = 0; // SUCCESS
    response.error_message = None;
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::FindCoordinator(response), response_size))
}
