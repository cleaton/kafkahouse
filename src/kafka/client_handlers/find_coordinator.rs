use kafka_protocol::messages::*;
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::protocol::{Encodable, StrBytes};
use crate::kafka::client::KafkaClient;
use log::debug;

pub(crate) fn handle_find_coordinator(
    client: &KafkaClient, 
    request: &FindCoordinatorRequest, 
    api_version: i16
) -> Result<(ResponseKind, i32), anyhow::Error> {
    debug!("Handling FindCoordinator request: {:?}", request);
    
    let mut response = FindCoordinatorResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0);
    
    // For older versions (v0-v3)
    if api_version <= 3 {
        response.node_id = BrokerId(client.broker_id);
        response.host = StrBytes::from_string("127.0.0.1".to_string());
        response.port = 9092;
    } else {
        // For v4+, we need to use the coordinators field
        let coordinator = find_coordinator_response::Coordinator::default()
            .with_key(request.key.clone())
            .with_node_id(BrokerId(client.broker_id))
            .with_host(StrBytes::from_string("127.0.0.1".to_string()))
            .with_port(9092)
            .with_error_code(0)
            .with_error_message(None);
        
        response.coordinators = vec![coordinator];
    }
    
    let response_size = response.compute_size(api_version)? as i32;
    debug!("FindCoordinator response size: {} bytes", response_size);
    
    Ok((ResponseKind::FindCoordinator(response), response_size))
}
