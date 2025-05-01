use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use log::debug;

use crate::kafka::protocol::KafkaRequestMessage;
use crate::kafka::protocol::KafkaResponseMessage;

pub(crate) async fn handle_api_versions(state: &mut ClientState, request: KafkaRequestMessage) -> Result<KafkaResponseMessage, anyhow::Error> {
    let api_version = request.header.request_api_version;
    
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.api_keys = state.supported_api_versions.to_vec();
    response.throttle_time_ms = 0;
    
    // Additional fields for v3+
    if api_version >= 3 {
        response.finalized_features_epoch = 0;
        response.finalized_features = vec![];
        response.supported_features = vec![];
        response.zk_migration_ready = false;
    }

    let response_size = response.compute_size(api_version)? as i32;
    debug!("ApiVersionResponse: {:?}", response);  // Debug the response
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::ApiVersions(response),
        response_size
    })
} 