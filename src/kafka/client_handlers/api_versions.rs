use kafka_protocol::messages::*;
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::protocol::Encodable;
use crate::kafka::client_actor::ClientState;
use log::debug;

pub(crate) fn handle_api_versions(client: &ClientState, _request: &ApiVersionsRequest, api_version: i16) -> Result<(ResponseKind, i32), anyhow::Error> {

    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    response.api_keys = client.supported_api_versions.to_vec();
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
    Ok((ResponseKind::ApiVersions(response), response_size))
} 