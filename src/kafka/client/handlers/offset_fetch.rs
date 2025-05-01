use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::types::ClientState;
use log::debug;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_offset_fetch(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the OffsetFetchRequest from the request
    let typed_request = if let RequestKind::OffsetFetch(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected OffsetFetch request"));
    };
    
    debug!("Handling OffsetFetch request: ${:?}", typed_request);
    
    // TODO: Implement the handler
    // This is a placeholder for the implementation
    let response = OffsetFetchResponse::default();
    
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::OffsetFetch(response),
        response_size
    })
}
