use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::Encodable;
use log::debug;

use crate::kafka::client::types::ClientState;
use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_produce(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the ProduceRequest from the request
    let produce_request = if let RequestKind::Produce(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected Produce request"));
    };
    
    debug!("Handling Produce request: {:?}", produce_request);

    // Use the broker to handle the actual produce operation
    let produce_response = state.broker.produce(produce_request).await?;
    
    // The response is already in the Kafka protocol format
    let response_size = produce_response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::Produce(produce_response),
        response_size
    })
} 