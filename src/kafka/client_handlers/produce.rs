use anyhow;
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::messages::ResponseKind;
use kafka_protocol::protocol::Encodable;
use log::info;

use crate::kafka::client_actor::ClientState;
pub(crate) async fn handle_produce(
    client: &mut ClientState,
    request: &ProduceRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    info!("Handling Produce request: {:?}", request);

    // Use the broker to handle the actual produce operation
    let produce_response = client.broker.produce(request.clone()).await?;
    
    // The response is already in the Kafka protocol format
    let size = produce_response.compute_size(api_version)?;
    return Ok((ResponseKind::Produce(produce_response), size as i32));
} 