use kafka_protocol::messages::*;
use kafka_protocol::messages::list_offsets_response::{ListOffsetsResponse, ListOffsetsTopicResponse, ListOffsetsPartitionResponse};
use kafka_protocol::protocol::Encodable;
use crate::kafka::client::KafkaClient;
use log::debug;

pub(crate) async fn handle_list_offsets(
    client: &mut KafkaClient,
    request: &ListOffsetsRequest,
    api_version: i16
) -> Result<(ResponseKind, i32), anyhow::Error> {
    debug!("Handling ListOffsets request: {:?}", request);
    
    let mut response = ListOffsetsResponse::default()
        .with_throttle_time_ms(0);
    
    let mut topic_responses = Vec::new();
    
    for topic in &request.topics {
        let topic_name = topic.name.0.to_string();
        let mut partition_responses = Vec::new();
        
        for partition in &topic.partitions {
            let partition_id = partition.partition_index;
            
            // Get the earliest or latest offset based on the timestamp
            let offset = match partition.timestamp {
                // -1 means latest offset
                -1 => {
                    // Query ClickHouse for the latest offset
                    let latest_offset = client.broker.get_latest_offset(&topic_name, partition_id).await?;
                    latest_offset
                },
                // -2 means earliest offset
                -2 => {
                    // Query ClickHouse for the earliest offset
                    let earliest_offset = client.broker.get_earliest_offset(&topic_name, partition_id).await?;
                    earliest_offset
                },
                // Specific timestamp
                ts => {
                    // Query ClickHouse for the offset at this timestamp
                    let offset_at_time = client.broker.get_offset_at_time(&topic_name, partition_id, ts).await?;
                    offset_at_time
                }
            };
            
            let partition_response = ListOffsetsPartitionResponse::default()
                .with_partition_index(partition_id)
                .with_error_code(0)
                .with_offset(offset)
                .with_timestamp(partition.timestamp);
            
            if api_version >= 4 {
                // For newer versions, include leader epoch
                // partition_response = partition_response.with_leader_epoch(0);
            }
            
            partition_responses.push(partition_response);
        }
        
        let topic_response = ListOffsetsTopicResponse::default()
            .with_name(topic.name.clone())
            .with_partitions(partition_responses);
        
        topic_responses.push(topic_response);
    }
    
    response.topics = topic_responses;
    
    let response_size = response.compute_size(api_version)? as i32;
    debug!("ListOffsets response size: {} bytes", response_size);
    
    Ok((ResponseKind::ListOffsets(response), response_size))
}
