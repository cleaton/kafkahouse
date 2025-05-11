use anyhow::Result;
use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    messages::{
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        RequestKind, ResponseKind, TopicName,
    },
    protocol::{Encodable, StrBytes},
    records::{Record, RecordBatchEncoder, RecordEncodeOptions, Compression, TimestampType},
};
use log::{debug, error};
use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::sleep;
use indexmap::IndexMap;

use crate::kafka::{
    broker::types::{TopicPartition, TopicPartitions},
    client::types::ClientState,
    protocol::{KafkaRequestMessage, KafkaResponseMessage},
};

pub async fn handle_fetch(
    state: &mut ClientState,
    fetch_req: KafkaRequestMessage,
) -> Result<KafkaResponseMessage> {
    let api_version = fetch_req.header.request_api_version;

    // Extract the FetchRequest from the request
    let request = if let RequestKind::Fetch(req) = fetch_req.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected Fetch request"));
    };

    debug!(
        "Handling Fetch request: max_bytes={}, min_bytes={}, max_wait_ms={}",
        request.max_bytes, request.min_bytes, request.max_wait_ms
    );

    // Create a basic response structure
    let mut response = FetchResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0);

    // Group partitions by topic
    let mut topics_map: HashMap<String, Vec<TopicPartition>> = HashMap::new();

    for topic in &request.topics {
        let topic_name = topic.topic.to_string();
        for partition in &topic.partitions {
            topics_map
                .entry(topic_name.clone())
                .or_insert_with(Vec::new)
                .push(TopicPartition {
                    partition_id: partition.partition,
                    offset: partition.fetch_offset,
                });
        }
    }

    // Convert to TopicPartitions vector
    let topic_partitions: Vec<TopicPartitions> = topics_map
        .into_iter()
        .map(|(topic, partitions)| TopicPartitions {
            topic,
            partitions,
        })
        .collect();

    // If we have topics to fetch, query ClickHouse with timeout
    if !topic_partitions.is_empty() {
        // Set up timeout based on max_wait_ms
        let max_wait = Duration::from_millis(request.max_wait_ms as u64);
        let start_time = Instant::now();
        let mut records = Vec::new();
        let mut retry_delay = Duration::from_millis(10); // Start with 10ms delay

        // Keep trying until we get records or timeout
        while start_time.elapsed() < max_wait {
            records = match state.broker.fetch_records(&topic_partitions).await {
                Ok(r) => r,
                Err(e) => {
                    error!("Error fetching records: {}", e);
                    return Ok(KafkaResponseMessage {
                        request_header: fetch_req.header,
                        api_key: fetch_req.api_key,
                        response: ResponseKind::Fetch(
                            FetchResponse::default()
                                .with_error_code(15) // GROUP_COORDINATOR_NOT_AVAILABLE
                                .with_throttle_time_ms(0),
                        ),
                        response_size: 0,
                    });
                }
            };

            // If we got records or min_bytes is 0, we can return immediately
            if !records.is_empty() || request.min_bytes == 0 {
                break;
            }

            // If we've waited long enough, break
            let remaining = max_wait
                .checked_sub(start_time.elapsed())
                .unwrap_or(Duration::from_millis(0));
            if remaining < retry_delay {
                sleep(remaining).await;
                break;
            }

            // Wait before retrying with exponential backoff
            sleep(retry_delay).await;
            retry_delay = std::cmp::min(retry_delay * 2, Duration::from_millis(100)); // Cap at 100ms
        }

        // Set throttle time to actual elapsed time
        let elapsed_ms = start_time.elapsed().as_millis() as i32;
        response = response.with_throttle_time_ms(elapsed_ms);

        // Group records by topic and partition
        let mut grouped_records = HashMap::new();

        for (topic, partition, key, value, offset) in records {
            grouped_records
                .entry(topic)
                .or_insert_with(HashMap::new)
                .entry(partition)
                .or_insert_with(Vec::new)
                .push((key, value, offset));
        }

        // Build response for each topic
        let mut topic_responses = Vec::new();
        for topic in &request.topics {
            let topic_name = topic.topic.to_string();
            let mut partition_responses = Vec::new();

            for partition in &topic.partitions {
                let partition_id = partition.partition;
                let fetch_offset = partition.fetch_offset;

                // Get records for this topic and partition
                let records = grouped_records
                    .get(&topic_name)
                    .and_then(|partitions| partitions.get(&partition_id))
                    .cloned()
                    .unwrap_or_default();

                // Create record batch if we have records
                let record_bytes = if !records.is_empty() {
                    // Get the current timestamp
                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;

                    // Create records
                    let kafka_records: Vec<Record> = records
                        .iter()
                        .map(|(key, value, offset)| {
                            // Create empty headers map
                            let headers = IndexMap::new();

                            Record {
                                transactional: false,
                                control: false,
                                partition_leader_epoch: 0,
                                producer_id: -1, // -1 indicates no producer ID
                                producer_epoch: 0,
                                timestamp_type: TimestampType::Creation,
                                offset: *offset,
                                sequence: 0,
                                timestamp: current_time,
                                key: Some(Bytes::from(key.clone())),
                                value: Some(Bytes::from(value.clone())),
                                headers,
                            }
                        })
                        .collect();

                    // Encode the records into a batch
                    let mut buf = BytesMut::new();
                    let options = RecordEncodeOptions {
                        compression: Compression::None,
                        version: 2, // Use version 2 for modern Kafka
                    };

                    // Use the RecordBatchEncoder to encode the records
                    RecordBatchEncoder::encode::<_, _, fn(&mut BytesMut, &mut BytesMut, Compression) -> Result<()>>(
                        &mut buf,
                        &kafka_records,
                        &options,
                    )?;

                    Some(buf.freeze())
                } else {
                    None
                };

                // Create partition response
                let mut partition_data = PartitionData::default()
                    .with_partition_index(partition_id)
                    .with_error_code(0)
                    .with_high_watermark(fetch_offset + 1) // Simple watermark implementation
                    .with_last_stable_offset(fetch_offset + 1)
                    .with_log_start_offset(0);

                // Add records if we have any
                if let Some(batch) = record_bytes {
                    partition_data = partition_data.with_records(Some(batch));
                }

                partition_responses.push(partition_data);
            }

            // Add topic response
            let topic_response = FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from(topic.topic.to_string())))
                .with_partitions(partition_responses);

            topic_responses.push(topic_response);
        }

        // Set topics in response
        response = response.with_responses(topic_responses);
    }

    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;

    // Log the number of records returned
    let mut total_records = 0;
    let mut elapsed_ms = if !topic_partitions.is_empty() {
        response.throttle_time_ms
    } else {
        0
    };

    // Count records and check if we're returning any data
    let mut has_data = false;
    for topic_response in &response.responses {
        for partition in &topic_response.partitions {
            if let Some(records_bytes) = &partition.records {
                // We don't have an easy way to count records in the encoded bytes,
                // so we'll estimate based on size
                let record_count = records_bytes.len() / 100; // Rough estimate
                total_records += record_count;
                
                if record_count > 0 {
                    has_data = true;
                }
            }
        }
    }
    
    // If we have no data to return, set a minimum throttle time of 1000ms
    // This helps reduce polling frequency from clients when there's no data
    if !has_data && elapsed_ms < 1000 {
        elapsed_ms = 1000;
        response = response.with_throttle_time_ms(elapsed_ms);
        debug!("No data returned, setting minimum throttle time to 1000ms");
    }

    debug!(
        "Fetch response returning approximately {} records in {} bytes (waited {}ms)",
        total_records, response_size, elapsed_ms
    );

    Ok(KafkaResponseMessage {
        request_header: fetch_req.header,
        api_key: fetch_req.api_key,
        response: ResponseKind::Fetch(response),
        response_size,
    })
}
