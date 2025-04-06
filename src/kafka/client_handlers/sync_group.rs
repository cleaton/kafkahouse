use anyhow::anyhow;
use anyhow::Result;
use bytes::Bytes;
use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::messages::sync_group_response::SyncGroupResponse;
use kafka_protocol::protocol::Encodable;
use kafka_protocol::protocol::StrBytes;
use log::{debug, info};
use std::collections::{BTreeMap, HashMap, HashSet};
use chrono::{DateTime, Utc};

use crate::kafka::client::KafkaClient;
use crate::kafka::client_types::*;


pub(crate) async fn handle_sync_group(
    client: &mut KafkaClient,
    request: &SyncGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    let group_id = request.group_id.to_string();
    let member_id = request.member_id.to_string();
    let generation_id = request.generation_id;
    let assignments: TopicAssignments = request.assignments.iter().map(|a| (a.member_id.to_string(), a.assignment.to_vec())).collect();
    
    info!("Handling SyncGroup request: group_id={}, member_id={}, generation_id={}",
          group_id, member_id, generation_id);

    let group_info = client.joined_groups.get_mut(&group_id).map(|g| Ok(g)).unwrap_or(Err(anyhow!("missing group")))?;
    group_info.assignments = assignments;

    
    client.consumer_group_cache.write_action(MemberAction::SyncGroup, client.client_id.clone(),client.client_host.clone(), group_id.clone(), generation_id, group_info).await?;
    
    let assignments = client.consumer_group_cache.wait_for_group_generation_assignments(&group_id, generation_id).await?;

    let my_assignments = assignments.iter().find(|k| k.0 == member_id).map(|a| Bytes::from(a.1.clone())).unwrap_or_default();

    let assignent_len = my_assignments.len();
    let response = SyncGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_protocol_type(Some(StrBytes::from_string("consumer".to_string())))
        .with_protocol_name(Some(StrBytes::from_string("range".to_string())))
        .with_assignment(my_assignments)
        .with_unknown_tagged_fields(BTreeMap::new());
    
    debug!("SyncGroup response sent to {} with assignment of size {} bytes", member_id, assignent_len);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::SyncGroup(response), response_size))
}