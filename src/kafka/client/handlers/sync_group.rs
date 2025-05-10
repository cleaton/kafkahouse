use anyhow::Result;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Encodable, StrBytes};
use crate::kafka::client::types::ClientState;
use crate::kafka::consumer::actor::MemberAction;
use log::{debug, info};
use std::collections::BTreeMap;
use bytes::Bytes;

use crate::kafka::protocol::{KafkaRequestMessage, KafkaResponseMessage};

pub(crate) async fn handle_sync_group(state: &mut ClientState, request: KafkaRequestMessage) 
    -> Result<KafkaResponseMessage, anyhow::Error> {
    
    let api_version = request.header.request_api_version;
    
    // Extract the SyncGroupRequest from the request
    let typed_request = if let RequestKind::SyncGroup(req) = request.request {
        req
    } else {
        return Err(anyhow::anyhow!("Expected SyncGroup request"));
    };
    
    let group_id = typed_request.group_id.to_string();
    let member_id = typed_request.member_id.to_string();
    let generation_id = typed_request.generation_id;
    let assignments = typed_request.assignments
        .iter()
        .map(|a| (a.member_id.to_string(), a.assignment.to_vec()))
        .collect();
    
    info!("Handling SyncGroup request: group_id={}, member_id={}, generation_id={}",
          group_id, member_id, generation_id);

    let mut group_info = state.active_groups.get(&group_id)
        .ok_or_else(|| anyhow::anyhow!("Group not found"))?
        .clone();
    group_info.assignments = assignments;
    state.active_groups.insert(group_id.clone(), group_info.clone());
    
    state.consumer_groups_api.write_action(
        MemberAction::SyncGroup,
        state.client_id.clone(),
        state.client_host.clone(),
        group_id.clone(),
        generation_id,
        &group_info,
    ).await?;
    
    let assignments = state.consumer_groups_api
        .wait_for_group_generation_assignments(&group_id, generation_id)
        .await?;

    info!("Assignments for group {}: {:?}", group_id, assignments.iter().map(|(id, _)| id).collect::<Vec<_>>());
    let my_assignment = assignments
        .iter()
        .find(|(id, _)| id == &member_id)
        .map(|(_, data)| Bytes::from(data.clone()))
        .unwrap_or_default();

    let assignment_len = my_assignment.len();
    let response = SyncGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_protocol_type(Some(StrBytes::from_string("consumer".to_string())))
        .with_protocol_name(Some(StrBytes::from_string("range".to_string())))
        .with_assignment(my_assignment)
        .with_unknown_tagged_fields(BTreeMap::new());
    
    info!("SyncGroup response sent to {} with assignment of size {} bytes", member_id, assignment_len);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok(KafkaResponseMessage {
        request_header: request.header,
        api_key: request.api_key,
        response: ResponseKind::SyncGroup(response),
        response_size,
    })
}
