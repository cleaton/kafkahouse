use anyhow::{anyhow, Result};
use kafka_protocol::messages::*;
use kafka_protocol::messages::heartbeat_response::HeartbeatResponse;
use kafka_protocol::protocol::Encodable;
use log::{debug, info};
use std::collections::HashSet;

use crate::kafka::{client::KafkaClient, client_types::MemberAction};

pub(crate) async fn handle_heartbeat(
    client: &mut KafkaClient,
    request: &HeartbeatRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    let group_id = request.group_id.to_string();
    let member_id = request.member_id.to_string();
    let generation_id = request.generation_id;
    
    info!("Handling Heartbeat request: group_id={}, member_id={}, generation_id={}",
          group_id, member_id, generation_id);
    
    // Check if the member's generation matches the group's current generation
    let current_generation = client.consumer_group_cache.get_group_generation(&group_id).unwrap_or(1);
    let leader = client.consumer_group_cache.get_group_leader(&group_id).unwrap_or_else(|| member_id.clone());

    let group_info = client.joined_groups.get_mut(&group_id).map(|g| Ok(g)).unwrap_or(Err(anyhow!("missing group")))?;

    client.consumer_group_cache.write_action(MemberAction::Heartbeat, client.client_id.clone(),client.client_host.clone(), group_id.clone(), generation_id, group_info).await?;
    
    // Determine error code based on whether member should rejoin
    let mut error_code = if current_generation != generation_id {
        debug!("Member {} has outdated generation {} (current is {}), requesting rejoin",
               member_id, generation_id, current_generation);
        27 // REBALANCE_IN_PROGRESS
    } else {
        0 // SUCCESS
    };


    if leader == member_id {
        if let Some(group_info) = client.joined_groups.get(&group_id) {
            let members = client.consumer_group_cache.get_members(&group_id);
            info!("Current members in group {}: {:?}", group_id, members);
            info!("in group_info {}: {:?}", group_id, group_info.members);
            if group_info.should_rebalance(&members) {
                info!("!!!!!! SHOULD REBALANCE !!!!!!!");
                error_code = 27;
            }
        }
    }
    
    // Create response
    let response = HeartbeatResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::Heartbeat(response), response_size))
} 