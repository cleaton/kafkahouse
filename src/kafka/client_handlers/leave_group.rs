use anyhow::{anyhow, Result};
use kafka_protocol::messages::*;
use kafka_protocol::messages::leave_group_response::LeaveGroupResponse;
use kafka_protocol::protocol::Encodable;
use log::{debug, info};

use crate::kafka::{client::KafkaClient, client_types::MemberAction};

pub(crate) async fn handle_leave_group(
    client: &mut KafkaClient,
    request: &LeaveGroupRequest,
    api_version: i16,
) -> Result<(ResponseKind, i32), anyhow::Error> {
    let group_id = request.group_id.to_string();
    info!("Handling LeaveGroup request: group_id={}", group_id);
    
    let mut error_code = 0;

    let group_info = client.joined_groups.get_mut(&group_id).map(|g| Ok(g)).unwrap_or(Err(anyhow!("missing group")))?;

    //for member in &request.members {
    //    let member_id = member.member_id.to_string();    
    //} TODO: Multiple Member ids???

    client.consumer_group_cache.write_action(MemberAction::LeaveGroup, client.client_id.clone(),client.client_host.clone(), group_id.clone(), -1, group_info);
    
    // Create response
    let response = LeaveGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error_code);
    
    // Compute response size
    let response_size = response.compute_size(api_version)? as i32;
    
    Ok((ResponseKind::LeaveGroup(response), response_size))
} 