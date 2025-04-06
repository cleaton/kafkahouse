use std::collections::HashMap;

use chrono::{DateTime, Utc};
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use serde_repr::{Deserialize_repr, Serialize_repr};

type Protocol = (String, Vec<u8>);
pub struct MemberInfo {
    pub join_time: DateTime<Utc>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub subscribed_topics: Vec<String>,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocol: Vec<Protocol>,
}

pub type TopicAssignments = Vec<(String, Vec<u8>)>;

pub struct GroupInfo {
    pub member_info: MemberInfo,
    pub members: Vec<JoinGroupResponseMember>,
    pub assignments: TopicAssignments,
}

impl GroupInfo {
    pub fn should_rebalance(&self, others: &Vec<JoinGroupResponseMember>) -> bool {
        // Compare current members with group_info members to detect changes
        let current_members: Vec<String> = others.iter().map(|m| m.member_id.to_string()).collect();
        let existing_members: Vec<String> = self
            .members
            .iter()
            .map(|m| m.member_id.to_string())
            .collect();
        return current_members != existing_members;
    }
}

pub type JoinedGroups = HashMap<String, GroupInfo>;

#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum MemberAction {
    JoinGroup = 1,
    SyncGroup = 2,
    Heartbeat = 3,
    LeaveGroup = 4,
}
