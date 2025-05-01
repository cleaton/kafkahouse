use clickhouse::Row;
use clickhouse::Client;
use kafka_protocol::protocol::StrBytes;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use chrono::{DateTime, Utc};
use anyhow::Result;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};


struct ConsumerGroupsActor;

struct State {
    clickhouse_client: Client
}

enum Message {
    UpdateConsumerGroupCache
}

pub struct GroupInfo {
    pub member_id: String,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    pub subscribed_topics: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub members: Vec<GroupMemberRow>,
    pub response_members: Vec<JoinGroupResponseMember>,
    pub generation: i32,
    pub leader: String,
    pub assignments: TopicAssignments,
    pub is_converged: bool,
}

impl ConsumerGroup {
    pub fn set_is_converged(&mut self) {
        let is_converged = self
            .members
            .iter()
            .all(|member| member.generation_id == self.generation);
        self.is_converged = is_converged;
    }
}

impl Default for ConsumerGroup {
    fn default() -> Self {
        ConsumerGroup {
            group_id: String::new(),
            members: Vec::new(),
            generation: 0,
            leader: String::new(),
            assignments: Vec::new(),
            response_members: Vec::new(),
            is_converged: false,
        }
    }
}

#[derive(Debug, Clone)]
struct ConsumerGroupCache {
    groups: HashMap<String, ConsumerGroup>,
    last_updated: Instant,
}

impl Default for ConsumerGroupCache {
    fn default() -> Self {
        Self {
            groups: HashMap::new(),
            last_updated: Instant::now(),
        }
    }
}

impl ConsumerGroupCache {
    fn new() -> Self {
        Self::default()
    }

    fn update(&mut self, new_groups: HashMap<String, ConsumerGroup>) {
        self.groups = new_groups;
        self.last_updated = Instant::now();
    }
}

struct ConsumerGroups {
    clickhouse_client: Client,
    cache: Arc<RwLock<ConsumerGroupCache>>
}


impl ConsumerGroups {
    pub fn new(clickhouse_client: Client) -> Self {
        Self {
            clickhouse_client,
            cache: ConsumerGroupCache::default()
        }
    }
    pub async fn write_action(
        &self,
        action: MemberAction,
        client_id: String,
        client_host: String,
        group_id: String,
        generation_id: i32,
        group_info: &GroupInfo,
    ) -> Result<()> {
        let now = Utc::now();
        let row = GroupMemberRow {
            group_id: group_id.clone(),
            member_id: group_info.member_id.clone(),
            action,
            generation_id,
            protocol_type: group_info.protocol_type.clone(),
            protocol: group_info.protocols.clone(),
            client_id,
            client_host,
            subscribed_topics: group_info.subscribed_topics.clone(),
            assignments: Vec::new(),
            join_time: now,
            update_time: now,
        };

        let mut insert = self.clickhouse_client.insert("group_members")?;
        insert.write(&row).await?;
        insert.end().await?;

        Ok(())
    }
}

pub type TopicAssignments = Vec<(String, Vec<u8>)>;

#[derive(Debug, Clone, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum MemberAction {
    JoinGroup = 1,
    SyncGroup = 2,
    Heartbeat = 3,
    LeaveGroup = 4,
}

/// Row representation for the ClickHouse group_members table
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct GroupMemberRow {
    pub group_id: String,
    pub member_id: String,
    pub action: MemberAction,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol: Vec<(String, Vec<u8>)>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topics: Vec<String>,
    pub assignments: TopicAssignments,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub join_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub update_time: DateTime<Utc>,
}


impl Actor for ConsumerGroupsActor {
    type Msg = Message;
    // and (optionally) internal state
    type State = State;
    // Startup initialization args
    type Arguments = Client;
    
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(State { clickhouse_client: args })
    }

    async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            message: Self::Msg,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match message {
                Message::UpdateConsumerGroupCache => {
                    debug!("Updating consumer group cache...");
                    match fetch_consumer_groups(&client).await {
                        Ok(groups) => {
                            let count = groups.len();
                            let mut cache = cache.write().unwrap();
                            cache.update(groups);
                            debug!("Updated consumer group cache with {} groups", count);
                        }
                        Err(e) => {
                            error!("Failed to fetch consumer groups: {:?}", e);
                        }
                    }
                }
            }
        Ok(())
    }

    async fn post_stop(
            &self,
            _myself: ActorRef<Self::Msg>,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}




async fn fetch_consumer_groups(client: &Client) -> Result<HashMap<String, ConsumerGroup>> {
    let query = r#"
        WITH latest_actions AS (
            SELECT
                group_id,
                member_id,
                argMax(action, update_time) as action,
                argMax(generation_id, update_time) as generation_id,
                argMax(protocol_type, update_time) as protocol_type,
                argMax(protocol, update_time) as protocol,
                argMax(client_id, update_time) as client_id,
                argMax(client_host, update_time) as client_host,
                argMax(subscribed_topics, update_time) as subscribed_topics,
                argMax(assignments, update_time) as assignments,
                min(join_time) as join_time,
                max(update_time) as update_time
            FROM group_members
            GROUP BY group_id, member_id
        )
        SELECT
            group_id,
            member_id,
            action,
            generation_id,
            protocol_type,
            protocol,
            client_id,
            client_host,
            subscribed_topics,
            assignments,
            join_time,
            update_time
        FROM latest_actions
        WHERE action != 'LeaveGroup'
        ORDER BY group_id, join_time
    "#;

    let rows: Vec<GroupMemberRow> = client.query(query).fetch_all().await?;

    let mut groups = HashMap::new();
    for row in rows {
        let group_id = row.group_id.clone();
        let group = groups.entry(group_id).or_insert_with(ConsumerGroup::default);
        
        // Set group info from first member (or highest gen)
        if group.generation < row.generation_id || group.group_id.is_empty() {
            group.group_id = row.group_id.clone();
            group.generation = row.generation_id;
        }
        
        // Add member
        group.members.push(row.clone());
        
        // Find leader - the one with the earliest join time in the current generation
        if row.generation_id == group.generation {
            if group.leader.is_empty() || row.join_time < 
                group.members.iter()
                    .find(|m| m.member_id == group.leader)
                    .map(|m| m.join_time)
                    .unwrap_or_else(|| Utc::now()) {
                group.leader = row.member_id.clone();
            }
        }
        
        // Create join group response member format
        let response_member = JoinGroupResponseMember::default()
            .with_member_id(StrBytes::from_string(row.member_id.clone()))
            .with_metadata(Vec::new().into()); // Empty metadata for now
            
        group.response_members.push(response_member);
    }
    
    // Process all groups to set assignments and converged status
    for group in groups.values_mut() {
        if let Some(leader_member) = group.members.iter().find(|m| m.member_id == group.leader) {
            group.assignments = leader_member.assignments.clone();
        }
        
        group.set_is_converged();
    }

    Ok(groups)
}