use anyhow::Error;
use anyhow::Result;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use clickhouse::Row;
use anyhow::anyhow;
use log::debug;
use log::error;
use log::info;
use ractor::RpcReplyPort;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::protocol::StrBytes;

struct ConsumerGroupsActor;

struct WaitForGeneration {
    group_id: String,
    generation: i32,
    reply: RpcReplyPort<()>,
}

struct WaitForJoin {
    group_id: String,
    member_id: String,
    reply: RpcReplyPort<()>,
}

pub struct GroupLeader {
    member_id: String,
    generation: i32,
    last_change: Instant,
}

struct State {
    clickhouse_client: Client,
    consumer_groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    wait_for_generation: Vec<WaitForGeneration>,
    wait_for_join: Vec<WaitForJoin>,
}

enum Message {
    UpdateConsumerGroupCache,
    WaitForGeneration(String, i32, RpcReplyPort<()>),
    WaitForJoin(String, String, RpcReplyPort<()>),
}

#[derive(Clone)]
pub struct GroupInfo {
    pub member_id: String,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    pub subscribed_topics: Vec<String>,
    pub join_time: DateTime<Utc>,
    pub assignments: Vec<(String, Vec<u8>)>,
    pub last_generation_change: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub members: Vec<GroupMemberRow>,
    pub response_members: Vec<JoinGroupResponseMember>,
    pub last_change: Instant,
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
            response_members: Vec::new(),
            generation: 0,
            last_change: Instant::now(),
            leader: String::new(),
            assignments: Vec::new(),
            is_converged: false,
        }
    }
}

pub struct ConsumerGroupsApi {
    clickhouse_client: Client,
    actor: ActorRef<Message>,
    cache: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
}

impl ConsumerGroupsApi {
    pub async fn new(clickhouse_client: Client) -> Self {
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let (actor, _handle) = Actor::spawn(None, ConsumerGroupsActor, (clickhouse_client.clone(), cache.clone()))
        .await
        .expect("ConsumerGroup Cache actor");
        Self { clickhouse_client, actor, cache }
    }

    pub fn get_group_info(&self, group_id: &str) -> Option<(String, i32, Instant)> {
        self.cache
            .read()
            .unwrap()
            .get(group_id)
            .map(|group| (group.leader.clone(), group.generation, group.last_change))
    }

    pub async fn wait_join(&self, group_id: String, member_id: String) {
        let _ = self.actor.call(|reply| Message::WaitForJoin(group_id, member_id, reply), Some(Duration::from_secs(10))).await;
    }

    pub async fn wait_generation(&self, group_id: String, generation: i32) -> Result<()> {
        let resp = self.actor.call(|reply| Message::WaitForGeneration(group_id, generation, reply), Some(Duration::from_secs(10))).await?;
        match resp {
            ractor::rpc::CallResult::Success(_) => Ok(()),
            ractor::rpc::CallResult::Timeout => Err(anyhow!("timeout")),
            ractor::rpc::CallResult::SenderError => Err(anyhow!("sender error")),
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
    ) -> Result<(), Error> {
        let group_id_clone = group_id.clone();
        debug!("Writing action={:?} for member={} in group={} with {} assignments", 
               action, group_info.member_id, group_id_clone, group_info.assignments.len());
               
        let mut insert = self.clickhouse_client
            .insert("kafka.group_members")?
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        // Write member action
        insert.write(&GroupMemberRow {
            group_id,
            member_id: group_info.member_id.clone(),
            action,
            generation_id,
            protocol_type: group_info.protocol_type.clone(),
            protocol: group_info.protocols.clone(),
            client_id,
            client_host,
            subscribed_topics: group_info.subscribed_topics.clone(),
            assignments: group_info.assignments.clone(), // Use assignments from GroupInfo
            join_time: group_info.join_time,
            update_time: Utc::now(),
        }).await?;

        insert.end().await?;
        debug!("Successfully wrote action for member={} in group={} with assignments", 
               group_info.member_id, group_id_clone);
        Ok(())
    }

    pub async fn write_offset_commit(
        &self,
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
        metadata: String,
        member_id: String,
        client_id: String,
        client_host: String,
    ) -> Result<(), Error> {
        let mut insert = self.clickhouse_client
            .insert("kafka.consumer_offsets")?
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");
        
        insert.write(&ConsumerOffsetRow {
            group_id,
            topic,
            partition,
            offset,
            metadata,
            member_id,
            client_id,
            client_host,
            commit_time: Utc::now(),
        }).await?;

        insert.end().await?;
        Ok(())
    }

    pub fn get_group_members(&self, group_id: &str) -> Option<Vec<GroupMemberRow>> {
        self.cache
            .read()
            .unwrap()
            .get(group_id)
            .map(|group| group.members.clone())
    }

    pub async fn wait_for_group_generation_assignments(&self, group_id: &str, generation: i32) -> Result<Vec<(String, Vec<u8>)>> {
        // Wait for the generation to converge
        self.wait_generation(group_id.to_string(), generation).await?;
        

        self.cache.read().unwrap().iter().for_each(|g| {
            info!("GROUP {}, {}", g.0, g.1.members.len());
        });

        // Get assignments from cache
        let assignments = self.cache
            .read()
            .unwrap()
            .get(group_id)
            .ok_or_else(|| anyhow!("Group not found"))?
            .assignments
            .clone();
            
        // Log the assignments for debugging
        debug!("Retrieved {} assignments for group {} generation {}", 
               assignments.len(), group_id, generation);
        for (member_id, data) in &assignments {
            debug!("Assignment for {}: {} bytes", member_id, data.len());
        }
        
        Ok(assignments)
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
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub join_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub update_time: DateTime<Utc>,
}

#[derive(Row, Serialize)]
struct ConsumerOffsetRow {
    group_id: String,
    topic: String,
    partition: i32,
    offset: i64,
    metadata: String,
    member_id: String,
    client_id: String,
    client_host: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    commit_time: DateTime<Utc>,
}

impl ConsumerGroupsActor {
    async fn handle_update_cache(state: &mut State) {
        debug!("Updating consumer group cache...");

        

        if let Err(e) = state.update_consumer_groups().await {
            error!("Failed to fetch consumer groups: {:?}", e);
            return;
        }

        let consumer_groups = state.consumer_groups.read().unwrap();

        state.wait_for_generation = state
            .wait_for_generation
            .drain(..)
            .filter_map(|w| {
                if w.reply.is_closed() {
                    return Some(w);
                }

                let group = consumer_groups.get(&w.group_id)?;
                if group.is_converged && group.generation == w.generation {
                    let _ = w.reply.send(());
                    None
                } else {
                    Some(w)
                }
            })
            .collect();

        state.wait_for_join = state
            .wait_for_join
            .drain(..)
            .filter_map(|w| {
                if w.reply.is_closed() {
                    return Some(w);
                }

                let group = consumer_groups.get(&w.group_id)?;
                if group.members.iter().any(|m| m.member_id == w.member_id) {
                    let _ = w.reply.send(());
                    None
                } else {
                    Some(w)
                }
            })
            .collect();

        debug!(
            "Updated consumer group cache with {} groups",
            consumer_groups.len()
        );
    }
}

impl Actor for ConsumerGroupsActor {
    type Msg = Message;
    // and (optionally) internal state
    type State = State;
    // Startup initialization args
    type Arguments = (Client, Arc<RwLock<HashMap<String, ConsumerGroup>>>);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

        myself.send_interval(Duration::from_secs(1), || Message::UpdateConsumerGroupCache);

        Ok(State {
            clickhouse_client: args.0,
            consumer_groups: args.1,
            wait_for_generation: Vec::new(),
            wait_for_join: Vec::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            Message::UpdateConsumerGroupCache => Self::handle_update_cache(state).await,
            Message::WaitForGeneration(group_id, generation, reply) => {
                if let Some(group) = state.consumer_groups.read().unwrap().get(&group_id) {
                    if group.is_converged && group.generation == generation {
                        let _ = reply.send(());
                        return Ok(());
                    }
                }
                state.wait_for_generation.push(WaitForGeneration {
                    group_id,
                    generation,
                    reply,
                });
            }
            Message::WaitForJoin(group_id, member_id, reply) => {
                if let Some(group) = state.consumer_groups.read().unwrap().get(&group_id) {
                    if group.members.iter().any(|m| m.member_id == member_id) {
                        let _ = reply.send(());
                        return Ok(());
                    }
                }
                state.wait_for_join.push(WaitForJoin {
                    group_id,
                    member_id,
                    reply,
                });
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

impl State {
    async fn update_consumer_groups(&mut self) -> Result<()> {
        debug!("Updating consumer group cache...");

        let query = r#"
        WITH members AS (
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
            FROM kafka.group_members
            WHERE update_time >= now64() - INTERVAL ? SECOND
            AND action != 'LeaveGroup'
            ORDER BY group_id, update_time DESC
            LIMIT 1 BY group_id, member_id
        )
        SELECT * FROM members ORDER BY group_id, join_time ASC
        "#;

        let mut cursor = self.clickhouse_client
            .query(query)
            .bind(15)
            .fetch::<GroupMemberRow>()?;

        let mut groups = HashMap::new();
        let mut current_group = ConsumerGroup::default();

        while let Some(row) = cursor.next().await? {
            // Start new group collection
            if current_group.group_id != row.group_id {
                // Complete previous group collection
                if !current_group.group_id.is_empty() {
                    current_group.set_is_converged();
                    debug!("Saved group {} with {} members, {} assignments", 
                           current_group.group_id, current_group.members.len(), 
                           current_group.assignments.len());
                    groups.insert(current_group.group_id.clone(), current_group);
                }
                // Initialize new group
                current_group = ConsumerGroup {
                    group_id: row.group_id.clone(),
                    members: Vec::new(),
                    response_members: Vec::new(),
                    generation: row.generation_id,
                    last_change: Instant::now(),
                    leader: row.member_id.clone(),
                    assignments: row.assignments.clone(),
                    is_converged: false,
                };
                
                debug!("Started new group collection for {}, leader: {}, with {} assignments", 
                       row.group_id, row.member_id, row.assignments.len());
            }

            // Add member to current group
            let join_group_member = JoinGroupResponseMember::default()
                .with_member_id(StrBytes::from_string(row.member_id.clone()))
                .with_metadata(row.protocol.first().map(|p| p.1.clone()).unwrap_or_default().into());
            current_group.response_members.push(join_group_member);
            current_group.members.push(row.clone());
        }

        if !current_group.group_id.is_empty() {
            current_group.set_is_converged();
            debug!("Saved final group {} with {} members, {} assignments", 
                   current_group.group_id, current_group.members.len(), 
                   current_group.assignments.len());
            groups.insert(current_group.group_id.clone(), current_group);
        }

        // Update the shared cache
        let mut consumer_groups = self.consumer_groups.write().unwrap();
        *consumer_groups = groups;

        // Print assignments for each group
        for (group_id, group) in consumer_groups.iter() {
            info!("Group {} assignments:", group_id);
            for (member_id, assignment) in &group.assignments {
                info!("  Member {}: {} bytes", member_id, assignment.len());
            }
        }

        debug!("Updated consumer group cache with {} groups", consumer_groups.len());
        Ok(())
    }
}
