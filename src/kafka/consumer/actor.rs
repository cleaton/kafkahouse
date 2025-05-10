use anyhow::Error;
use anyhow::Result;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use clickhouse::Row;
use anyhow::anyhow;
use log::debug;
use log::error;
use ractor::RpcReplyPort;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

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
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub members: Vec<GroupMemberRow>,
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
            assignments: Vec::new(), // Empty for now, will be updated in sync
            join_time: Utc::now(),
            update_time: Utc::now(),
        }).await?;

        insert.end().await?;
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
        
        // Get assignments from cache
        let assignments = self.cache
            .read()
            .unwrap()
            .get(group_id)
            .ok_or_else(|| anyhow!("Group not found"))?
            .assignments
            .clone();
            
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
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub join_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
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
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
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
            FROM group_members WHERE update_time >= now() - interval 30 second
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

        let rows: Vec<GroupMemberRow> = self.clickhouse_client.query(query).fetch_all().await?;
        let mut groups: HashMap<String, Vec<GroupMemberRow>> = HashMap::new();
        for row in rows {
            groups.entry(row.group_id.clone()).or_default().push(row);
        }
        let mut consumer_groups = self.consumer_groups.write().unwrap();
        consumer_groups
            .retain(|group_id, _| groups.contains_key(group_id));
        for (group_id, members) in groups {
            let current_group = consumer_groups.entry(group_id.clone()).or_default();

            // Check if any member differs at same index
            let has_changes = current_group.members.len() != members.len()
                || current_group
                    .members
                    .iter()
                    .zip(members.iter())
                    .any(|(current, new)| current.member_id != new.member_id);

            if has_changes {
                current_group.last_change = Instant::now();
            }
            current_group.members = members;
            current_group.set_is_converged();
        }
        Ok(())
    }
}
