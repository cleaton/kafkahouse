use anyhow::Result;
use chrono::{DateTime, Utc};
use clickhouse::Client;
use clickhouse::Row;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::protocol::StrBytes;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::time as tokio_time;
use serde_repr::{Deserialize_repr, Serialize_repr};

pub type TopicAssignments = Vec<(String, Vec<u8>)>;

pub struct GroupInfo {
    pub member_id: String,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
    pub subscribed_topics: Vec<String>,
}

/// Representation of a topic partition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// Assignment Map representation - maps topic name to partition map (partition ID -> member ID)
type PartitionMap = HashMap<i32, String>;
pub type GroupAssignment = HashMap<String, PartitionMap>;



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

pub struct ConsumerGroups {
    cache: Arc<RwLock<ConsumerGroupCache>>,
    clickhouse_client: Client,
}

impl ConsumerGroups {
    pub fn new(clickhouse_client: Client) -> Self {
        Self {
            cache: Arc::new(RwLock::new(ConsumerGroupCache::new())),
            clickhouse_client,
        }
    }

    /// Get the current generation ID for a group
    pub fn get_group_generation(&self, group_id: &str) -> Option<i32> {
        self.with_read_lock(|cache| cache.groups.get(group_id).map(|g| g.generation))
    }

    /// Get the leader member for a group
    pub fn get_group_leader(&self, group_id: &str) -> Option<String> {
        self.with_read_lock(|cache| cache.groups.get(group_id).map(|group| group.leader.clone()))
    }

    /// Helper method to reduce lock boilerplate
    fn with_read_lock<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&ConsumerGroupCache) -> T,
    {
        let cache = self.cache.read().unwrap();
        f(&cache)
    }

    fn group_generation_assignments(
        &self,
        group_id: &str,
        generation_id: i32,
    ) -> Option<TopicAssignments> {
        self.with_read_lock(|cache| {
            if let Some(group) = cache.groups.get(group_id) {
                if group
                    .members
                    .iter()
                    .all(|member| member.generation_id == generation_id)
                {
                    return Some(
                        group
                            .members
                            .iter()
                            .find(|m| m.member_id == group.leader)
                            .map(|l| l.assignments.clone())
                            .unwrap_or_default(),
                    );
                }
            }
            return None;
        })
    }

    fn is_member_generation(&self, group_id: &str, member_id: &str, generation_id: i32) -> bool {
        self.with_read_lock(|cache| {
            if let Some(group) = cache.groups.get(group_id) {
                group.members.iter().any(|member| {
                    member.member_id == member_id && member.generation_id == generation_id
                })
            } else {
                false
            }
        })
    }

    /// Wait for a condition to be true with timeout and return value
    async fn wait_for<F, T>(&self, timeout: Duration, mut check_fn: F, on_timeout: T) -> Result<T>
    where
        F: FnMut() -> Option<T>,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if let Some(value) = check_fn() {
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(on_timeout)
    }

    /// Wait for member to reach generation with timeout  
    pub async fn wait_for_member_generation(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        timeout: Duration,
    ) -> Result<bool> {
        self.wait_for(
            timeout,
            || {
                Some(self.is_member_generation(group_id, member_id, generation_id))
            },
            false,
        )
        .await
    }

    pub fn group_count(&self) -> usize {
        self.with_read_lock(|cache| cache.groups.len())
    }

    fn get_group(&self, group_id: &str) -> Option<ConsumerGroup> {
        self.with_read_lock(|cache| cache.groups.get(group_id).cloned())
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
            assignments: Vec::new(), // Empty assignments initially
            join_time: now,
            update_time: now,
        };

        // Clickhouse insert - handle the result differently
        let mut insert = self.clickhouse_client.insert("group_members")?;
        insert.write(&row).await?;
        insert.end().await?;

        Ok(())
    }

    pub fn get_members(&self, group_id: &str) -> Vec<JoinGroupResponseMember> {
        self.with_read_lock(|cache| {
            cache
                .groups
                .get(group_id)
                .map(|group| group.response_members.clone())
                .unwrap_or_default()
        })
    }

    pub async fn start_worker(&self) -> Result<()> {
        let cache_clone = self.cache.clone();
        let client_clone = self.clickhouse_client.clone();

        tokio::spawn(async move {
            if let Err(e) = run_cache_worker(cache_clone, client_clone).await {
                error!("Consumer group cache worker error: {:?}", e);
            }
        });

        info!("Started consumer group cache worker");
        Ok(())
    }

    pub async fn wait_for_group_generation_assignments(&self, group_id: &str, generation_id: i32) -> Result<TopicAssignments> {
        let timeout = Duration::from_secs(10);
        self.wait_for(
            timeout,
            || self.group_generation_assignments(group_id, generation_id),
            Vec::new(),
        )
        .await
    }

    fn with_write_lock<F>(&self, f: F)
    where
        F: FnOnce(&mut ConsumerGroupCache),
    {
        let mut cache = self.cache.write().unwrap();
        f(&mut cache)
    }
}

async fn run_cache_worker(cache: Arc<RwLock<ConsumerGroupCache>>, client: Client) -> Result<()> {
    let cache_update_interval = Duration::from_secs(5);
    let mut interval = tokio_time::interval(cache_update_interval);

    loop {
        interval.tick().await;

    }
} 