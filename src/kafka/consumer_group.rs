use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clickhouse::Client;
use clickhouse::Row;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::protocol::StrBytes;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::time as tokio_time;

use super::client_types::{GroupInfo, MemberAction, TopicAssignments};

/// Representation of a topic partition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// Assignment Map representation - maps topic name to partition map (partition ID -> member ID)
type PartitionMap = HashMap<i32, String>;
pub type GroupAssignment = HashMap<String, PartitionMap>;

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
    // Using DateTime64(9) with nanosecond precision
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub join_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub update_time: DateTime<Utc>,
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
                if self.is_member_generation(group_id, member_id, generation_id) {
                    Some(true)
                } else {
                    None
                }
            },
            false, // Return false on timeout
        )
        .await
    }

    /// Get the number of consumer groups in the cache
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
        let row = GroupMemberRow {
            group_id,
            member_id: group_info.member_info.member_id.clone(),
            action,
            generation_id: generation_id,
            protocol_type: group_info.member_info.protocol_type.clone(),
            protocol: group_info.member_info.protocol.clone(),
            client_id: client_id,
            client_host: client_host,
            subscribed_topics: group_info.member_info.subscribed_topics.clone(),
            assignments: group_info.assignments.clone(),
            join_time: group_info.member_info.join_time,
            update_time: Utc::now(),
        };

        let mut insert = self.clickhouse_client.insert("group_members")?;
        insert.write(&row).await?;
        insert.end().await?;

        Ok(())
    }

    pub fn get_members(&self, group_id: &str) -> Vec<JoinGroupResponseMember> {
        return self.with_read_lock(|cache| {
            cache
                .groups
                .get(group_id)
                .map(|g| g.response_members.clone())
                .unwrap_or_default()
        });
    }

    /// Start the background worker that updates the cache
    pub async fn start_worker(&self) -> Result<()> {
        let cache = self.cache.clone();
        let client = self.clickhouse_client.clone();

        tokio::spawn(async move {
            if let Err(e) = run_cache_worker(cache, client).await {
                error!("Cache worker failed: {}", e);
            }
        });

        info!("Started consumer group cache worker");
        Ok(())
    }

    /// Wait for all members of a group to reach a specific generation ID
    pub async fn wait_for_group_generation_assignments(&self, group_id: &str, generation_id: i32) -> Result<TopicAssignments> {
        let start_time = Instant::now();
        let timeout = Duration::from_secs(10);

        loop {
            if let Some(assignments) = self.group_generation_assignments(group_id, generation_id) {
                return Ok(assignments);
            }

            // Check timeout
            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for group {} to reach generation {}",
                    group_id,
                    generation_id
                ));
            }

            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// Background worker that updates the cache
async fn run_cache_worker(cache: Arc<RwLock<ConsumerGroupCache>>, client: Client) -> Result<()> {
    let mut interval = tokio_time::interval(Duration::from_millis(500));

    loop {
        interval.tick().await;
        match fetch_consumer_groups(&client).await {
            Ok(groups) => {
                debug!("Fetched {} consumer groups", groups.len());
                let mut cache = cache.write().unwrap();
                cache.update(groups);
            }
            Err(e) => {
                error!("Failed to fetch consumer groups: {}", e);
            }
        }
    }
}

/// Fetch consumer groups from ClickHouse
async fn fetch_consumer_groups(client: &Client) -> Result<HashMap<String, ConsumerGroup>> {
    let query = "
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
        FROM group_members
        WHERE update_time >= now64() - INTERVAL ? SECOND
        AND action != 'LeaveGroup'
        ORDER BY group_id, update_time DESC
        LIMIT 1 BY group_id, member_id
        )
        SELECT * FROM members ORDER BY group_id, join_time ASC
    ";

    let mut cursor = client
        .query(query)
        .bind(15)
        .fetch::<GroupMemberRow>()
        .context("Failed to fetch consumer groups")?;

    let mut groups = HashMap::new();
    let mut current_group = ConsumerGroup::default();

    while let Some(row) = cursor.next().await? {
        // Start new group collection
        if current_group.group_id != row.group_id {
            // Complete previous group collection
            if !current_group.group_id.is_empty() {
                current_group.set_is_converged();
                groups.insert(current_group.group_id.clone(), current_group);
            }
            // Initialize new group
            current_group = ConsumerGroup {
                group_id: row.group_id.clone(),
                members: Vec::new(),
                response_members: Vec::new(),
                generation: row.generation_id,
                leader: row.member_id.clone(),
                assignments: row.assignments.clone(),
                is_converged: false,
            };
        }
        let join_group_member = JoinGroupResponseMember::default()
            .with_member_id(StrBytes::from_string(row.member_id.clone()));
        current_group.response_members.push(join_group_member);
        current_group.members.push(row);
    }

    if !current_group.group_id.is_empty() {
        current_group.set_is_converged();
        groups.insert(current_group.group_id.clone(), current_group);
    }

    Ok(groups)
}
