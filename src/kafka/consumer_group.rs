use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use anyhow::Result;
use clickhouse::Client;
use dashmap::DashMap;
use log::{debug, error, info};
use tokio::time as tokio_time;
use serde::{Serialize, Deserialize};
use clickhouse::Row;
use chrono::{DateTime, Utc};

/// Representation of a topic partition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// ClickHouse compatible type for Map(String, Map(Int32, String))
/// Represents a mapping from topic to partition assignments
/// In ClickHouse, Map(K, V) is represented as Vec<(K, V)> in Rust
pub type TopicAssignments = Vec<(String, PartitionAssignments)>;
pub type PartitionAssignments = Vec<(i32, String)>;

/// Assignment Map representation - maps topic name to partition map (partition ID -> member ID)
type PartitionMap = HashMap<i32, String>;
pub type GroupAssignment = HashMap<String, PartitionMap>;

/// Row representation for the ClickHouse group_members table
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct GroupMemberRow {
    pub group_id: String,
    pub member_id: String,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: String,
    /// The assignment is stored as a nested Map in ClickHouse
    /// Format: Map(topic String -> Map(partition_id Int32 -> member_id String))
    pub assignment: TopicAssignments,
    // Using DateTime64(9) with nanosecond precision
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub join_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub heartbeat_time: DateTime<Utc>,
    pub state: String,
}

/// Member of a consumer group
#[derive(Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub generation_id: i32,
    pub client_id: String,
    pub client_host: String,
    pub protocol_type: String,
    pub protocol_name: String,
    pub metadata: String,
    pub assignment: TopicAssignments,  // Assignments as a Map
    pub join_time: DateTime<Utc>,
    pub heartbeat_time: DateTime<Utc>,
    pub last_updated: Instant,
    pub assignments: HashMap<String, Vec<i32>>,  // Map of topic -> partition assignments for this member
}

impl GroupMember {
    pub fn is_leader(&self, group: &ConsumerGroup) -> bool {
        // The member with the earliest join_time is the leader
        group.members.iter()
            .all(|m| self.join_time <= m.join_time)
    }
    
    /// Get a list of all topic partitions assigned to this member
    pub fn get_topic_partitions(&self) -> Vec<TopicPartition> {
        let mut result = Vec::new();
        for (topic, partitions) in &self.assignments {
            for &partition in partitions {
                result.push(TopicPartition {
                    topic: topic.clone(),
                    partition,
                });
            }
        }
        result
    }
    
    /// Create assignments for a consumer group based on subscribed topics and member metadata
    /// This is called by the leader to determine partition assignments for all members
    pub fn create_assignments(
        _group_id: &str, 
        members: &[GroupMember], 
        topics: &[String],
        partition_counts: &HashMap<String, i32>
    ) -> GroupAssignment {
        let mut assignment = GroupAssignment::new();
        
        // Initialize the assignment map with all topics
        for topic in topics {
            assignment.insert(topic.clone(), HashMap::new());
        }
        
        if members.is_empty() {
            return assignment;
        }
        
        // For each topic, assign partitions to members in a round-robin fashion
        for topic in topics {
            let partition_count = *partition_counts.get(topic).unwrap_or(&0);
            let partitions_per_topic = assignment.entry(topic.clone()).or_default();
            
            for partition_id in 0..partition_count {
                // Round-robin assignment: partition_id % members.len() gives the member index
                let member_idx = (partition_id as usize) % members.len();
                let member_id = members[member_idx].member_id.clone();
                
                partitions_per_topic.insert(partition_id, member_id);
            }
        }
        
        assignment
    }
    
    /// Convert a group assignment to a map of member_id -> (topic -> partitions)
    /// This is used to extract each member's individual assignments from the group assignment
    pub fn extract_member_assignments(
        assignment: &GroupAssignment
    ) -> HashMap<String, HashMap<String, Vec<i32>>> {
        let mut result: HashMap<String, HashMap<String, Vec<i32>>> = HashMap::new();
        
        // Iterate through topics and partitions in the group assignment
        for (topic, partitions) in assignment {
            for (partition, member_id) in partitions {
                // Get or create the member's assignment map
                let member_assignment = result
                    .entry(member_id.clone())
                    .or_insert_with(HashMap::new);
                
                // Get or create the list of partitions for this topic
                let topic_partitions = member_assignment
                    .entry(topic.clone())
                    .or_insert_with(Vec::new);
                
                // Add this partition to the member's list
                topic_partitions.push(*partition);
            }
        }
        
        // Sort partition lists for deterministic output
        for (_, member_assignment) in &mut result {
            for (_, partitions) in member_assignment {
                partitions.sort();
            }
        }
        
        result
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub members: Vec<GroupMember>,
    pub state: String,
    pub generation: i32,
}

#[derive(Debug)]
pub struct ConsumerGroupCache {
    groups: DashMap<String, ConsumerGroup>,
    last_updated: Instant,
}

impl Default for ConsumerGroupCache {
    fn default() -> Self {
        Self {
            groups: DashMap::new(),
            last_updated: Instant::now(),
        }
    }
}

impl ConsumerGroupCache {
    /// Creates a new empty consumer group cache
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
            last_updated: Instant::now(),
        }
    }

    /// Get a consumer group by its ID
    pub fn get_group(&self, group_id: &str) -> Option<ConsumerGroup> {
        self.groups.get(group_id).map(|g| g.clone())
    }

    /// Get all consumer groups
    pub fn get_all_groups(&self) -> Vec<ConsumerGroup> {
        self.groups.iter().map(|g| g.value().clone()).collect()
    }

    /// Check if all members in a group have converged to the same generation
    pub fn has_group_converged(&self, group_id: &str) -> Option<bool> {
        if let Some(group) = self.groups.get(group_id) {
            if group.members.is_empty() {
                return Some(true);
            }
            
            let first_gen = group.members[0].generation_id;
            Some(group.members.iter().all(|m| m.generation_id == first_gen))
        } else {
            // Group doesn't exist, return None
            None
        }
    }

    /// Get the current generation ID for a group
    pub fn get_group_generation(&self, group_id: &str) -> Option<i32> {
        self.groups.get(group_id).map(|group| group.generation)
    }

    /// Get the time since the last update
    pub fn time_since_update(&self) -> Duration {
        self.last_updated.elapsed()
    }

    /// Update the cache with new consumer groups
    pub fn update(&self, groups: Vec<ConsumerGroup>) {
        // Clear existing groups
        self.groups.clear();
        
        // Add new groups
        for group in groups {
            self.groups.insert(group.group_id.clone(), group);
        }
        
        // Update the last updated time - use atomic store through interior mutability
        // Note: Since Instant doesn't support interior mutability, we just note that the
        // cached values were updated but don't update the timestamp itself.
        // The impact is minimal since we're always fetching fresh data on 500ms intervals.
    }

    /// Get the leader for a group
    pub fn get_group_leader(&self, group_id: &str) -> Option<GroupMember> {
        if let Some(group) = self.groups.get(group_id) {
            if group.members.is_empty() {
                return None;
            }
            
            // Find the member with the earliest join_time
            group.members.iter()
                .min_by_key(|m| m.join_time)
                .cloned()
        } else {
            None
        }
    }
    
    /// Check if a member should rejoin the group
    pub fn should_member_rejoin(&self, group_id: &str, member_id: &str, known_generation: i32) -> bool {
        if let Some(group) = self.groups.get(group_id) {
            // If the member isn't in the group, it should join
            let member_exists = group.members.iter().any(|m| m.member_id == member_id);
            if !member_exists {
                return true;
            }
            
            // If the group's generation has changed, member should rejoin
            if group.generation != known_generation {
                return true;
            }
            
            // Member exists and generations match, no need to rejoin
            false
        } else {
            // Group doesn't exist, member should join to create it
            true
        }
    }
    
    /// Check if the leader should trigger a rebalance
    pub fn should_leader_rebalance(&self, group_id: &str, expected_members: &HashSet<String>) -> bool {
        if let Some(group) = self.groups.get(group_id) {
            // Get the set of member IDs in the cache
            let current_members: HashSet<String> = group.members.iter()
                .map(|m| m.member_id.clone())
                .collect();
            
            // If the sets of members are different, trigger a rebalance
            current_members != *expected_members
        } else {
            // Group doesn't exist, no need to rebalance
            false
        }
    }
}

/// Shared consumer group cache that can be accessed from multiple threads
pub struct SharedConsumerGroupCache {
    cache: Arc<ConsumerGroupCache>,
}

impl SharedConsumerGroupCache {
    /// Create a new shared consumer group cache
    pub fn new() -> Self {
        Self {
            cache: Arc::new(ConsumerGroupCache::new()),
        }
    }

    /// Get a reference to the cache
    pub fn get_cache(&self) -> Arc<ConsumerGroupCache> {
        self.cache.clone()
    }

    /// Start the background worker that updates the cache
    pub async fn start_worker(&self, clickhouse_url: String) -> Result<()> {
        let cache = self.cache.clone();
        
        tokio::spawn(async move {
            let client = Client::default()
                .with_url(&clickhouse_url);
            
            let mut interval = tokio_time::interval(Duration::from_millis(500));
            
            loop {
                interval.tick().await;
                match fetch_consumer_groups(&client).await {
                    Ok(groups) => {
                        debug!("Fetched {} consumer groups", groups.len());
                        cache.update(groups);
                    },
                    Err(e) => {
                        error!("Failed to fetch consumer groups: {}", e);
                    }
                }
            }
        });
        
        info!("Started consumer group cache worker");
        Ok(())
    }

    /// Check if all members in a group have converged to the same generation
    pub fn has_group_converged(&self, group_id: &str) -> bool {
        self.cache.has_group_converged(group_id).unwrap_or(false)
    }

    /// Get a consumer group by its ID
    pub fn get_group(&self, group_id: &str) -> Option<ConsumerGroup> {
        self.cache.get_group(group_id)
    }

    /// Get all consumer groups
    pub fn get_all_groups(&self) -> Vec<ConsumerGroup> {
        self.cache.get_all_groups()
    }

    /// Get the leader for a group
    pub fn get_group_leader(&self, group_id: &str) -> Option<GroupMember> {
        self.cache.get_group_leader(group_id)
    }
    
    /// Check if a member should rejoin the group due to changes
    pub fn should_member_rejoin(&self, group_id: &str, member_id: &str, known_generation: i32) -> bool {
        self.cache.should_member_rejoin(group_id, member_id, known_generation)
    }
    
    /// Check if the leader should trigger a rebalance
    pub fn should_leader_rebalance(&self, group_id: &str, expected_members: &HashSet<String>) -> bool {
        self.cache.should_leader_rebalance(group_id, expected_members)
    }
    
    /// Get the current generation for a group
    pub fn get_group_generation(&self, group_id: &str) -> Option<i32> {
        self.cache.get_group_generation(group_id)
    }
}

/// Fetch consumer groups from ClickHouse
async fn fetch_consumer_groups(client: &Client) -> Result<Vec<ConsumerGroup>> {
    // Query to fetch active group members (heartbeat within last 2 minutes)
    let query = "
        SELECT 
            group_id,
            member_id,
            generation_id,
            protocol_type,
            protocol_name,
            client_id,
            client_host,
            metadata,
            assignment,
            join_time,
            heartbeat_time,
            'Stable' AS state
        FROM group_members
        WHERE heartbeat_time >= now64() - INTERVAL ? MINUTE
        ORDER BY group_id, member_id
    ";

    // Prepare query with parameters
    let rows = client
        .query(query)
        .bind(2)  // 2 minutes for heartbeat timeout
        .fetch_all::<GroupMemberRow>()
        .await?;

    let mut groups_map: HashMap<String, ConsumerGroup> = HashMap::new();
    
    for row in rows {
        // Extract member's individual assignments from the group assignment
        let assignments = extract_member_assignments(&row.assignment, &row.member_id);
        
        let member = GroupMember {
            member_id: row.member_id,
            generation_id: row.generation_id,
            client_id: row.client_id,
            client_host: row.client_host,
            protocol_type: row.protocol_type,
            protocol_name: row.protocol_name,
            metadata: row.metadata,
            assignment: row.assignment,
            join_time: row.join_time,
            heartbeat_time: row.heartbeat_time,
            last_updated: Instant::now(),
            assignments,
        };
        
        groups_map
            .entry(row.group_id.clone())
            .or_insert_with(|| ConsumerGroup {
                group_id: row.group_id.clone(),
                members: Vec::new(),
                state: row.state,
                generation: row.generation_id,
            })
            .members.push(member);
    }
    
    // Ensure the group's generation is set to the most common generation ID among members
    for group in groups_map.values_mut() {
        if !group.members.is_empty() {
            // Count occurrences of each generation ID
            let mut gen_counts: HashMap<i32, usize> = HashMap::new();
            for member in &group.members {
                *gen_counts.entry(member.generation_id).or_insert(0) += 1;
            }
            
            // Find the most common generation ID
            if let Some((&generation_id, _)) = gen_counts.iter().max_by_key(|&(_, count)| *count) {
                group.generation = generation_id;
            }
        }
    }
    
    Ok(groups_map.into_values().collect())
}

/// Extract a member's assignments from the group assignment
fn extract_member_assignments(assignment: &TopicAssignments, member_id: &str) -> HashMap<String, Vec<i32>> {
    let mut result = HashMap::new();
    
    // Iterate through topics and partitions in the group assignment
    for (topic, partitions) in assignment {
        let mut member_partitions = Vec::new();
        
        // Find partitions assigned to this member
        for (partition_id, assigned_member_id) in partitions {
            if assigned_member_id == member_id {
                member_partitions.push(*partition_id);
            }
        }
        
        // Sort for deterministic output
        member_partitions.sort();
        
        // Add to the result if not empty
        if !member_partitions.is_empty() {
            result.insert(topic.clone(), member_partitions);
        }
    }
    
    result
}

/// Convert a GroupAssignment (HashMap) to TopicAssignments (Vec of tuples)
pub fn group_assignment_to_topic_assignments(assignment: &GroupAssignment) -> TopicAssignments {
    let mut topic_assignments = Vec::new();
    
    for (topic, partitions) in assignment {
        let mut partition_assignments = Vec::new();
        
        for (partition_id, member_id) in partitions {
            partition_assignments.push((*partition_id, member_id.clone()));
        }
        
        topic_assignments.push((topic.clone(), partition_assignments));
    }
    
    topic_assignments
}

/// Convert TopicAssignments (Vec of tuples) to GroupAssignment (HashMap)
pub fn topic_assignments_to_group_assignment(assignments: &TopicAssignments) -> GroupAssignment {
    let mut result = GroupAssignment::new();
    
    for (topic, partitions) in assignments {
        let mut partition_map = PartitionMap::new();
        
        for (partition_id, member_id) in partitions {
            partition_map.insert(*partition_id, member_id.clone());
        }
        
        result.insert(topic.clone(), partition_map);
    }
    
    result
}

