/// Represents a single partition within a topic
#[derive(Debug, Clone)]
pub struct TopicPartition {
    /// Partition ID
    pub partition_id: i32,
    /// Fetch offset for this partition
    pub offset: i64,
}

impl TopicPartition {
    /// Creates a new TopicPartition
    pub fn new(partition_id: i32, offset: i64) -> Self {
        Self { partition_id, offset }
    }
}

/// Represents a topic and its partitions
#[derive(Debug, Clone)]
pub struct TopicPartitions {
    /// The name of the topic
    pub topic: String,
    /// The list of partitions for this topic
    pub partitions: Vec<TopicPartition>,
}

impl TopicPartitions {
    /// Creates a new TopicPartitions instance
    pub fn new(topic: String, partitions: Vec<TopicPartition>) -> Self {
        Self { topic, partitions }
    }
} 