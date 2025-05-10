use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::kafka::broker::Broker;
use crate::kafka::consumer::actor::{ConsumerGroupsApi, GroupInfo};

#[derive(Debug, Clone)]
pub struct TopicPartition {
    pub partition_id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

pub type JoinedGroups = HashMap<String, Vec<(String, String)>>; // group_id -> [(member_id, client_id)]

pub struct ClientState {
    pub broker: Arc<Broker>,
    pub broker_id: i32,
    pub cluster_id: String,
    pub controller_id: Option<i32>,
    pub client_id: String,
    pub client_host: String,
    pub tcp_reader: OwnedReadHalf,
    pub tcp_writer: OwnedWriteHalf,
    
    // Topic metadata
    pub topics: HashMap<String, Vec<TopicPartition>>, // topic name -> partitions
    
    // Group membership
    pub joined_groups: JoinedGroups,
    
    // Active consumer groups
    pub active_groups: HashMap<String, GroupInfo>, // group_id -> group_info
    
    // Committed offsets
    pub committed_offsets: HashMap<String, HashMap<String, HashMap<i32, i64>>>, // group_id -> (topic -> (partition -> offset))
    
    // Supported API versions
    pub supported_api_versions: &'static Vec<ApiVersion>,
    
    // Message buffers
    pub read_buffer: BytesMut,
    pub response_buffer: BytesMut,

    // Consumer groups API
    pub consumer_groups_api: Arc<ConsumerGroupsApi>,
} 