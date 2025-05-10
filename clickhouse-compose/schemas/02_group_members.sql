CREATE TABLE IF NOT EXISTS kafka.group_members
(
    group_id String,
    member_id String,
    action Enum8('JoinGroup' = 1, 'SyncGroup' = 2, 'Heartbeat' = 3, 'LeaveGroup' = 4),
    generation_id Int32,
    protocol_type String,
    protocol Array(Tuple(String, String)), -- (protocol_name, protocol_metadata)
    client_id String,
    client_host String,
    subscribed_topics Array(String),
    assignments Array(Tuple(String, String)), -- (topic, assignment_data)
    join_time DateTime64(3),
    update_time DateTime64(3)
)
ENGINE = ReplicatedMergeTree
ORDER BY (group_id, member_id, update_time); 