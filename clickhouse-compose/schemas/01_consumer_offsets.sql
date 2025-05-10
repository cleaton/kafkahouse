CREATE TABLE IF NOT EXISTS kafka.consumer_offsets
(
    group_id String,
    topic String,
    partition Int32,
    offset Int64,
    metadata String,
    member_id String,
    client_id String,
    client_host String,
    commit_time DateTime64(3)
)
ENGINE = ReplicatedMergeTree
ORDER BY (group_id, topic, partition, commit_time); 