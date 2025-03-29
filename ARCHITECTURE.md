# Kafka ClickHouse Broker Architecture

## Overview

This document outlines the architecture of the Kafka ClickHouse Broker, with a focus on the consumer group implementation. The broker serves as a Kafka-compatible interface that stores messages in ClickHouse instead of Kafka's native storage.

## System Architecture

The Kafka ClickHouse Broker consists of the following components:

1. **Kafka Protocol Handler**: Handles Kafka protocol requests and responses
2. **ClickHouse Storage Interface**: Manages data storage in ClickHouse
3. **Consumer Group Coordinator**: Manages consumer group membership and partition assignments
4. **Offset Manager**: Tracks committed offsets for consumer groups

## Data Flow Architecture

```
                                 ┌─────────────────────┐
                                 │                     │
                                 │  Kafka Clients      │
                                 │                     │
                                 └─────────┬───────────┘
                                           │
                                           ▼
                           ┌───────────────────────────────┐
                           │                               │
                           │   Kafka ClickHouse Broker     │
                           │                               │
                           └───────────────┬───────────────┘
                                           │
                                           ▼
                    ┌─────────────────────────────────────────────┐
                    │                                             │
┌───────────────────┴────────────────┐    ┌────────────────────────┴─────────────────┐
│                                    │    │                                          │
│   ClickHouse Server 1              │    │   ClickHouse Server 2                    │
│   (Partitions 0-49)                │    │   (Partitions 50-99)                     │
│                                    │    │                                          │
└────────────────────────────────────┘    └──────────────────────────────────────────┘
```

### Write Path

1. Producer sends a produce request to the broker
2. Broker assigns partitions based on available ClickHouse servers
3. Data is routed to the appropriate ClickHouse server for storage
4. Broker acknowledges the write to the producer

```
Producer                    Broker                   ClickHouse
   │                          │                          │
   │ ProduceRequest           │                          │
   │────────────────────────►│                          │
   │                          │                          │
   │                          │ Insert into kafka_messages_ingest
   │                          │─────────────────────────►│
   │                          │                          │
   │                          │                          │
   │                          │ Assignment to partition  │
   │                          │ via materialized view    │
   │                          │◄─────────────────────────│
   │                          │                          │
   │ ProduceResponse          │                          │
   │◄────────────────────────│                          │
   │                          │                          │
```

### Read Path

1. Consumer sends a fetch request to the broker
2. Broker determines which ClickHouse server hosts the requested partitions
3. Broker fetches data from the appropriate ClickHouse server
4. Broker returns the data to the consumer

```
Consumer                    Broker                   ClickHouse
   │                          │                          │
   │ FetchRequest             │                          │
   │────────────────────────►│                          │
   │                          │ Query kafka_messages     │
   │                          │─────────────────────────►│
   │                          │                          │
   │                          │ Return records           │
   │                          │◄─────────────────────────│
   │                          │                          │
   │ FetchResponse            │                          │
   │◄────────────────────────│                          │
   │                          │                          │
```

## Consumer Group Implementation

### Consumer Group Metadata

Consumer group metadata will be stored in ClickHouse tables:

```sql
-- Group member mapping table
CREATE TABLE group_members ON CLUSTER '{cluster}' (
    group_id String,
    member_id String,
    generation_id Int32,
    protocol_type String,
    protocol_name String,
    client_id String,
    client_host String,
    metadata String,
    assignment Map(String, Map(Int32, String)) COMMENT 'Map of topic -> (partition_id -> member_id)', 
    join_time DateTime64(9) DEFAULT now64(),
    heartbeat_time DateTime64(9) DEFAULT now64(),
    state String,
    PRIMARY KEY (group_id, member_id)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/group_members', '{replica}')
PARTITION BY toYYYYMMDD(toStartOfInterval(heartbeat_time, INTERVAL 1 WEEK))
ORDER BY (group_id, member_id, heartbeat_time)
TTL heartbeat_time + INTERVAL 4 WEEK;

-- Offset tracking table
CREATE TABLE consumer_offsets ON CLUSTER '{cluster}' (
    group_id String,
    topic String,
    partition Int32,
    offset Int64,
    metadata String,
    member_id String,
    client_id String,
    client_host String,
    commit_time DateTime64(3) DEFAULT now64(),
    PRIMARY KEY (group_id, topic, partition)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/consumer_offsets', '{replica}')
PARTITION BY toYYYYMMDD(toStartOfInterval(commit_time, INTERVAL 1 WEEK))
ORDER BY (group_id, topic, partition)
TTL commit_time + INTERVAL 8 WEEK;
```

The `group_members` table tracks all active consumer group members with their metadata and assignments. The member with the earliest `join_time` is considered the leader of the group. The `join_time` is set when a member first joins and remains unchanged, while `heartbeat_time` is updated with each heartbeat.

The assignment field uses ClickHouse's native Map type to store partition assignments. The format is a nested Map structure:
- The outer Map maps topic names (String) to an inner Map of partition assignments
- The inner Map maps partition IDs (Int32) to member IDs (String)

This structure allows efficient lookups for which member owns a specific partition without needing to parse JSON strings.

The `consumer_offsets` table tracks committed offsets for each consumer group, topic, and partition combination, along with which member committed the offset.

Both tables are partitioned by week based on their timestamp fields and have TTL settings to automatically clean up old data.

### Consumer Group Coordination Design

Our implementation uses a shared cache approach for consumer group coordination:

1. **Shared Cache Worker**:
   - Each broker instance runs a single background worker that fetches group members every 500ms
   - The worker queries for active members (heartbeat within last 2 minutes)
   - Updates a thread-safe shared cache that all connections can access

2. **Leader Determination**:
   - The member with the earliest join_time is automatically the group leader
   - The leader is responsible for group rebalancing and partition assignments
   - If the leader leaves, the member with the next earliest join_time becomes leader

3. **Leader Responsibilities**:
   - On each heartbeat, leader checks its known members against the shared cache
   - If members have joined or left, leader triggers rebalance and creates new assignments
   - Leader distributes new assignments with updated generation ID

4. **Member Behavior**:
   - Non-leader members check the shared cache on each heartbeat
   - If the group's generation has changed, they trigger rejoin with new assignments
   - This ensures all members converge to the same generation and assignments

5. **Rebalancing**:
   - Partition assignment uses a round-robin strategy across members
   - Assignments are stored in JSON format mapping topics to partition ownership
   - Each member extracts its own assignments from the group assignment

### Partition Assignment Architecture

In our implementation, each ClickHouse server is assigned a specific range of Kafka partitions. Unlike traditional Kafka, where producers specify the partition, in our system:

1. Each ClickHouse server manages a predefined range of partitions
2. On produce, the data is assigned to whichever partitions are available on the server handling the insert
3. For consumption, fetches are routed to the ClickHouse server owning the requested partition

This approach provides:
- High throughput by optimizing data locality
- High availability by allowing writes to continue on remaining replicas if one ClickHouse server is down
- Data consistency through partition ownership

```
┌─────────────────────────────────────────┐   ┌─────────────────────────────────────────┐
│                                         │   │                                         │
│  ClickHouse Server 1                    │   │  ClickHouse Server 2                    │
│                                         │   │                                         │
│  ┌─────────────┐    ┌───────────────┐   │   │  ┌─────────────┐    ┌───────────────┐   │
│  │             │    │               │   │   │  │             │    │               │   │
│  │ Partitions  │    │ Committed     │   │   │  │ Partitions  │    │ Committed     │   │
│  │ 0-49        │    │ Offsets for   │   │   │  │ 50-99       │    │ Offsets for   │   │
│  │             │    │ Partitions    │   │   │  │             │    │ Partitions    │   │
│  │             │    │ 0-49          │   │   │  │             │    │ 50-99         │   │
│  └─────────────┘    └───────────────┘   │   │  └─────────────┘    └───────────────┘   │
│                                         │   │                                         │
└─────────────────────────────────────────┘   └─────────────────────────────────────────┘
```

### Consumer Group Protocol Flow

1. **Find Coordinator**: Consumer requests the coordinator for a consumer group
   - The broker acts as the coordinator for all consumer groups

2. **Join Group**: Consumer joins a consumer group
   - The broker records the member in the group_members table
   - The first member becomes the group leader

3. **Sync Group**: Group leader assigns partitions to group members
   - Leader creates assignment based on subscribed topics
   - Each member receives their assignment
   - Assignments are stored in the group_members table

4. **Heartbeat**: Consumers send heartbeats to maintain group membership
   - Heartbeats update the heartbeat_time in group_members
   - If a member fails to heartbeat, it is removed from the group

5. **Offset Commit**: Consumers commit their progress
   - Offsets are stored in consumer_offsets table on the ClickHouse server that owns the partition
   - This ensures offset data locality and consistency

6. **Offset Fetch**: Consumers retrieve their last committed offsets
   - Offsets are fetched from the consumer_offsets table

7. **Leave Group**: Consumers leave the group when they're done
   - Member is removed from the group_members table

```
Consumer                    Broker                   ClickHouse
   │                          │                          │
   │ FindCoordinator          │                          │
   │────────────────────────►│                          │
   │ CoordinatorResponse      │                          │
   │◄────────────────────────│                          │
   │                          │                          │
   │ JoinGroup                │                          │
   │────────────────────────►│                          │
   │                          │ Insert group member      │
   │                          │─────────────────────────►│
   │ JoinGroupResponse        │                          │
   │◄────────────────────────│                          │
   │                          │                          │
   │ SyncGroup                │                          │
   │────────────────────────►│                          │
   │                          │ Store assignment         │
   │                          │─────────────────────────►│
   │ SyncGroupResponse        │                          │
   │◄────────────────────────│                          │
   │                          │                          │
   │ Heartbeat                │                          │
   │────────────────────────►│                          │
   │                          │ Update heartbeat time    │
   │                          │─────────────────────────►│
   │ HeartbeatResponse        │                          │
   │◄────────────────────────│                          │
   │                          │                          │
   │ OffsetCommit             │                          │
   │────────────────────────►│                          │
   │                          │ Store committed offset   │
   │                          │─────────────────────────►│
   │ OffsetCommitResponse     │                          │
   │◄────────────────────────│                          │
   │                          │                          │
```

## Implementation Plan

1. **Create ClickHouse Tables**:
   - Set up consumer_groups, group_members, and consumer_offsets tables
   - Update the existing kafka_partition_mapping to track partition ownership

2. **Enhance Broker Logic**:
   - Modify the produce logic to route to the appropriate ClickHouse server
   - Update fetch logic to query the correct ClickHouse server based on partition ownership

3. **Implement Consumer Group Coordination**:
   - Store consumer group state in ClickHouse instead of in-memory
   - Implement expiration/cleanup of inactive group members based on heartbeat

4. **Offset Management**:
   - Implement offset commit to store offsets on the ClickHouse server owning the partition
   - Implement offset fetch to retrieve committed offsets

5. **Implement Rebalance Protocol**:
   - Handle group rebalance when members join or leave
   - Ensure proper partition distribution among consumers

6. **Monitoring and Diagnostics**:
   - Create views for monitoring consumer progress and lag
   - Implement diagnostic queries for troubleshooting

## Monitoring Consumer Progress

We can create views to monitor consumer progress and lag:

```sql
CREATE VIEW consumer_lag ON CLUSTER '{cluster}' AS
SELECT
    co.group_id,
    co.topic,
    co.partition,
    co.offset AS committed_offset,
    (SELECT max(offset) FROM kafka_messages WHERE topic = co.topic AND partition = co.partition) AS latest_offset,
    (SELECT max(offset) FROM kafka_messages WHERE topic = co.topic AND partition = co.partition) - co.offset AS lag,
    co.commit_time
FROM consumer_offsets co;
```

This view will show:
- The latest committed offset for each consumer group
- The latest available offset for each topic-partition
- The lag between the two (how far behind a consumer is)
- When the offset was last committed

## Conclusion

This architecture provides a robust foundation for implementing consumer groups in the Kafka ClickHouse Broker. By leveraging ClickHouse for both message storage and consumer group metadata, we can achieve:

1. High throughput for producers and consumers
2. High availability through ClickHouse replication
3. Scalability by adding more ClickHouse servers
4. Simplified operations by using ClickHouse for all storage needs

The implementation prioritizes data locality and availability, while providing the full consumer group functionality expected from a Kafka broker. The shared cache worker design ensures all broker instances have a consistent view of consumer group state while minimizing database load.