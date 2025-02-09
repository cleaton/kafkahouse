# KafkaHouse

A Kafka-compatible message broker using ClickHouse for storage.

## Setup

### ClickHouse Table Schema

Before running the application, ensure you have created the required table in ClickHouse:

```sql
CREATE TABLE messages ON CLUSTER '{cluster}' (
    topic LowCardinality(String),
    partition UInt32,
    data String,
    offset UInt64 DEFAULT generateSerialID(concat('{shard}', '_', '{replica}', '_messages')),
    timestamp DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/messages', '{replica}')
ORDER BY (topic, partition, offset);
```

Note: Replace `{cluster}` with your cluster name (e.g., 'cluster_1S_2R' in the example config). or use the cluster macro in the config file.

This creates a distributed table with:
- Topic names stored efficiently using dictionary encoding
- Message offsets auto-generated using `generateSerialID` with unique counter per shard/replica
- Async inserts enabled for better performance
- Replication and sharding support via ReplicatedMergeTree engine 