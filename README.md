# KafkaHouse

A Kafka-compatible message broker using ClickHouse for storage.

## Setup

### ClickHouse Table Schema

Before running the application, ensure you have created the required table in ClickHouse:

```sql
CREATE TABLE kafka_messages ON CLUSTER '{cluster}' (
    topic LowCardinality(String) CODEC(ZSTD(1)),
    partition UInt32 CODEC(ZSTD(1)),
    offset UInt64 DEFAULT generateSerialID(concat('{shard}', '_', '{replica}', '_messages')) CODEC(Delta, ZSTD(1)),
    key String CODEC(ZSTD(1)),
    value String CODEC(ZSTD(1)),
    ts DateTime64 DEFAULT now() CODEC(Delta, ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/kafka_messages', '{replica}')
    PARTITION BY (toYYYYMMDD(ts))
    PRIMARY KEY (topic, partition, toStartOfTenMinutes(ts))
    ORDER BY (topic, partition, toStartOfTenMinutes(ts), offset);
```

Note: Replace `{cluster}` with your cluster name (e.g., 'cluster_1S_2R' in the example config). or use the cluster macro in the config file.

This creates a distributed table with:
- Topic names stored efficiently using dictionary encoding
- Message offsets auto-generated using `generateSerialID` with unique counter per shard/replica
- Async inserts enabled for better performance
- Replication and sharding support via ReplicatedMergeTree engine 