# Kafka ClickHouse Broker

A Kafka broker implementation that stores messages in ClickHouse.

## Prerequisites

- [kcat](https://github.com/edenhill/kcat) (formerly known as kafkacat)
- ClickHouse server running on localhost:8123

## Usage

### Starting the Broker

```bash
cargo run --bin kafka_ch
```

The broker will start listening on `localhost:9092`.

### Producing Messages

Use the `produce.sh` script to send messages to the broker:

```bash
./produce.sh "Your message here" [topic]
```

This will send a message to the specified topic (or "testing_clickhouse_broker" by default) with a random key.

### Consuming Messages

Use the `consume.sh` script to read messages from the broker:

```bash
./consume.sh [topic]
```

This will start a consumer that reads from the specified topic (or "testing_clickhouse_broker" by default) and prints the topic, partition, key, and value of each message.

## Scripts

### produce.sh

This script sends a message to the Kafka broker using kcat:

```bash
./produce.sh "Hello, Kafka!" my_topic
```

If no topic is specified, it defaults to "testing_clickhouse_broker".

### consume.sh

This script reads messages from the Kafka broker using kcat:

```bash
./consume.sh my_topic
```

If no topic is specified, it defaults to "testing_clickhouse_broker".

Press Ctrl+C to stop the consumer.

## Implementation Details

The broker stores messages in ClickHouse and implements the following Kafka protocol features:

- Produce API
- Fetch API
- Consumer Group Management
- Offset Management

The implementation uses the `kafka-protocol` crate for encoding and decoding Kafka protocol messages.

## Setup

### ClickHouse Table Schema

Before running the application, ensure you have created the required table in ClickHouse:

```sql
-- Create a mapping table for machine_id -> partition range using ReplicatedReplacingMergeTree
CREATE TABLE kafka_partition_mapping ON CLUSTER '{cluster}' (
    machine_id UInt16,
    min_partition UInt32,
    max_partition UInt32,
    version DateTime DEFAULT now(),
    PRIMARY KEY (machine_id)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/kafka_partition_mapping', '{replica}', version)
ORDER BY machine_id;

-- Create a dictionary that loads from the mapping table
CREATE DICTIONARY kafka_partition_dict ON CLUSTER '{cluster}' (
    machine_id UInt16,
    min_partition UInt32,
    max_partition UInt32
)
PRIMARY KEY machine_id
SOURCE(CLICKHOUSE(TABLE 'kafka_partition_mapping'))
LIFETIME(MIN 5 MAX 30)
LAYOUT(FLAT());

-- Create the main messages table with generateSnowflakeID using machine_id
CREATE TABLE kafka_messages ON CLUSTER '{cluster}' (
    topic LowCardinality(String) CODEC(ZSTD(1)),
    partition UInt32 CODEC(ZSTD(1)),
    offset Int64 DEFAULT toInt64(generateSnowflakeID(toUInt16(getMacro('machine_id'))) - bitShiftLeft(toUInt64(1735689600000), 22)) CODEC(Delta, ZSTD(1)),
    key String CODEC(ZSTD(1)),
    value String CODEC(ZSTD(1)),
    ts DateTime64 DEFAULT now() CODEC(Delta, ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/kafka_messages', '{replica}')
    PARTITION BY toYYYYMMDD(snowflakeIDToDateTime(toUInt64(offset), 1735689600000))
    ORDER BY (topic, partition, offset);

-- Create a null table for inserts
CREATE TABLE kafka_messages_ingest ON CLUSTER '{cluster}' (
    topic LowCardinality(String) CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    value String CODEC(ZSTD(1))
)
ENGINE = Null;

-- Create a materialized view that uses the dictionary for lookups and fast partition selection
CREATE MATERIALIZED VIEW kafka_messages_mv ON CLUSTER '{cluster}'
TO kafka_messages
AS 
WITH 
    dictGet('kafka_partition_dict', 'min_partition', toUInt16(getMacro('machine_id'))) AS min_partition,
    dictGet('kafka_partition_dict', 'max_partition', toUInt16(getMacro('machine_id'))) AS max_partition
SELECT
    topic,
    min_partition + rand32() % (max_partition - min_partition + 1) AS partition,
    key,
    value
FROM kafka_messages_ingest;

-- Initialize the mapping table with partition assignments
-- First, clear any existing mappings
TRUNCATE TABLE kafka_partition_mapping ON CLUSTER '{cluster}';

-- Insert mappings for machine_id 1 (partitions 0-49)
INSERT INTO kafka_partition_mapping (machine_id, min_partition, max_partition)
VALUES (1, 0, 49);

-- Insert mappings for machine_id 2 (partitions 50-99)
INSERT INTO kafka_partition_mapping (machine_id, min_partition, max_partition)
VALUES (2, 50, 99); 