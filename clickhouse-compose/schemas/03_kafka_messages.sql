CREATE TABLE IF NOT EXISTS kafka.kafka_messages
(
    topic LowCardinality(String) CODEC(ZSTD(1)),
    partition UInt32 CODEC(ZSTD(1)),
    offset Int64 DEFAULT toInt64(generateSnowflakeID(toUInt16(getMacro('machine_id'))) - bitShiftLeft(toUInt64(1735689600000), 22)) CODEC(Delta, ZSTD(1)),
    key String CODEC(ZSTD(1)),
    value String CODEC(ZSTD(1)),
    ts DateTime64 DEFAULT now() CODEC(Delta, ZSTD(1))
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYYMMDD(snowflakeIDToDateTime(toUInt64(offset), 1735689600000))
ORDER BY (topic, partition, offset);

CREATE TABLE IF NOT EXISTS kafka.kafka_messages_ingest
(
    topic LowCardinality(String) CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    value String CODEC(ZSTD(1))
)
ENGINE = Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS kafka.kafka_messages_mv
TO kafka.kafka_messages
AS 
WITH 
    dictGet('kafka.kafka_partition_dict', 'min_partition', toUInt16(getMacro('machine_id'))) AS min_partition,
    dictGet('kafka.kafka_partition_dict', 'max_partition', toUInt16(getMacro('machine_id'))) AS max_partition
SELECT
    topic,
    min_partition + rand32() % (max_partition - min_partition + 1) AS partition,
    key,
    value
FROM kafka.kafka_messages_ingest; 