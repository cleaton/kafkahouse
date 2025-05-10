CREATE TABLE IF NOT EXISTS kafka.kafka_partition_mapping
(
    machine_id UInt16,
    min_partition UInt32,
    max_partition UInt32,
    version DateTime DEFAULT now(),
    PRIMARY KEY (machine_id)
)
ENGINE = ReplicatedMergeTree
ORDER BY machine_id;

CREATE DICTIONARY IF NOT EXISTS kafka.kafka_partition_dict
(
    machine_id UInt16,
    min_partition UInt32,
    max_partition UInt32
)
PRIMARY KEY machine_id
SOURCE(CLICKHOUSE(TABLE 'kafka.kafka_partition_mapping'))
LIFETIME(MIN 5 MAX 30)
LAYOUT(FLAT()); 