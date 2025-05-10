-- Initialize the mapping table with partition assignments
-- First, clear any existing mappings
TRUNCATE TABLE kafka.kafka_partition_mapping;

-- Insert mappings for machine_id 1 (partitions 0-49)
INSERT INTO kafka.kafka_partition_mapping (machine_id, min_partition, max_partition)
VALUES (1, 0, 49);

-- Insert mappings for machine_id 2 (partitions 50-99)
INSERT INTO kafka.kafka_partition_mapping (machine_id, min_partition, max_partition)
VALUES (2, 50, 99); 