CREATE DATABASE IF NOT EXISTS kafka
ENGINE = Replicated('/clickhouse/databases/kafka', '{shard}', '{replica}'); 