# Kafka CH

A Kafka-compatible broker implementation that uses ClickHouse for storage.

## Why?

### Simplify Your Stack
- **Single Storage Layer**: Store both streaming data and analytics in ClickHouse
- **Reduced Operational Overhead**: Maintain one system instead of separate Kafka and analytics databases
- **Cost-Effective**: Leverage ClickHouse's efficient storage and compression

### Powerful Capabilities
- **Real-time Analytics**: Attach materialized views to topics for instant aggregations as data arrives
- **SQL-Based Filtering**: Use SQL to filter messages by topic, content, or metadata
- **Advanced Querying**: Process streaming data with the full power of ClickHouse SQL
- **Object Storage Integration**: Use ClickHouse's native S3 support for scalable, cost-effective storage

### Technical Advantages
- **Query-First Design**: Adds Kafka protocol to a powerful query engine instead of adding query capabilities to a broker
- **Columnar Storage**: Store data in optimized formats with field extraction and efficient compression
- **JSON Processing**: Native handling of JSON data with automatic field extraction

### Industry Context
- Projects like Confluent and Redpanda are integrating analytics storage (Iceberg, S3) with Kafka
- Solutions like WarpStream use S3 as primary storage for Kafka, making serverless Kafka possible
- Kafka CH follows this trend by making ClickHouse the single source of truth for both ingestion and analytics

## Development Status

This is a raw proof-of-concept in very early development. The broker currently:
- Accepts Kafka client connections
- Handles producer requests and stores messages in ClickHouse
- Supports basic consumer group coordination
- Processes fetch requests to read data
- Supports offset commit and listing (Kafka consumer group offsets)

## Running the Project

### Prerequisites

- [Docker](https://www.docker.com/get-started/) for running ClickHouse cluster
- [kcat](https://github.com/edenhill/kcat) (formerly known as kafkacat) for testing
- [Rust and Cargo](https://www.rust-lang.org/tools/install) to build and run the project

### Steps to Run

1. Start the ClickHouse cluster:
   ```
   ./start_clickhouse_cluster.sh
   ```

2. Run the Kafka-CH broker:
   ```
   cargo run
   ```
   The broker listens on port 9092 by default.

3. Start a test consumer:
   ```
   ./consume.sh
   ```

4. Send test messages:
   ```
   ./produce.sh "Hello World Message"
   ```

5. View data in ClickHouse:
   - Open ClickHouse UI at http://127.0.0.1:8123/play
   - All Kafka data is stored in the `kafka` database

## Usage

## Project Structure

```
src/
├── main.rs              # Application entry point
├── kafka/               # Kafka implementation
│   ├── mod.rs           # Module exports
│   ├── broker/          # Broker implementation
│   │   ├── mod.rs
│   │   ├── broker.rs    # Core broker implementation
│   │   └── types.rs     # Broker-specific types
│   ├── client/          # Client handling
│   │   ├── mod.rs
│   │   ├── actor.rs     # Client connection actor
│   │   ├── types.rs     # Client-related types
│   │   └── handlers/    # Protocol handlers
│   │       ├── mod.rs
│   │       ├── api_versions.rs
│   │       ├── metadata.rs
│   │       └── ...      # Other protocol handlers
│   ├── consumer/        # Consumer group implementation
│   │   ├── mod.rs
│   │   ├── group.rs     # Consumer group management
│   │   └── actor.rs     # Consumer group actor
│   └── protocol.rs      # Kafka protocol handling
└── storage/             # Storage implementation
```

## Implementation Details

The broker stores messages in ClickHouse and implements the following Kafka protocol features:

- Produce API
- Fetch API
- Consumer Group Management
- Offset Management

The implementation uses the `kafka-protocol` crate for encoding and decoding Kafka protocol messages.


# ClickHouse Table Schema

`clickhouse-compose/schemas`