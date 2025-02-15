#!/usr/bin/env bash

set -euo pipefail

# Benchmark script using kafka-producer-perf-test
# This provides more detailed metrics than kcat

# Find kafka tools in deps directory
KAFKA_DIR="deps/kafka"
KAFKA_PERF_TEST="${KAFKA_DIR}/bin/kafka-producer-perf-test.sh"

if [ ! -x "${KAFKA_PERF_TEST}" ]; then
    echo "Error: kafka-producer-perf-test not found in ${KAFKA_DIR}/bin/"
    echo "Please run ./setup_kafka_tools.sh first"
    exit 1
fi

# Configuration
BROKER="127.0.0.1:9092"
TOPIC="test_topic"
MESSAGE_SIZE=1000     # Size of each message in bytes
NUM_MESSAGES=1000000  # Number of messages to send
BATCH_SIZE=10000     # Messages per batch
THROUGHPUT=-1        # -1 means no throttling

echo "Starting Kafka producer performance test..."
echo "Configuration:"
echo "- Broker: $BROKER"
echo "- Topic: $TOPIC"
echo "- Message size: $MESSAGE_SIZE bytes"
echo "- Number of messages: $NUM_MESSAGES"
echo "- Batch size: $BATCH_SIZE"
echo "- Throughput: ${THROUGHPUT} messages/sec (unlimited)"
echo

# Run the performance test
"${KAFKA_PERF_TEST}" \
    --topic $TOPIC \
    --num-records $NUM_MESSAGES \
    --record-size $MESSAGE_SIZE \
    --throughput $THROUGHPUT \
    --producer-props bootstrap.servers=$BROKER \
                     acks=1 \
                     client.id=perf-producer-client

# The test will output:
# - Records/sec
# - MB/sec
# - Average latency (ms)
# - Max latency (ms)
# - 50th, 95th, 99th and 99.9th percentile latencies (ms) 