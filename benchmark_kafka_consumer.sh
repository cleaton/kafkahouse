#!/usr/bin/env bash

set -euo pipefail

# Benchmark script using kafka-consumer-perf-test
# This provides more detailed metrics than kcat

# Find kafka tools in deps directory
KAFKA_DIR="deps/kafka"
KAFKA_PERF_TEST="${KAFKA_DIR}/bin/kafka-consumer-perf-test.sh"

if [ ! -x "${KAFKA_PERF_TEST}" ]; then
    echo "Error: kafka-consumer-perf-test not found in ${KAFKA_DIR}/bin/"
    echo "Please run ./setup_kafka_tools.sh first"
    exit 1
fi

# Configuration
BROKER="127.0.0.1:9092"
TOPIC="test_topic"
NUM_MESSAGES=1000000  # Number of messages to fetch
FETCH_SIZE=1048576   # Fetch size in bytes (1MB)
TIMEOUT=100000        # Timeout between records in ms
REPORTING_INTERVAL=1000  # How often to print stats in ms

echo "Starting Kafka consumer performance test..."
echo "Configuration:"
echo "- Broker: $BROKER"
echo "- Topic: $TOPIC"
echo "- Number of messages: $NUM_MESSAGES"
echo "- Fetch size: $FETCH_SIZE bytes"
echo "- Timeout: $TIMEOUT ms"
echo "- Reporting interval: $REPORTING_INTERVAL ms"
echo

# Run the performance test
"${KAFKA_PERF_TEST}" \
    --bootstrap-server $BROKER \
    --topic $TOPIC \
    --messages $NUM_MESSAGES \
    --fetch-size $FETCH_SIZE \
    --timeout $TIMEOUT \
    --reporting-interval $REPORTING_INTERVAL \
    --show-detailed-stats \
    --from-latest

# The test will output performance metrics including:
# - MB/sec
# - Records/sec
# - Total time
# - Total data consumed 