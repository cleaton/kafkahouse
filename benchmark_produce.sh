#!/usr/bin/env bash

# Benchmark script to test Kafka broker produce performance using kcat
# Ensure kcat is installed: https://github.com/edenhill/kcat

# Configuration
BROKER="127.0.0.1:9092"
TOPIC="test_topic"
MESSAGE_SIZE=100      # Size of each message in bytes
NUM_MESSAGES=1000   # Number of messages to send
BATCH_SIZE=1000     # Messages per batch

echo "Starting benchmark..."
echo "Configuration:"
echo "- Broker: $BROKER"
echo "- Topic: $TOPIC"
echo "- Message size: $MESSAGE_SIZE bytes"
echo "- Number of messages: $NUM_MESSAGES"
echo "- Batch size: $BATCH_SIZE"
echo

# Run benchmark using kcat
kcat -P -b $BROKER -t $TOPIC \
    -q \
    -X batch.size=$BATCH_SIZE \
    -c $NUM_MESSAGES \
    -Z \
    -X statistics.interval.ms=1000 >/dev/null < <(while true; do echo "test string message"; sleep 0.001; done | head -n $NUM_MESSAGES)

# For comparison, you can also use kafka-producer-perf-test:
# kafka-producer-perf-test \
#     --topic $TOPIC \
#     --num-records $NUM_MESSAGES \
#     --record-size $MESSAGE_SIZE \
#     --throughput -1 \
#     --producer-props bootstrap.servers=$BROKER batch.size=$BATCH_SIZE 