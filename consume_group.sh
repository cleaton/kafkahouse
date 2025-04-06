#!/bin/bash

# Check if a group ID was provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <group_id> [topic]"
    echo "Example: $0 my-group testing_clickhouse_broker"
    exit 1
fi

# The consumer group ID
GROUP_ID="$1"

# Topic to consume from (default or provided as second argument)
TOPIC="${2:-testing_clickhouse_broker}"

# Broker address
BROKER="127.0.0.1:9092"

echo "Starting consumer in group $GROUP_ID for topic $TOPIC"
echo "Press Ctrl+C to stop consuming"

# Use kcat to consume messages with consumer group
# -C: Consumer mode
# -b: Broker list
# -G: Consumer group mode (group.id topic.list..)
# -f: Custom format string
kcat -X debug=broker -C -b $BROKER -G $GROUP_ID $TOPIC \
    -f '\nTopic: %t\nPartition: %p\nOffset: %o\nKey: %k\nValue: %s\nTimestamp: %T\n--\n'
