#!/bin/bash

# Topic to consume from (default or provided as first argument)
TOPIC="${1:-testing_clickhouse_broker}"

# Broker address
BROKER="127.0.0.1:9092"

# Consumer group ID
GROUP_ID="clickhouse-consumer-group"

echo "Starting consumer for topic $TOPIC..."
echo "Press Ctrl+C to exit"
echo "----------------------------------------"

# Use kcat to consume messages
# -C: Consumer mode
# -b: Broker address
# -t: Topic
# -f: Format string
# -o: Offset (end = latest, beginning = earliest)
# -G: Consumer group mode
# -f: Format string with %o for offset
kcat -C -b $BROKER -t $TOPIC -f 'Topic: %t\nPartition: %p\nKey: %k\nValue: %s\nOffset: %o\n----------------------------------------\n' -o end -G $GROUP_ID $TOPIC 