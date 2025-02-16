#!/usr/bin/env bash

# Shell script to test the Kafka broker proxy consume interface using kcat.
# Ensure kcat is installed: https://github.com/edenhill/kcat

echo "Starting consumer for Kafka broker on topic 'test_topic'..."
echo "Press Ctrl+C to stop consuming messages."

# Use kcat to consume messages from the broker at localhost:9092
# -C flag for consumer mode
# -f flag for custom formatting:
#   %t = topic
#   %p = partition
#   %o = offset
#   %T = timestamp
#   %k = key
#   %s = value
kcat \
  -C -b 127.0.0.1:9092 -t test_topic \
  -f '\n─────────────────────\nTopic: %t\nPartition: %p\nOffset: %o\nTimestamp: %T\nKey: %k\nValue: %s\n─────────────────────\n' \
  -d broker

if [ $? -eq 0 ]; then
  echo "Consumer finished successfully."
else
  echo "Consumer encountered an error."
fi 