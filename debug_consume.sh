#!/usr/bin/env bash

# Shell script to test the Kafka broker proxy consume interface using kcat.
# Ensure kcat is installed: https://github.com/edenhill/kcat

echo "Starting consumer for Kafka broker on topic 'test_topic'..."
echo "Press Ctrl+C to stop consuming messages."

# Use kcat to consume messages from the broker at localhost:9092
# -C flag for consumer mode
# -G flag for consumer group mode, followed by group name and topic list
# -f flag for custom formatting:
#   %t = topic
#   %p = partition
#   %o = offset
#   %T = timestamp
#   %k = key
#   %s = value
# -v = Increase verbosity to show some statistics
# -d = Debug flags for: protocol (wire protocol requests/responses), 
#      broker (broker connections), consumer (consumer events), 
#      cgrp (consumer group events)
RDKAFKA_DEBUG=all kcat -X debug=broker,protocol,cgrp,topic \
  -C -b 127.0.0.1:9092 \
  -G test-consumer-group test_topic \
  -X enable.auto.commit=true \
  -X auto.commit.interval.ms=5000 \
  -X enable.auto.offset.store=true \
  -X debug=consumer,cgrp,protocol \
  -f '\n─────────────────────\nTopic: %t\nPartition: %p\nOffset: %o\nTimestamp: %T\nKey: %k\nValue: %s\n─────────────────────\n' \
  -v \
  -d protocol,broker,consumer,cgrp

if [ $? -eq 0 ]; then
  echo "Consumer finished successfully."
else
  echo "Consumer encountered an error."
fi 