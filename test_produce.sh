#!/usr/bin/env bash

# Shell script to test the Kafka broker proxy produce interface using kcat.
# Ensure kcat is installed: https://github.com/edenhill/kcat

MESSAGE="Test message from kcat at $(date)"

echo "Producing message to Kafka broker on topic 'test_topic'..."

# Use kcat to produce the message to the broker at localhost:9092
# Adjust broker, topic, and options as necessary
echo "$MESSAGE" | RDKAFKA_DEBUG=all kcat -X debug=broker,protocol -P -b 127.0.0.1:9092 -t test_topic -d broker

if [ $? -eq 0 ]; then
  echo "Message produced successfully."
else
  echo "Failed to produce message."
fi 