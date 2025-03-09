#!/bin/bash

# Check if a message was provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <message> [topic]"
    echo "Example: $0 'Hello, Kafka!' testing_clickhouse_broker"
    exit 1
fi

# The message to produce
MESSAGE="$1"

# Topic to produce to (default or provided as second argument)
TOPIC="${2:-testing_clickhouse_broker}"

# Broker address
BROKER="127.0.0.1:9092"

# Generate a random key
KEY="key-$(date +%s)"

echo "Producing message to $TOPIC:"
echo "Key: $KEY"
echo "Value: $MESSAGE"

# Use kcat to produce the message
echo "$MESSAGE" | kcat -P -b $BROKER -t $TOPIC -k "$KEY"

echo "Message sent!" 