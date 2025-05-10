#!/bin/bash

# Export current user and group IDs for container permissions
#export CURRENT_UID=$(id -u)
#export CURRENT_GID=$(id -g)

# First, stop any running containers and clean up orphans
cd clickhouse-compose
# Start the containers
echo "Starting ClickHouse cluster..."
docker compose -f docker-compose.yaml up -d --remove-orphans
cd ..

# Wait for services to be ready
echo "Waiting for ClickHouse services to be ready..."
sleep 5

# Check if services are up
echo "Checking ClickHouse services..."
curl -s http://127.0.0.1:8123/ping
if [ $? -eq 0 ]; then
    echo "ClickHouse-01 is ready"
else
    echo "Warning: ClickHouse-01 is not responding"
fi

curl -s http://127.0.0.1:8124/ping
if [ $? -eq 0 ]; then
    echo "ClickHouse-02 is ready"
else
    echo "Warning: ClickHouse-02 is not responding"
fi

echo "Done! Services should be running."
echo "You can check logs with: cd clickhouse-compose && docker compose -f docker-compose.yaml logs -f" 