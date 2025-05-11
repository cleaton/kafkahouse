#!/bin/bash

echo "Stopping ClickHouse cluster..."

# Export current user and group IDs for container permissions
export CURRENT_UID=$(id -u)
export CURRENT_GID=$(id -g)

# Stop the containers and remove volumes
cd clickhouse-compose
docker compose -f docker-compose.yaml down

echo "ClickHouse cluster stopped."
echo "Configuration in fs/volumes is preserved for next startup." 