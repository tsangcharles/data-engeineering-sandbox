#!/bin/bash

# Setup script for Airflow PostgreSQL connection
# Run this once after starting the environment with: docker compose up -d

echo "Setting up Airflow PostgreSQL connection..."

# Wait for Airflow to be ready
echo "Waiting for Airflow webserver to be ready..."
while ! curl -f http://localhost:8080/health >/dev/null 2>&1; do
    echo "Waiting for Airflow..."
    sleep 5
done

# Create the PostgreSQL connection
echo "Creating PostgreSQL connection in Airflow..."
docker exec data-engineering-sandbox-airflow-webserver-1 airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-login 'dw_user' \
    --conn-password 'dw_password' \
    --conn-port 5432 \
    --conn-schema 'datawarehouse' \
    --conn-description 'Data Warehouse PostgreSQL Connection' || echo "Connection already exists"

echo "✅ Airflow connection setup completed!"
echo "You can now run DAGs in Airflow at http://localhost:8080"
