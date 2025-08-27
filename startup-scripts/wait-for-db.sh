#!/bin/bash

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
while ! pg_isready -h postgres -U dw_user -d datawarehouse >/dev/null 2>&1; do
  echo "PostgreSQL not ready yet..."
  sleep 2
done

# Wait for specific database to exist
DB_NAME=$1
echo "Waiting for database '$DB_NAME' to be created..."
while ! psql -h postgres -U dw_user -d $DB_NAME -c 'SELECT 1' >/dev/null 2>&1; do
  echo "Database '$DB_NAME' not ready yet..."
  sleep 2
done

echo "Database '$DB_NAME' is ready!"
exec "$@"
