#!/bin/bash
set -e

echo "Starting initialization process..."

# Wait for database to be ready
echo "Waiting for database..."
until pg_isready -h postgis_db -U "${POSTGRES_USER}" -d "${POSTGRES_DB_NAME}"; do
  echo "Database not ready yet - sleeping 2s"
  sleep 2
done
echo "Database is up and running!"

# Initialize database schema
echo "Initializing database schema..."
python /app/init_db.py

echo "Running data collectors..."
python /app/data_collectors/data_collection_system.py
echo "Data collection complete!"

# Check and fix data issues
echo "Checking and fixing data issues..."
python /app/check_and_fix_data.py

echo "Initialization complete!"

# Execute the command passed to the script
exec "$@"
