# Create the file
cat > housing_api/startup.sh << 'EOL'
#!/bin/sh
set -e

# Wait for database to be ready
echo "Checking database connection..."
until pg_isready -h ${POSTGIS_HOST:-postgis_db} -U "${POSTGRES_USER:-bayarea_housing}" -d "${POSTGRES_DB_NAME:-bayarea_housing_db}"; do
  echo "Database not ready yet - sleeping 2s"
  sleep 2
done
echo "Database is ready"

# Start API server
exec node server.js
EOL

# Make it executable
chmod +x housing_api/startup.sh