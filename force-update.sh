#!/bin/bash
set -e

echo "Forcing data update by resetting update timestamps..."
docker exec postgis_db psql -U "${POSTGRES_USER:-bayarea_housing}" -d "${POSTGRES_DB_NAME:-bayarea_housing_db}" -c "DELETE FROM data_sources;"

echo "Restarting data processor to trigger update..."
docker restart bay_area_housing_processor

echo "Update process started. Check logs with: docker logs -f bay_area_housing_processor"
