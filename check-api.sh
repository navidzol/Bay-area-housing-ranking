#!/bin/bash
# Save this script as check-api.sh and run it from the root of your project

echo "==============================================="
echo "      Bay Area Housing API Diagnostics"
echo "==============================================="

# Check Docker containers
echo -e "\n[1] Checking Docker containers..."
docker ps -a | grep bay_area

# Check API container logs
echo -e "\n[2] Checking API container logs (last 20 lines)..."
API_CONTAINER=$(docker ps -a | grep bay_area | grep api | awk '{print $1}')
if [ -n "$API_CONTAINER" ]; then
  docker logs $API_CONTAINER --tail 20
else
  echo "API container not found"
fi

# Check database connection
echo -e "\n[3] Testing database connection..."
DB_CONTAINER=$(docker ps -a | grep postgis | awk '{print $1}')
if [ -n "$DB_CONTAINER" ]; then
  docker exec $DB_CONTAINER psql -U ${POSTGRES_USER:-bayarea_housing} -d ${POSTGRES_DB_NAME:-bayarea_housing_db} -c "SELECT 'Database connection successful';"
  docker exec $DB_CONTAINER psql -U ${POSTGRES_USER:-bayarea_housing} -d ${POSTGRES_DB_NAME:-bayarea_housing_db} -c "SELECT COUNT(*) FROM zipcodes;"
  docker exec $DB_CONTAINER psql -U ${POSTGRES_USER:-bayarea_housing} -d ${POSTGRES_DB_NAME:-bayarea_housing_db} -c "SELECT COUNT(*) FROM zipcode_ratings;"
else
  echo "Database container not found"
fi

# Check frontend container logs
echo -e "\n[4] Checking frontend container logs (last 20 lines)..."
FRONTEND_CONTAINER=$(docker ps -a | grep bay_area | grep frontend | awk '{print $1}')
if [ -n "$FRONTEND_CONTAINER" ]; then
  docker logs $FRONTEND_CONTAINER --tail 20
else
  echo "Frontend container not found"
fi

# Test API endpoints
echo -e "\n[5] Testing API endpoints..."
echo "Testing /health endpoint..."
curl -s http://localhost:${WEB_PORT:-8083}/housing_api/health | jq || echo "Error: Failed to reach health endpoint"

echo "Testing /zipcodes/geojson endpoint..."
curl -s -I http://localhost:${WEB_PORT:-8083}/housing_api/zipcodes/geojson | head -n 1 || echo "Error: Failed to reach geojson endpoint"

# Check Nginx config
echo -e "\n[6] Checking Nginx configuration..."
if [ -n "$FRONTEND_CONTAINER" ]; then
  docker exec $FRONTEND_CONTAINER nginx -t 
else
  echo "Frontend container not found, cannot check Nginx config"
fi

# Show network info
echo -e "\n[7] Showing Docker network information..."
docker network ls | grep -E 'bay|postgis'
echo "Network inspection:"
NETWORK=$(docker network ls | grep -E 'bay|postgis' | head -n 1 | awk '{print $2}')
if [ -n "$NETWORK" ]; then
  docker network inspect $NETWORK
else
  echo "Network not found"
fi

echo -e "\n[8] Summary of diagnostics:"
echo "--------------------------------------"

# Check if zipcodes table has data
ZIP_COUNT=0
if [ -n "$DB_CONTAINER" ]; then
  ZIP_COUNT=$(docker exec $DB_CONTAINER psql -U ${POSTGRES_USER:-bayarea_housing} -d ${POSTGRES_DB_NAME:-bayarea_housing_db} -At -c "SELECT COUNT(*) FROM zipcodes;")
fi

# Check if ratings table has data
RATING_COUNT=0
if [ -n "$DB_CONTAINER" ]; then
  RATING_COUNT=$(docker exec $DB_CONTAINER psql -U ${POSTGRES_USER:-bayarea_housing} -d ${POSTGRES_DB_NAME:-bayarea_housing_db} -At -c "SELECT COUNT(*) FROM zipcode_ratings;")
fi

# Check if API is responding
API_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:${WEB_PORT:-8083}/housing_api/health || echo "Error")

if [ "$API_STATUS" == "200" ]; then
  API_STATUS="OK (200)"
else
  API_STATUS="NOT OK ($API_STATUS)"
fi

# Check if GeoJSON endpoint is returning data
GEOJSON_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:${WEB_PORT:-8083}/housing_api/zipcodes/geojson || echo "Error")

if [ "$GEOJSON_STATUS" == "200" ]; then
  GEOJSON_STATUS="OK (200)"
else
  GEOJSON_STATUS="NOT OK ($GEOJSON_STATUS)"
fi

echo "Database Zipcodes: $ZIP_COUNT"
echo "Database Ratings: $RATING_COUNT"
echo "API Health Endpoint: $API_STATUS"
echo "API GeoJSON Endpoint: $GEOJSON_STATUS"

echo -e "\n[9] Suggested fixes:"
echo "--------------------------------------"

if [ "$ZIP_COUNT" -eq 0 ]; then
  echo "- Database has no zipcode data. Try running this command:"
  echo "  docker exec -it \$(docker ps -q -f name=processor) python /app/init_db.py && python /app/load_zipcode_data.py"
fi

if [ "$RATING_COUNT" -eq 0 ]; then
  echo "- Database has no ratings data. Try running this command:"
  echo "  docker exec -it \$(docker ps -q -f name=processor) python /app/update_data.py"
fi

if [ "$API_STATUS" != "OK (200)" ]; then
  echo "- API health endpoint is not responding correctly. Check the API container logs and make sure it's running."
  echo "  docker logs \$(docker ps -q -f name=api)"
  echo "  docker restart \$(docker ps -q -f name=api)"
fi

if [ "$GEOJSON_STATUS" != "OK (200)" ]; then
  echo "- GeoJSON endpoint is not responding correctly. This is likely why your map isn't displaying data."
  echo "  You can fix this by adding this to your frontend's nginx config:"
  echo "  location /housing_api/ {"
  echo "      proxy_pass http://bay_area_housing_api:8000/;"
  echo "      # Note the trailing slash in the proxy_pass URL"
  echo "  }"
fi

echo -e "\nDiagnostics complete. Check the results above for potential issues."