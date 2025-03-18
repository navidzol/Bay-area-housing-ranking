#!/bin/bash
# Monitoring script for Bay Area Housing Map application

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Bay Area Housing Criteria Map - System Monitor ===${NC}"
echo -e "Running monitoring checks at $(date)"
echo

# Check containers status
echo -e "${BLUE}Container Status:${NC}"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E 'postgis_db|bay_area_housing_processor|bay_area_housing_api|bay_area_housing_frontend'

echo

# Check disk space
echo -e "${BLUE}Disk Space:${NC}"
df -h | grep -E '(Filesystem|/$)'

echo

# Check Docker volume usage
echo -e "${BLUE}Docker Volume Usage:${NC}"
docker system df -v | grep -E "(VOLUME NAME|postgres_data|api_data|processor_data)"

echo

# Check database status
echo -e "${BLUE}Database Tables Status:${NC}"
if docker exec postgis_db psql -U "$(grep POSTGRES_USER .env | cut -d '=' -f2)" -d "$(grep POSTGRES_DB_NAME .env | cut -d '=' -f2)" -c '\dt' > /dev/null 2>&1; then
    echo -e "${GREEN}Database is accessible${NC}"
    
    # Get table counts
    ZIPCODE_COUNT=$(docker exec postgis_db psql -U "$(grep POSTGRES_USER .env | cut -d '=' -f2)" -d "$(grep POSTGRES_DB_NAME .env | cut -d '=' -f2)" -t -c "SELECT COUNT(*) FROM zipcodes;")
    RATING_COUNT=$(docker exec postgis_db psql -U "$(grep POSTGRES_USER .env | cut -d '=' -f2)" -d "$(grep POSTGRES_DB_NAME .env | cut -d '=' -f2)" -t -c "SELECT COUNT(*) FROM zipcode_ratings;")
    SOURCES_COUNT=$(docker exec postgis_db psql -U "$(grep POSTGRES_USER .env | cut -d '=' -f2)" -d "$(grep POSTGRES_DB_NAME .env | cut -d '=' -f2)" -t -c "SELECT COUNT(*) FROM data_sources;")
    
    echo "ZIPCodes: ${ZIPCODE_COUNT// /}"
    echo "Ratings: ${RATING_COUNT// /}"
    echo "Data Sources: ${SOURCES_COUNT// /}"
    
    # Show data source status
    echo -e "\n${BLUE}Data Update Status:${NC}"
    docker exec postgis_db psql -U "$(grep POSTGRES_USER .env | cut -d '=' -f2)" -d "$(grep POSTGRES_DB_NAME .env | cut -d '=' -f2)" -c "SELECT source_name, to_char(last_updated, 'YYYY-MM-DD HH24:MI') as last_updated, to_char(next_update, 'YYYY-MM-DD HH24:MI') as next_update, update_frequency FROM data_sources ORDER BY next_update;"
else
    echo -e "${RED}Database is not accessible${NC}"
fi

echo

# Check API status
echo -e "${BLUE}API Health Check:${NC}"
if curl -s http://localhost:$(grep API_PORT .env | cut -d '=' -f2)/health > /dev/null; then
    echo -e "${GREEN}API is healthy${NC}"
    
    # Get API details
    API_STATUS=$(curl -s http://localhost:$(grep API_PORT .env | cut -d '=' -f2)/health | jq -r '.')
    echo "$API_STATUS" | jq '.'
else
    echo -e "${RED}API is not responding${NC}"
fi

echo

# Show container resource usage
echo -e "${BLUE}Container Resource Usage:${NC}"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}" | grep -E 'postgis_db|bay_area_housing_processor|bay_area_housing_api|bay_area_housing_frontend'

echo

# Print useful commands
echo -e "${BLUE}Useful Commands:${NC}"
echo -e "- View API logs:         ${YELLOW}docker logs -f bay_area_housing_api${NC}"
echo -e "- View processor logs:   ${YELLOW}docker logs -f bay_area_housing_processor${NC}"
echo -e "- View database logs:    ${YELLOW}docker logs -f postgis_db${NC}"
echo -e "- Force data update:     ${YELLOW}./force-update.sh${NC}"
echo -e "- Restart API:           ${YELLOW}docker restart bay_area_housing_api${NC}"
echo -e "- Restart all services:  ${YELLOW}docker compose restart${NC}"