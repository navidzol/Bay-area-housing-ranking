#!/bin/bash
set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print section header
section() {
  echo -e "\n${BLUE}==== $1 ====${NC}"
}

# Print success message
success() {
  echo -e "${GREEN}✓ $1${NC}"
}

# Print error message
error() {
  echo -e "${RED}✗ $1${NC}"
}

# Print warning message
warning() {
  echo -e "${YELLOW}! $1${NC}"
}

# Define environment (development or production)
ENVIRONMENT=${DEPLOYMENT_ENV:-development}
section "Bay Area Housing Criteria Map Deployment"
echo "Deploying in $ENVIRONMENT mode"

# Check that Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
  error "Docker is not installed. Please install Docker first."
  exit 1
fi

if ! docker compose version &> /dev/null; then
  warning "Using legacy docker-compose. Consider upgrading to Docker Compose V2."
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
  success "Using Docker Compose V2"
fi

# Check if .env file exists, create if not
if [ ! -f .env ]; then
    warning "Creating .env file with default values..."
    cat > .env << EOL
# Database Configuration
POSTGRES_USER=bayarea_housing
POSTGRES_PASSWORD=$(openssl rand -base64 12)
POSTGRES_DB_NAME=bayarea_housing_db
POSTGRES_PORT=5433
API_PORT=8000
WEB_PORT=8083

# Deployment Environment
DEPLOYMENT_ENV=${ENVIRONMENT}
NODE_ENV=production

# Census API 
CENSUS_API_KEY=""
CENSUS_ZCTA_URL="https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/tl_2020_us_zcta520.zip"
CENSUS_ACS_BASE_URL="https://api.census.gov/data"
EOL
    success ".env file created with secure random password."
else
    success ".env file exists, using existing configuration."
fi

# Load environment variables
source .env

# Check required directories exist
section "Checking directory structure"

# Create directories if they don't exist
mkdir -p frontend/nginx-conf
mkdir -p data_collectors
mkdir -p processor
mkdir -p housing_api

# Check if nginx config exists, create if not
if [ ! -f frontend/nginx-conf/default.conf ]; then
    warning "Creating nginx configuration..."
    cat > frontend/nginx-conf/default.conf << 'EOL'
server {
    listen 80;
    server_name localhost;
    
    # Improved CSP headers for map tiles and fonts
    add_header Content-Security-Policy "default-src 'self'; connect-src 'self'; font-src 'self' https://fonts.googleapis.com https://fonts.gstatic.com http://fonts.googleapis.com http://fonts.gstatic.com https://cdnjs.cloudflare.com; img-src 'self' data: https://*.tile.openstreetmap.org; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com http://fonts.googleapis.com https://cdnjs.cloudflare.com; worker-src 'self'";
    
    location / {
        root /usr/share/nginx/html;
        index index.html;
        try_files $uri $uri/ /index.html;
    }
    
    # Fix for API proxying - add trailing slash to proxy_pass
    location /housing_api/ {
        proxy_pass http://bay_area_housing_api:8000/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 90;
    }
}
EOL
    success "Nginx configuration created."
fi

# Check available ports
section "Checking port availability"

check_port() {
    local port=$1
    if command -v nc &> /dev/null; then
        if nc -z localhost $port &> /dev/null; then
            warning "Port $port is already in use. You may need to change it in .env file."
            return 1
        fi
    elif command -v lsof &> /dev/null; then
        if lsof -i:$port -sTCP:LISTEN &> /dev/null; then
            warning "Port $port is already in use. You may need to change it in .env file."
            return 1
        fi
    else
        warning "Cannot check if port $port is available. Make sure it's not in use."
        return 0
    fi
    
    success "Port $port is available"
    return 0
}

check_port ${POSTGRES_PORT}
check_port ${API_PORT}
check_port ${WEB_PORT}

# Build and deploy services
section "Building and starting services"

# Check if containers already exist and are running
if $COMPOSE_CMD ps | grep -q "running"; then
    warning "Some containers are already running."
    read -p "Do you want to stop and rebuild all containers? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping existing containers..."
        $COMPOSE_CMD down
    else
        echo "Continuing with existing containers..."
    fi
else
    # Clean up any stopped containers
    $COMPOSE_CMD down
fi

# Build and start the services
echo "Building and starting services..."
$COMPOSE_CMD up -d --build

if [ $? -ne 0 ]; then
    error "Failed to start services. Check the logs with '$COMPOSE_CMD logs'"
    exit 1
fi

# Wait for services to initialize
section "Waiting for services to initialize"
echo "This may take a few minutes for the database to initialize and data to load..."

# Function to wait for container health
wait_for_health() {
    local container=$1
    local max_attempts=$2
    local attempt=1
    
    echo "Waiting for $container to be healthy..."
    
    while [ $attempt -le $max_attempts ]; do
        health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $container 2>/dev/null || echo "not_found")
        
        if [ "$health" == "healthy" ]; then
            success "$container is healthy"
            return 0
        elif [ "$health" == "running" ] && [ $attempt -eq $max_attempts ]; then
            # Container is running but doesn't have a health check
            warning "$container is running but doesn't have a health check"
            return 0
        elif [ "$health" == "not_found" ]; then
            error "$container not found"
            return 1
        fi
        
        echo "Attempt $attempt/$max_attempts: $container status is '$health'. Waiting 10s..."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    warning "$container is not reporting as healthy after $max_attempts attempts, but it might still be working"
    return 0
}

# Wait for services to be ready
wait_for_health "postgis_db" 6
wait_for_health "bay_area_housing_api" 6

# Display service status
section "Service Status"
$COMPOSE_CMD ps

# Test API connection
section "Testing API connection"
if curl -s http://localhost:${API_PORT}/health | grep -q "ok"; then
    success "API is accessible and reporting healthy"
else
    warning "API health check failed. You can check the logs with '$COMPOSE_CMD logs bay_area_housing_api'"
fi

# Create a force-update script
section "Creating maintenance scripts"
cat > force-update.sh << EOL
#!/bin/bash
set -e

echo "Forcing data update by resetting update timestamps..."
docker exec postgis_db psql -U "\${POSTGRES_USER:-${POSTGRES_USER}}" -d "\${POSTGRES_DB_NAME:-${POSTGRES_DB_NAME}}" -c "DELETE FROM data_sources;"

echo "Restarting data processor to trigger update..."
docker restart bay_area_housing_processor

echo "Update process started. Check logs with: docker logs -f bay_area_housing_processor"
EOL
chmod +x force-update.sh

success "Created force-update.sh script to manually trigger data updates"

# Create a view-logs script
cat > view-logs.sh << EOL
#!/bin/bash
if [ "\$1" == "" ]; then
  echo "Usage: ./view-logs.sh [processor|api|db|frontend|all]"
  exit 1
fi

case "\$1" in
  processor)
    docker logs -f bay_area_housing_processor
    ;;
  api)
    docker logs -f bay_area_housing_api
    ;;
  db)
    docker logs -f postgis_db
    ;;
  frontend)
    docker logs -f bay_area_housing_frontend
    ;;
  all)
    docker compose logs -f
    ;;
  *)
    echo "Unknown service: \$1"
    echo "Usage: ./view-logs.sh [processor|api|db|frontend|all]"
    exit 1
    ;;
esac
EOL
chmod +x view-logs.sh

success "Created view-logs.sh script for easy log viewing"

# Provide final instructions
section "Deployment Complete!"
echo -e "Your Bay Area Housing Criteria Map is now deployed and running!"
echo -e "Frontend:  ${GREEN}http://localhost:${WEB_PORT}${NC}"
echo -e "API:       ${GREEN}http://localhost:${API_PORT}/health${NC}"
echo -e "Database:  ${GREEN}PostgreSQL on port ${POSTGRES_PORT}${NC}"
echo
echo -e "Useful commands:"
echo -e "- View logs:            ${YELLOW}./view-logs.sh [processor|api|db|frontend|all]${NC}"
echo -e "- Force data update:    ${YELLOW}./force-update.sh${NC}"
echo -e "- Stop all services:    ${YELLOW}$COMPOSE_CMD down${NC}"
echo -e "- Start all services:   ${YELLOW}$COMPOSE_CMD up -d${NC}"
echo -e "- Restart a service:    ${YELLOW}$COMPOSE_CMD restart [service-name]${NC}"
echo
echo -e "If you encounter issues, check the logs and ensure all ports are available."