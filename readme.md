# Bay Area Housing Criteria Map

An interactive web application that visualizes Bay Area ZIP codes based on various housing criteria including school district rankings, crime rates, commute times, and neighborhood ratings.

## Overview

This project provides a complete solution for visualizing housing data across the Bay Area, allowing users to explore neighborhoods based on customizable criteria. The system includes:

- Interactive map visualization with color coding by criteria score
- Customizable criteria selection with weighting adjustments
- Detailed zipcode information display
- Automated data collection from multiple sources
- Docker-based deployment for easy installation

## System Architecture

The application uses a microservices architecture with the following components:

1. **PostGIS Database**: Stores geographic data, ratings, and other neighborhood information
2. **Data Processor**: Python-based services that collect and process data from various sources
3. **Node.js API**: Serves data to the frontend and handles user requests
4. **Web Frontend**: Interactive map interface built with HTML, CSS, and JavaScript

## Prerequisites

- Docker and Docker Compose
- 2GB+ of available RAM
- 1GB+ of available storage
- Available ports (configurable):
  - 5433 (PostgreSQL)
  - 8000 (API)
  - 8083 (Web)

## Quick Start

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/bay-area-housing-map.git
   cd bay-area-housing-map
   ```

2. Run the deployment script:
   ```bash
   ./deploy.sh
   ```

3. Access the application at http://localhost:8083

## Configuration

The application can be configured using environment variables in the `.env` file:

```
# Database Configuration
POSTGRES_USER=bayarea_housing
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB_NAME=bayarea_housing_db
POSTGRES_PORT=5433
API_PORT=8000
WEB_PORT=8083

# Deployment Environment
DEPLOYMENT_ENV=development
NODE_ENV=production

# Census API Key (optional but recommended)
CENSUS_API_KEY=your_census_api_key
```

## Maintenance and Management

Several scripts are provided to help manage the application:

- `./deploy.sh`: Deploy and start all services
- `./monitor.sh`: Check system status and health
- `./force-update.sh`: Force immediate data updates
- `./view-logs.sh [service]`: View logs for a specific service

## Directory Structure

```
bay-area-housing-map/
├── data_collectors/          # Data collection scripts
│   ├── census-api-collector.py
│   ├── crime-data-collector.py
│   ├── database-schema.sql
│   └── ...
├── processor/                # Data processing scripts
│   ├── check_and_fix_data.py
│   ├── init_db.py
│   ├── load_zipcode_data.py
│   └── ...
├── housing_api/              # Node.js API server
│   ├── ApiDockerfile
│   ├── server.js
│   └── ...
├── frontend/                 # Web frontend
│   ├── index.html
│   └── nginx-conf/
│       └── default.conf
├── docker-compose.yml        # Docker Compose configuration
├── deploy.sh                 # Deployment script
├── monitor.sh                # Monitoring script
├── force-update.sh           # Data update script
└── view-logs.sh              # Log viewing utility
```

## Data Sources

This application collects and integrates data from various sources:

- Census Bureau (demographics, income, housing data)
- Niche.com (neighborhood ratings)
- California Department of Education (school performance)
- Crime data from open data portals
- OpenStreetMap (amenities and location data)

## Troubleshooting

### Database Issues

If the database appears empty or corrupted:

1. Check the database status with `./monitor.sh`
2. Force a data update with `./force-update.sh`
3. Review logs with `./view-logs.sh processor`

### API Not Responding

If the API is not responding:

1. Check the API container status with `docker ps`
2. Restart the API with `docker restart bay_area_housing_api`
3. Review logs with `./view-logs.sh api`

### Web Interface Issues

If the web interface is not loading properly:

1. Ensure the frontend container is running
2. Check Nginx logs with `./view-logs.sh frontend`
3. Verify the API is accessible

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Leaflet](https://leafletjs.com/) for the interactive mapping functionality
- [D3.js](https://d3js.org/) for data visualization
- [OpenStreetMap](https://www.openstreetmap.org/) for map tiles
