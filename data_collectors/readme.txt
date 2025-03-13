# Bay Area Housing Criteria Map

An interactive web application that visualizes Bay Area zipcodes based on various housing criteria including school district rankings, crime rates, commute times, and Niche.com neighborhood ratings.

## Features

- Interactive map visualization with color coding by criteria score
- Customizable criteria selection with weighting adjustments
- Detailed zipcode information display
- Data update functionality
- Docker deployment for easy self-hosting

## Project Structure

```
bay-area-housing-map/
├── api/                # Node.js backend API
│   ├── Dockerfile
│   ├── package.json
│   └── server.js
├── processor/          # Python data processing scripts
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── init_db.py
│   └── update_data.py
├── frontend/           # Web frontend
│   ├── Dockerfile
│   ├── public/
│   │   ├── index.html  # Main HTML file
│   │   └── ...         # Other static assets
│   └── nginx.conf      # Nginx configuration
├── docker-compose.yml  # Docker Compose configuration
└── README.md           # This file
```

## Prerequisites

- Docker and Docker Compose
- A running PostGIS database (see Setup Instructions)

## Setup Instructions

### 1. Prerequisites

Ensure you have Docker and Docker Compose installed on your system. This application requires a PostgreSQL database with PostGIS extension, which is expected to be running in a separate Docker Compose setup.

### 2. Environment Configuration

Create a `.env` file with database connection details:

```env
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=bay_area_housing
```

### 3. Directory Structure

Create the project structure as shown above:

```bash
mkdir -p bay-area-housing-map/{api,processor,frontend/public}
```

### 4. Copy Files

Copy all the configuration files and scripts to their respective directories:

- Copy frontend code to `frontend/public/`
- Copy API code to `api/`
- Copy processor scripts to `processor/`
- Place docker-compose.yml in the root directory

### 5. Build and Run

Run the following command from the project root directory:

```bash
docker-compose up -d
```

This will build and start all the required containers.

### 6. Accessing the Application

Once the containers are running, you can access the application at:

```
http://localhost
```

## Data Updates

The application automatically checks for data updates when loaded. You can manually trigger an update by clicking the "Refresh Data" button in the application header.

## Connecting to Your Existing PostGIS Database

This project is designed to use an existing PostGIS database. Make sure your existing database setup:

1. Has the PostGIS extension installed
2. Is accessible from your Docker containers via the network name `postgis_network`
3. Has the database credentials specified in your .env file

The connection between this application and your existing PostGIS database is established through the shared Docker network `postgis_network`. Since your existing database is already running in a separate Docker Compose stack, our docker-compose.yml is configured to connect to that network.

## Database Schema

The application requires the following database schema:

1. `zipcodes` table: Stores zipcode boundaries and basic information
2. `zipcode_ratings` table: Stores various ratings for each zipcode
3. `data_sources` table: Tracks when data sources were last updated

These tables are automatically created by the initialization script if they don't exist.

## Custom Data Sources

The application can be extended to use additional data sources by:

1. Creating a new data fetcher class in the processor code
2. Adding the corresponding update logic to the `update_data.py` script
3. Updating the API to expose the new data

## Troubleshooting

### Connection Issues

If the application cannot connect to your database, check:

1. The network name in your docker-compose.yml matches your PostgreSQL network
2. Database credentials in the .env file are correct
3. PostgreSQL is configured to allow connections from other containers

### Data Not Showing

If the map shows but no data appears:

1. Check the browser console for API errors
2. Verify database tables have been created correctly
3. Ensure the data processor has run successfully (check container logs)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Leaflet](https://leafletjs.com/) for the interactive mapping functionality
- [D3.js](https://d3js.org/) for data visualization
- [OpenStreetMap](https://www.openstreetmap.org/) for map tiles
- [Niche.com](https://www.niche.com/) for neighborhood data