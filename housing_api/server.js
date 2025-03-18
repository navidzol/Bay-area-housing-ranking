const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

// Initialize Express app
const app = express();

// Get port from environment variable, defaulting to 8000
const port = process.env.PORT || 8000;

// Middleware
app.use(cors());
app.use(express.json());

// Path prefix for API routes - handle both paths with and without prefix
const PATH_PREFIX = '/housing_api';

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 
    `postgres://${process.env.POSTGRES_USER}:${process.env.POSTGRES_PASSWORD}@${process.env.POSTGIS_HOST}:${process.env.POSTGRES_PORT}/${process.env.POSTGRES_DB_NAME}`,
});

// Test database connection on startup
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Database connection error:', err);
  } else {
    console.log('Database connected at:', res.rows[0].now);
  }
});

// Helper to get current timestamp
const getCurrentTimestamp = () => {
  const now = new Date();
  return now.toISOString();
};

// Save update timestamp
const saveUpdateTimestamp = () => {
  const timestamp = getCurrentTimestamp();
  const dataPath = path.join(__dirname, 'data', 'last_update.json');
  
  // Create directory if it doesn't exist
  fs.mkdirSync(path.dirname(dataPath), { recursive: true });
  
  fs.writeFileSync(dataPath, JSON.stringify({ lastUpdate: timestamp }));
  return timestamp;
};

// Get last update timestamp
const getLastUpdateTimestamp = () => {
  try {
    const dataPath = path.join(__dirname, 'data', 'last_update.json');
    
    if (fs.existsSync(dataPath)) {
      try {
        const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
        return data.lastUpdate;
      } catch (parseError) {
        console.error('Error parsing timestamp file, creating new one:', parseError);
        // If parsing fails, create a new file with current timestamp
        return saveUpdateTimestamp();
      }
    }
    
    // If file doesn't exist, create it with current time
    return saveUpdateTimestamp();
  } catch (error) {
    console.error('Error getting update timestamp:', error);
    return getCurrentTimestamp();
  }
};

// API routes - handle both prefixed and non-prefixed routes
// Health endpoint
app.get([`${PATH_PREFIX}/health`, '/health'], (req, res) => {
  res.json({
    status: 'ok',
    lastUpdate: getLastUpdateTimestamp(),
    database: 'connected'
  });
});

// Status endpoint
app.get([`${PATH_PREFIX}/status`, '/status'], (req, res) => {
  pool.query('SELECT COUNT(*) as zipcode_count FROM zipcodes', (err, result) => {
    if (err) {
      console.error('Database query error:', err);
      return res.status(500).json({ 
        status: 'error', 
        message: 'Database error', 
        error: err.message
      });
    }
    
    const zipcodeCount = result.rows[0]?.zipcode_count || 0;
    
    res.json({
      status: 'ok',
      lastUpdate: getLastUpdateTimestamp(),
      zipcodeCount: zipcodeCount
    });
  });
});

// Get all zipcodes with ratings
app.get([`${PATH_PREFIX}/zipcodes`, '/zipcodes'], async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT z.zip, z.name, z.county, z.state, 
             json_agg(json_build_object(
               'type', zr.rating_type, 
               'value', zr.rating_value, 
               'confidence', zr.confidence
             )) as ratings
      FROM zipcodes z
      LEFT JOIN zipcode_ratings zr ON z.zip = zr.zip
      GROUP BY z.zip, z.name, z.county, z.state
    `);
    
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching zipcodes:', error);
    res.status(500).json({ error: 'Database error', message: error.message });
  }
});

// Get zipcode GeoJSON
app.get([`${PATH_PREFIX}/zipcodes/geojson`, '/zipcodes/geojson'], async (req, res) => {
  try {
    console.log("Fetching zipcode GeoJSON");
    // Get zipcode boundaries
    const zipcodesResult = await pool.query(`
      SELECT z.zip, z.name, z.county, z.state, 
             ST_AsGeoJSON(z.geometry) as geom
      FROM zipcodes z
    `);
    console.log("Query completed. Rows returned:", zipcodesResult.rows.length);
    if (zipcodesResult.rows.length > 0) {
      console.log("Sample zipcode:", zipcodesResult.rows[0].zip);
    } else {
      console.log("No zipcodes found in database");
    }

    // Early check for empty data
    if (zipcodesResult.rows.length === 0) {
      console.error("No zipcode data found. Data initialization may have failed.");
      return res.status(404).json({ 
        error: 'No zipcode data found. The system will attempt to load the data automatically. Please try again in a few minutes.' 
      });
    }
    
    // Get zipcode ratings
    const ratingsResult = await pool.query(`
      SELECT zip, rating_type, rating_value, confidence
      FROM zipcode_ratings
    `);
    
    // Organize ratings by zipcode
    const ratingsByZipcode = {};
    ratingsResult.rows.forEach(row => {
      if (!ratingsByZipcode[row.zip]) {
        ratingsByZipcode[row.zip] = {};
      }
      ratingsByZipcode[row.zip][row.rating_type] = row.rating_value;
    });
    
    // Create GeoJSON structure
    const geojson = {
      type: 'FeatureCollection',
      features: zipcodesResult.rows.map(row => {
        // Get all ratings for this zipcode
        const ratings = ratingsByZipcode[row.zip] || {};
        
        return {
          type: 'Feature',
          properties: {
            ZIP: row.zip,
            NAME: row.name,
            county: row.county,
            state: row.state,
            // Include all ratings as properties
            schoolRating: ratings.schoolRating || null,
            crimeRate: ratings.crimeRate || null,
            nicheRating: ratings.nicheRating || null,
            commuteTime: ratings.commuteTime || null,
            neighborhoodRating: ratings.neighborhoodRating || null
          },
          geometry: JSON.parse(row.geom)
        };
      })
    };
    
    res.json(geojson);
  } catch (error) {
    console.error('Error fetching zipcode GeoJSON:', error);
    res.status(500).json({ error: 'Database error: ' + error.message });
  }
});

// Get a specific zipcode with ratings
app.get([`${PATH_PREFIX}/zipcodes/:zipcode`, '/zipcodes/:zipcode'], async (req, res) => {
  try {
    const zipcode = req.params.zipcode;
    
    const result = await pool.query(`
      SELECT z.zip, z.name, z.county, z.state, 
             json_agg(json_build_object(
               'type', zr.rating_type, 
               'value', zr.rating_value, 
               'confidence', zr.confidence
             )) as ratings
      FROM zipcodes z
      LEFT JOIN zipcode_ratings zr ON z.zip = zr.zip
      WHERE z.zip = $1
      GROUP BY z.zip, z.name, z.county, z.state
    `, [zipcode]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Zipcode not found' });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    console.error('Error fetching zipcode:', error);
    res.status(500).json({ error: 'Database error' });
  }
});

// Trigger data update - improved with better error handling
app.post([`${PATH_PREFIX}/update-data`, '/update-data'], (req, res) => {
  console.log('Data update requested');
  
  // Path to the update script
  const processorPath = process.env.PROCESSOR_PATH || '/app/processor';
  const updateScript = path.join(processorPath, 'update_data.py');
  
  if (!fs.existsSync(updateScript)) {
    return res.status(500).json({ 
      error: 'Update script not found',
      path: updateScript,
      message: 'The processor scripts may not be properly mounted in the container'
    });
  }
  
  // Execute data processor script
  exec(`python ${updateScript}`, (error, stdout, stderr) => {
    if (error) {
      console.error('Error updating data:', error);
      console.error(stderr);
      return res.status(500).json({ 
        error: 'Update process failed',
        message: error.message,
        stderr: stderr
      });
    }
    
    console.log('Data update successful:', stdout);
    
    // Update timestamp
    const timestamp = saveUpdateTimestamp();
    
    res.json({
      status: 'success',
      message: 'Data updated successfully',
      lastUpdate: timestamp
    });
  });
});

// Get the last update timestamp
app.get([`${PATH_PREFIX}/last-update`, '/last-update'], (req, res) => {
  res.json({
    lastUpdate: getLastUpdateTimestamp()
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ 
    error: 'Something went wrong!',
    message: err.message
  });
});

// Start the server
app.listen(port, () => {
  console.log(`API server running on port ${port}`);
  console.log(`API endpoints are available at both /endpoint and ${PATH_PREFIX}/endpoint`);
  
  // Ensure data directory exists
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  
  // Initialize last update timestamp if not exists
  getLastUpdateTimestamp();
});