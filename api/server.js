const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');

// Initialize Express app
const app = express();
const port = process.env.PORT || 8000;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Ensure connection
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
  
  fs.writeFileSync(dataPath, JSON.stringify({ lastUpdate: timestamp }));
  return timestamp;
};

// Get last update timestamp
const getLastUpdateTimestamp = () => {
  try {
    const dataPath = path.join(__dirname, 'data', 'last_update.json');
    
    if (fs.existsSync(dataPath)) {
      const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
      return data.lastUpdate;
    }
    
    // If file doesn't exist, create it with current time
    return saveUpdateTimestamp();
  } catch (error) {
    console.error('Error getting update timestamp:', error);
    return getCurrentTimestamp();
  }
};

// API routes
app.get('/housing_api/status', (req, res) => {
  res.json({
    status: 'ok',
    lastUpdate: getLastUpdateTimestamp()
  });
});

// Get all zipcodes with ratings
app.get('/housing_api/zipcodes', async (req, res) => {
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
    res.status(500).json({ error: 'Database error' });
  }
});

// Get zipcode GeoJSON
// Get zipcode GeoJSON
app.get('/housing_api/zipcodes/geojson', async (req, res) => {
  try {
    // Get zipcode boundaries
    const zipcodesResult = await pool.query(`
      SELECT z.zip, z.name, z.county, z.state, 
             ST_AsGeoJSON(z.geometry) as geom
      FROM zipcodes z
    `);
    
    // Early check for empty data
    if (zipcodesResult.rows.length === 0) {
      return res.status(404).json({ 
        error: 'No zipcode data found. Please run the data loader script.' 
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
app.get('/housing_api/zipcodes/:zipcode', async (req, res) => {
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

// Trigger data update
app.post('/housing_api/update-data', (req, res) => {
  console.log('Data update requested');
  
  // Execute data processor script
  exec('python /app/processor/update_data.py', (error, stdout, stderr) => {
    if (error) {
      console.error('Error updating data:', error);
      console.error(stderr);
      return res.status(500).json({ error: 'Update process failed' });
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
app.get('/housing_api/last-update', (req, res) => {
  res.json({
    lastUpdate: getLastUpdateTimestamp()
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// Start the server
app.listen(port, () => {
  console.log(`API server running on port ${port}`);
  
  // Ensure data directory exists
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  
  // Initialize last update timestamp if not exists
  getLastUpdateTimestamp();
});
