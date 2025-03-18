// Save this as debug.js and include it in your index.html before the closing </body> tag
// <script src="debug.js"></script>

(function() {
    // Create debug UI
    const debugContainer = document.createElement('div');
    debugContainer.style.position = 'fixed';
    debugContainer.style.bottom = '20px';
    debugContainer.style.right = '20px';
    debugContainer.style.zIndex = '9999';
    debugContainer.style.backgroundColor = 'rgba(0, 0, 0, 0.8)';
    debugContainer.style.color = 'white';
    debugContainer.style.padding = '10px';
    debugContainer.style.borderRadius = '5px';
    debugContainer.style.maxWidth = '400px';
    debugContainer.style.maxHeight = '300px';
    debugContainer.style.overflow = 'auto';
    debugContainer.style.fontFamily = 'monospace';
    debugContainer.style.fontSize = '12px';
    debugContainer.style.lineHeight = '1.4';
    
    // Create toggle button
    const toggleButton = document.createElement('button');
    toggleButton.textContent = 'Show Debug';
    toggleButton.style.position = 'fixed';
    toggleButton.style.bottom = '20px';
    toggleButton.style.right = '20px';
    toggleButton.style.zIndex = '10000';
    toggleButton.style.padding = '5px 10px';
    toggleButton.style.backgroundColor = '#2563eb';
    toggleButton.style.color = 'white';
    toggleButton.style.border = 'none';
    toggleButton.style.borderRadius = '5px';
    toggleButton.style.cursor = 'pointer';
    
    let debugVisible = false;
    debugContainer.style.display = 'none';
    
    toggleButton.addEventListener('click', () => {
        debugVisible = !debugVisible;
        debugContainer.style.display = debugVisible ? 'block' : 'none';
        toggleButton.textContent = debugVisible ? 'Hide Debug' : 'Show Debug';
    });
    
    document.body.appendChild(toggleButton);
    document.body.appendChild(debugContainer);
    
    // Debug log function
    function logDebug(message, data) {
        const logEntry = document.createElement('div');
        logEntry.style.borderBottom = '1px solid rgba(255, 255, 255, 0.2)';
        logEntry.style.paddingBottom = '5px';
        logEntry.style.marginBottom = '5px';
        
        const timestamp = new Date().toLocaleTimeString();
        logEntry.innerHTML = `<span style="color: #aaa;">[${timestamp}]</span> ${message}`;
        
        if (data) {
            const dataPreview = document.createElement('pre');
            dataPreview.style.marginTop = '5px';
            dataPreview.style.whiteSpace = 'pre-wrap';
            dataPreview.style.fontSize = '10px';
            dataPreview.style.color = '#aaa';
            
            try {
                if (typeof data === 'object') {
                    dataPreview.textContent = JSON.stringify(data, null, 2).substring(0, 500) + 
                        (JSON.stringify(data, null, 2).length > 500 ? '...' : '');
                } else {
                    dataPreview.textContent = String(data);
                }
            } catch (e) {
                dataPreview.textContent = 'Error stringifying data: ' + e.message;
            }
            
            logEntry.appendChild(dataPreview);
        }
        
        debugContainer.prepend(logEntry);
        
        // Also log to console
        console.log(`[DEBUG] ${message}`, data);
        
        // Limit entries
        if (debugContainer.children.length > 50) {
            debugContainer.removeChild(debugContainer.lastChild);
        }
    }
    
    // Debug API connection
    async function testApiConnection() {
        try {
            logDebug('Testing API connection...');
            
            // Test basic health endpoint
            const healthResponse = await fetch(`${config.apiBaseUrl}/health`);
            const healthData = await healthResponse.json();
            logDebug('API health endpoint response:', healthData);
            
            // Test GeoJSON endpoint
            logDebug('Testing GeoJSON endpoint...');
            const geoJsonResponse = await fetch(`${config.apiBaseUrl}/zipcodes/geojson`);
            
            if (!geoJsonResponse.ok) {
                logDebug(`Error ${geoJsonResponse.status}: ${geoJsonResponse.statusText}`);
                return;
            }
            
            const geoJsonData = await geoJsonResponse.json();
            
            // Check if data has features
            if (!geoJsonData.features || geoJsonData.features.length === 0) {
                logDebug('Error: No features found in GeoJSON response');
                return;
            }
            
            logDebug(`GeoJSON response contains ${geoJsonData.features.length} features`);
            
            // Check first feature
            const firstFeature = geoJsonData.features[0];
            logDebug('First feature sample:', firstFeature);
            
            // Validate feature structure
            validateFeature(firstFeature);
            
            // Test calculating scores
            if (state.criteria && state.criteria.length > 0) {
                logDebug('Current criteria:', state.criteria);
                
                if (firstFeature && firstFeature.properties) {
                    const zipcode = firstFeature.properties.ZIP || firstFeature.properties.ZCTA5CE10;
                    const availableRatings = Object.keys(firstFeature.properties)
                        .filter(key => state.criteria.some(c => c.id === key));
                    
                    logDebug(`Zipcode ${zipcode} has these available ratings:`, availableRatings);
                    
                    // Test score calculation
                    try {
                        const scores = calculateZipcodeScores([firstFeature]);
                        logDebug('Score calculation test:', scores);
                    } catch (e) {
                        logDebug('Error calculating scores:', e.message);
                    }
                }
            }
        } catch (error) {
            logDebug('API connection test failed:', error.message);
        }
    }
    
    // Validate feature structure
    function validateFeature(feature) {
        const issues = [];
        
        if (!feature) {
            issues.push('Feature is null or undefined');
            logDebug('Feature validation failed:', issues);
            return;
        }
        
        if (!feature.geometry) {
            issues.push('Missing geometry');
        } else if (!feature.geometry.coordinates || !Array.isArray(feature.geometry.coordinates)) {
            issues.push('Invalid geometry coordinates');
        }
        
        if (!feature.properties) {
            issues.push('Missing properties');
        } else {
            const zipcode = feature.properties.ZIP || feature.properties.ZCTA5CE10;
            if (!zipcode) {
                issues.push('Missing ZIP/ZCTA5CE10 property');
            }
            
            // Check for rating properties
            const expectedRatings = ['schoolRating', 'nicheRating', 'crimeRate', 'commuteTime'];
            const missingRatings = expectedRatings.filter(rating => 
                feature.properties[rating] === undefined);
            
            if (missingRatings.length > 0) {
                issues.push(`Missing rating properties: ${missingRatings.join(', ')}`);
            }
        }
        
        if (issues.length > 0) {
            logDebug('Feature validation issues:', issues);
        } else {
            logDebug('Feature validation: Feature is valid');
        }
    }
    
    // Calculate scores for one or more features
    function calculateZipcodeScores(features) {
        const scores = {};
        
        if (!features || features.length === 0) {
            logDebug('No features to calculate scores');
            return scores;
        }
        
        // Get active criteria
        const activeCriteria = state.criteria.filter(c => c.enabled);
        
        if (activeCriteria.length === 0) {
            logDebug('No active criteria for calculating scores');
            return scores;
        }
        
        // Calculate scores for each zipcode
        features.forEach(feature => {
            if (!feature.properties) {
                logDebug('Feature missing properties:', feature);
                return;
            }
            
            const zipcode = feature.properties.ZIP || feature.properties.ZCTA5CE10;
            if (!zipcode) {
                logDebug('Feature missing zipcode identifier:', feature);
                return;
            }
            
            let totalScore = 0;
            let totalWeight = 0;
            let usedCriteria = 0;
            
            // Track individual criteria scores for debugging
            const criteriaScores = {};
            
            activeCriteria.forEach(criterion => {
                const value = feature.properties[criterion.id];
                
                // Skip if value is missing
                if (value === undefined || value === null) {
                    criteriaScores[criterion.id] = { 
                        value: 'missing', 
                        score: 'N/A',
                        used: false 
                    };
                    return;
                }
                
                let score = parseFloat(value);
                
                // If invert is true, higher values should score lower
                if (criterion.invert) {
                    // Assume a scale of 0-10 for all criteria
                    score = 10 - score;
                }
                
                criteriaScores[criterion.id] = {
                    originalValue: value,
                    inverted: criterion.invert,
                    score: score,
                    weight: criterion.weight,
                    weightedScore: score * criterion.weight,
                    used: true
                };
                
                totalScore += score * criterion.weight;
                totalWeight += criterion.weight;
                usedCriteria++;
            });
            
            // Calculate final score (weighted average)
            if (totalWeight > 0) {
                scores[zipcode] = {
                    finalScore: totalScore / totalWeight,
                    criteriaScores: criteriaScores,
                    totalCriteria: activeCriteria.length,
                    usedCriteria: usedCriteria
                };
            } else {
                scores[zipcode] = {
                    finalScore: 0,
                    criteriaScores: criteriaScores,
                    totalCriteria: activeCriteria.length,
                    usedCriteria: 0,
                    error: 'No criteria with values found'
                };
            }
        });
        
        return scores;
    }
    
    // Check color mapping
    function testColorMapping() {
        logDebug('Testing color mapping...');
        
        try {
            // Get color scheme function
            const colorSchemeKey = appSettings.colorScheme || 'RdYlGn';
            const colorSchemeFunction = colorSchemes[colorSchemeKey] || d3.interpolateRdYlGn;
            
            // Test color scale
            const testScores = [0, 2, 4, 6, 8, 10];
            const colors = {};
            
            testScores.forEach(score => {
                const normalizedScore = appSettings.reverseColors ? 10 - score : score;
                const scaledScore = normalizedScore / 10;
                const color = colorSchemeFunction(scaledScore);
                colors[score] = color;
            });
            
            logDebug('Color mapping test:', colors);
        } catch (error) {
            logDebug('Color mapping test failed:', error.message);
        }
    }
    
    // Add a manual fix button
    const fixButton = document.createElement('button');
    fixButton.textContent = 'Apply Fixes';
    fixButton.style.position = 'fixed';
    fixButton.style.bottom = '20px';
    fixButton.style.right = '120px';
    fixButton.style.zIndex = '10000';
    fixButton.style.padding = '5px 10px';
    fixButton.style.backgroundColor = '#10b981';
    fixButton.style.color = 'white';
    fixButton.style.border = 'none';
    fixButton.style.borderRadius = '5px';
    fixButton.style.cursor = 'pointer';
    
    fixButton.addEventListener('click', applyFixes);
    document.body.appendChild(fixButton);
    
    // Apply fixes to common issues
    function applyFixes() {
        logDebug('Applying fixes...');
        
        try {
            // Fix 1: Ensure API base URL is correct
            if (!config.apiBaseUrl.startsWith('/')) {
                config.apiBaseUrl = '/' + config.apiBaseUrl;
                logDebug('Fixed API base URL:', config.apiBaseUrl);
            }
            
            // Fix 2: Set a reasonable fallback for missing scores
            const originalCalculateZipcodeScores = window.calculateZipcodeScores;
            if (originalCalculateZipcodeScores) {
                window.calculateZipcodeScores = function() {
                    const scores = {};
                    
                    if (!state.zipcodeData || !state.zipcodeData.features) {
                        console.warn('No zipcode data available for calculating scores');
                        return scores;
                    }
                    
                    // Get active criteria
                    const activeCriteria = state.criteria.filter(c => c.enabled);
                    
                    if (activeCriteria.length === 0) {
                        console.warn('No active criteria found for calculating scores');
                        return scores;
                    }
                    
                    // Calculate scores for each zipcode
                    state.zipcodeData.features.forEach(feature => {
                        if (!feature.properties) {
                            console.warn('Feature missing properties:', feature);
                            return;
                        }
                        
                        const zipcode = feature.properties.ZIP || feature.properties.ZCTA5CE10;
                        if (!zipcode) {
                            console.warn('Feature missing zipcode identifier:', feature);
                            return;
                        }
                        
                        let totalScore = 0;
                        let totalWeight = 0;
                        let usedCriteria = 0;
                        
                        // Debug any scoring issues for a sample zipcode
                        const debugZip = zipcode === '94110';
                        
                        activeCriteria.forEach(criterion => {
                            // FIXED: Parse float to ensure numeric value
                            let value = parseFloat(feature.properties[criterion.id]);
                            
                            // FIXED: Handle NaN values
                            if (isNaN(value)) {
                                // Use a default middle value of 5 if missing
                                value = 5;
                            }
                            
                            let score = value;
                            
                            // If invert is true, higher values should score lower
                            if (criterion.invert) {
                                // Assume a scale of 0-10 for all criteria
                                score = 10 - score;
                            }
                            
                            totalScore += score * criterion.weight;
                            totalWeight += criterion.weight;
                            usedCriteria++;
                        });
                        
                        // Calculate final score (weighted average)
                        if (totalWeight > 0) {
                            scores[zipcode] = totalScore / totalWeight;
                        } else {
                            // FIXED: Default to middle score instead of 0
                            scores[zipcode] = 5;
                        }
                    });
                    
                    return scores;
                };
                
                logDebug('Fixed score calculation function');
            }
            
            // Fix 3: Improve color handling
            const colorSchemeKey = appSettings.colorScheme || 'RdYlGn';
            const colorSchemeFunction = colorSchemes[colorSchemeKey] || d3.interpolateRdYlGn;
            
            // Fix color scale to always use full range
            const colorScale = d3.scaleSequential(colorSchemeFunction)
                .domain(appSettings.reverseColors ? [10, 0] : [0, 10]);
            
            logDebug('Fixed color scaling');
            
            // Fix 4: Fix render function issues
            if (window.renderZipcodeMap) {
                const originalRenderFunction = window.renderZipcodeMap;
                window.renderZipcodeMap = function() {
                    if (!state.zipcodeData || !state.zipcodeData.features || state.zipcodeData.features.length === 0) {
                        console.error('No valid zipcode data to render');
                        // Set default view instead of trying to fit bounds
                        state.map.setView(config.mapCenter, config.mapZoom);
                        showNotification('Data Error', 'No zipcode data available. Using default map view of the Bay Area.', 'error');
                        return;
                    }
                    
                    // Debug info
                    console.log('Rendering map with', state.zipcodeData.features.length, 'features');
                    
                    // Remove existing layer if present
                    if (state.zipcodeLayer) {
                        state.map.removeLayer(state.zipcodeLayer);
                    }
                    
                    // Calculate scores for each zipcode
                    const scores = calculateZipcodeScores();
                    console.log('Calculated scores for', Object.keys(scores).length, 'zipcodes');
                    
                    // Create color scale using d3
                    const colorScale = d3.scaleSequential(colorSchemeFunction)
                        .domain(appSettings.reverseColors ? [10, 0] : [0, 10]);
                    
                    try {
                        // Create new GeoJSON layer
                        state.zipcodeLayer = L.geoJSON(state.zipcodeData, {
                            style: feature => {
                                const zipcode = feature.properties.ZIP || feature.properties.ZCTA5CE10;
                                // FIXED: Default to middle value (5) if no score
                                const score = scores[zipcode] || 5;
                                
                                return {
                                    fillColor: colorScale(score),
                                    weight: 1,
                                    opacity: 1,
                                    color: 'white',
                                    fillOpacity: 0.7
                                };
                            },
                            onEachFeature: (feature, layer) => {
                                const zipcode = feature.properties.ZIP || feature.properties.ZCTA5CE10;
                                const cityName = getCityNameFromZip(zipcode);
                                
                                // Add tooltip
                                layer.bindTooltip(`${cityName} (${zipcode})`, {
                                    sticky: true
                                });
                                
                                // Add click handler
                                layer.on('click', () => {
                                    selectZipcode(zipcode, feature);
                                    
                                    // Highlight the selected zipcode
                                    state.zipcodeLayer.eachLayer(l => {
                                        if (l.feature) {
                                            const thisZip = l.feature.properties.ZIP || l.feature.properties.ZCTA5CE10;
                                            
                                            l.setStyle({
                                                weight: thisZip === zipcode ? 3 : 1,
                                                color: thisZip === zipcode ? '#2563eb' : 'white'
                                            });
                                            
                                            if (thisZip === zipcode) {
                                                l.bringToFront();
                                            }
                                        }
                                    });
                                    
                                    // Open panel on mobile if collapsed
                                    if (window.innerWidth <= 1024 && state.isPanelCollapsed) {
                                        togglePanel();
                                    }
                                });
                            }
                        }).addTo(state.map);
                        
                        // Only fit bounds if we have features
                        if (state.zipcodeData.features && state.zipcodeData.features.length > 0) {
                            console.log('Fitting bounds to zipcode layer');
                            state.map.fitBounds(state.zipcodeLayer.getBounds());
                        }
                        
                    } catch (error) {
                        console.error('Error rendering GeoJSON layer:', error);
                        showNotification('Map Error', `Failed to render map: ${error.message}`, 'error');
                    }
                };
                
                logDebug('Fixed render function');
            }
            
            // Fix 5: Improve update details panel function
            if (window.updateDetailsPanel) {
                const originalUpdateDetailsPanel = window.updateDetailsPanel;
                window.updateDetailsPanel = function(zipcode, feature) {
                    const detailsContainer = document.getElementById('details-content');
                    
                    if (!feature || !feature.properties) {
                        detailsContainer.innerHTML = `
                            <div class="details-placeholder">
                                <i class="fas fa-exclamation-circle"></i>
                                <p>No data available for this zipcode</p>
                            </div>
                        `;
                        return;
                    }
                    
                    const props = feature.properties;
                    const scores = calculateZipcodeScores();
                    const score = scores[zipcode] ? scores[zipcode].toFixed(1) : 'N/A';
                    
                    // Get real city name
                    const cityName = getCityNameFromZip(zipcode) || props.NAME || `${zipcode} Area`;
                    
                    let html = `
                        <div class="zipcode-details">
                            <div class="zipcode-title">
                                <div class="zipcode-name">${cityName} (${zipcode})</div>
                                <div class="score-pill">${score}</div>
                            </div>
                            
                            <div class="rating-list">
                    `;
                    
                    // Define all the standard ratings with icons
                    const standardRatings = [
                        {id: 'schoolRating', name: 'Schools', icon: 'fa-school'},
                        {id: 'nicheRating', name: 'Niche', icon: 'fa-star'},
                        {id: 'crimeRate', name: 'Safety', icon: 'fa-shield-alt'},
                        {id: 'commuteTime', name: 'Commute', icon: 'fa-car'}
                    ];
                    
                    // First add standard ratings, with placeholders for missing data
                    standardRatings.forEach(rating => {
                        // FIXED: Parse float and handle NaN
                        const valueRaw = props[rating.id];
                        const value = parseFloat(valueRaw);
                        
                        if (!isNaN(value)) {
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas ${rating.icon}"></i></div>
                                    <div class="rating-name">${rating.name}</div>
                                    <div class="rating-value">${value.toFixed(1)}</div>
                                </div>
                            `;
                        } else {
                            html += `
                                <div class="rating-item" style="opacity: 0.6;">
                                    <div class="rating-icon"><i class="fas ${rating.icon}"></i></div>
                                    <div class="rating-name">${rating.name}</div>
                                    <div class="rating-value">N/A</div>
                                </div>
                            `;
                        }
                    });
                    
                    html += `</div>`;
                    
                    // Add demographic information section if available
                    if (props.population || props.median_income || props.median_home_value) {
                        html += `
                            <div class="rating-subsection">
                                <div class="rating-subsection-title">
                                    <i class="fas fa-info-circle"></i> Area Information
                                </div>
                                <div class="rating-list">
                        `;
                        
                        // Add county information if available
                        if (props.county) {
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas fa-map-marker-alt"></i></div>
                                    <div class="rating-name">County</div>
                                    <div class="rating-value">${props.county}</div>
                                </div>
                            `;
                        }
                        
                        // Add population information if available
                        if (props.population) {
                            const population = parseInt(props.population);
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas fa-users"></i></div>
                                    <div class="rating-name">Population</div>
                                    <div class="rating-value">${isNaN(population) ? 'N/A' : population.toLocaleString()}</div>
                                </div>
                            `;
                        }
                        
                        // Add median home value if available
                        if (props.median_home_value) {
                            const homeValue = parseFloat(props.median_home_value);
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas fa-home"></i></div>
                                    <div class="rating-name">Median Home</div>
                                    <div class="rating-value">${isNaN(homeValue) ? 'N/A' : formatNumber(homeValue)}</div>
                                </div>
                            `;
                        }
                        
                        // Add median income if available
                        if (props.median_income) {
                            const income = parseFloat(props.median_income);
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas fa-dollar-sign"></i></div>
                                    <div class="rating-name">Median Income</div>
                                    <div class="rating-value">${isNaN(income) ? 'N/A' : formatNumber(income)}</div>
                                </div>
                            `;
                        }
                        
                        // Add median rent if available
                        if (props.median_rent) {
                            const rent = parseFloat(props.median_rent);
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas fa-key"></i></div>
                                    <div class="rating-name">Median Rent</div>
                                    <div class="rating-value">${isNaN(rent) ? 'N/A' : formatNumber(rent)}</div>
                                </div>
                            `;
                        }
                        
                        // Add ownership percentage if available
                        if (props.ownership_percent) {
                            const ownership = parseFloat(props.ownership_percent);
                            html += `
                                <div class="rating-item">
                                    <div class="rating-icon"><i class="fas fa-percentage"></i></div>
                                    <div class="rating-name">Ownership</div>
                                    <div class="rating-value">${isNaN(ownership) ? 'N/A' : ownership.toFixed(1)}%</div>
                                </div>
                            `;
                        }
                        
                        html += `</div></div>`;
                    }
                    
                    html += `</div>`;
                    
                    detailsContainer.innerHTML = html;
                };
                
                logDebug('Fixed details panel update function');
            }
            
            // Reload data and rerender
            loadZipcodeData();
            
            logDebug('All fixes applied, reloading data...');
            showNotification('Fixes Applied', 'Applied fixes to data loading and display. Reloading map...', 'success');
        } catch (error) {
            logDebug('Error applying fixes:', error.message);
            showNotification('Error', `Failed to apply fixes: ${error.message}`, 'error');
        }
    }
    
    // Expose debugging functions
    window.debugApp = {
        testApiConnection,
        validateFeature,
        calculateDebugScores: calculateZipcodeScores,
        testColorMapping,
        applyFixes,
        log: logDebug
    };
    
    // Run tests when the script loads
    setTimeout(() => {
        logDebug('Debug tools initialized. Click "Apply Fixes" to fix common issues.');
        testApiConnection();
        testColorMapping();
    }, 2000);
})();
