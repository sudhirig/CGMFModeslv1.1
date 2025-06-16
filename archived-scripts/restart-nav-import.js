import axios from 'axios';

// Use the API directly in the current Replit environment
const API_BASE_URL = '';

/**
 * Script to restart the NAV data import process
 */
async function restartNavImport() {
  try {
    console.log('Stopping any existing scheduled imports...');
    await axios.get('/api/amfi/stop-scheduled-import?type=all');
    
    console.log('Waiting for 3 seconds...');
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    console.log('Starting new import with a smaller sample of funds...');
    // Pass a smaller limit to reduce the number of funds processed
    const response = await axios.post('/api/import-historical-nav/import', {
      months: 6  // Use 6 months instead of the default 12 for faster processing
    });
    
    console.log('Import restart initiated:', response.data);
    
    return response.data;
  } catch (error) {
    console.error('Error restarting NAV import:', error.message);
    console.error('Response data:', error.response?.data);
    
    throw error;
  }
}

// Execute the restart
restartNavImport()
  .then(result => {
    console.log('NAV import successfully restarted');
  })
  .catch(error => {
    console.error('Failed to restart NAV import');
  });