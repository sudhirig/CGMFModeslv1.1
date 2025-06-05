// Script to clear existing NAV data and trigger a fresh import with varied values
import pg from 'pg';
import axios from 'axios';

const { Pool } = pg;

async function clearAndReimportNavData() {
  try {
    console.log('Starting clean NAV data reimport process...');
    
    // Connect to database
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    
    // First, stop any running imports
    console.log('Stopping any running imports...');
    await axios.get('http://localhost:5000/api/amfi/stop-scheduled-import?type=all');
    
    // Clear all existing NAV data
    console.log('Clearing existing NAV data...');
    await pool.query('TRUNCATE TABLE nav_data');
    console.log('✓ NAV data cleared successfully');
    
    // Start a new historical import with our improved code (36 months)
    console.log('Starting new historical NAV import with 36 months of data...');
    await axios.post('http://localhost:5000/api/historical-nav/import', { months: 36 });
    
    console.log('✓ Import initiated successfully');
    console.log('The import will run in the background. Check ETL status for progress.');
    
    // Close the pool
    await pool.end();
  } catch (error) {
    console.error('Error during clear and reimport process:', error);
  }
}

// Run the process
clearAndReimportNavData();