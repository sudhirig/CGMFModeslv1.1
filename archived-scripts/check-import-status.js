/**
 * Check status of funds needing historical data and test MFAPI connectivity
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function checkImportStatus() {
  try {
    // Check funds without NAV data
    const noNavFunds = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN f.scheme_code IS NOT NULL THEN 1 END) as funds_with_scheme_code
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE nd.fund_id IS NULL
    `);
    
    console.log('Funds needing historical data:');
    console.log(`Total: ${noNavFunds.rows[0].total_funds}`);
    console.log(`With scheme codes: ${noNavFunds.rows[0].funds_with_scheme_code}`);
    
    // Test MFAPI connectivity with a known fund
    console.log('\nTesting MFAPI connectivity...');
    try {
      const testResponse = await axios.get('https://api.mfapi.in/mf/120503', {
        timeout: 5000
      });
      
      if (testResponse.data && testResponse.data.data) {
        console.log('✓ MFAPI is accessible');
        console.log(`Sample data points available: ${testResponse.data.data.length}`);
        
        // Show sample data structure
        if (testResponse.data.data.length > 0) {
          const sample = testResponse.data.data[0];
          console.log(`Sample record: Date=${sample.date}, NAV=${sample.nav}`);
        }
      } else {
        console.log('⚠ MFAPI response format unexpected');
      }
    } catch (apiError) {
      console.log('✗ MFAPI connectivity issue:', apiError.message);
      console.log('This may require additional API access or credentials');
    }
    
  } catch (error) {
    console.error('Error checking import status:', error);
  } finally {
    await pool.end();
  }
}

checkImportStatus();