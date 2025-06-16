/**
 * Analyze scheme code mismatch between our database and AMFI/MFAPI sources
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function analyzeSchemeCodeMismatch() {
  try {
    console.log('Analyzing scheme code patterns and data sources...\n');
    
    // 1. Check our database scheme codes
    const ourCodes = await pool.query(`
      SELECT scheme_code, fund_name
      FROM funds
      WHERE scheme_code IS NOT NULL
      ORDER BY id
      LIMIT 10
    `);
    
    console.log('=== Our Database Scheme Codes ===');
    ourCodes.rows.forEach(fund => {
      console.log(`${fund.scheme_code}: ${fund.fund_name}`);
    });
    
    // 2. Get AMFI data format
    console.log('\n=== Fetching AMFI Data Sample ===');
    const amfiResponse = await axios.get('https://www.amfiindia.com/spages/NAVAll.txt', {
      timeout: 15000
    });
    
    const amfiLines = amfiResponse.data.split('\n').slice(0, 20);
    console.log('AMFI format sample:');
    amfiLines.forEach((line, i) => {
      if (line.includes(';') && !line.includes('Scheme Code')) {
        console.log(`  ${i}: ${line.substring(0, 120)}...`);
      }
    });
    
    // 3. Try alternative matching strategies
    console.log('\n=== Testing Alternative Matching Strategies ===');
    
    // Test fund name matching
    const testFund = ourCodes.rows[0];
    await tryFundNameMatching(testFund, amfiResponse.data);
    
    // 4. Check if we have funds that work with MFAPI
    console.log('\n=== Checking MFAPI-compatible funds ===');
    const mfapiCompatible = await pool.query(`
      SELECT f.scheme_code, f.fund_name, COUNT(nd.fund_id) as nav_count
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE f.scheme_code IS NOT NULL
      GROUP BY f.scheme_code, f.fund_name
      HAVING COUNT(nd.fund_id) > 100
      ORDER BY nav_count DESC
      LIMIT 5
    `);
    
    console.log('Funds with substantial MFAPI data:');
    for (const fund of mfapiCompatible.rows) {
      console.log(`${fund.scheme_code}: ${fund.fund_name} (${fund.nav_count} records)`);
      
      // Test this working code
      try {
        const testUrl = `https://api.mfapi.in/mf/${fund.scheme_code}`;
        const testResponse = await axios.get(testUrl, { timeout: 5000 });
        if (testResponse.data && testResponse.data.data) {
          console.log(`  ✓ MFAPI confirms ${testResponse.data.data.length} records available`);
        }
      } catch (error) {
        console.log(`  ✗ MFAPI test failed: ${error.message}`);
      }
    }
    
    // 5. Suggest solution
    console.log('\n=== Recommended Solution ===');
    console.log('1. Use working MFAPI scheme codes to import historical data for funds that have it');
    console.log('2. For new funds without MFAPI data, use AMFI current NAV and build historical data over time');
    console.log('3. Focus on the 694 funds ready for scoring first');
    
  } catch (error) {
    console.error('Error in analysis:', error);
  } finally {
    await pool.end();
  }
}

async function tryFundNameMatching(testFund, amfiData) {
  console.log(`Trying to match: "${testFund.fund_name}"`);
  
  const lines = amfiData.split('\n');
  let found = false;
  
  for (const line of lines) {
    if (line.includes(';') && line.toLowerCase().includes(testFund.fund_name.toLowerCase().substring(0, 20))) {
      console.log(`  Potential match: ${line.substring(0, 120)}...`);
      found = true;
      break;
    }
  }
  
  if (!found) {
    console.log('  No name-based match found in AMFI data');
  }
}

analyzeSchemeCodeMismatch();