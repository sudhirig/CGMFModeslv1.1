/**
 * Investigate MFAPI connectivity patterns and find working scheme codes
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function investigateMFAPIConnectivity() {
  try {
    console.log('Investigating MFAPI connectivity patterns...\n');
    
    // 1. Check funds that already have NAV data - what scheme codes work?
    const workingFunds = await pool.query(`
      SELECT DISTINCT f.scheme_code, f.fund_name, COUNT(nd.fund_id) as nav_records
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE f.scheme_code IS NOT NULL
      GROUP BY f.scheme_code, f.fund_name
      ORDER BY nav_records DESC
      LIMIT 10
    `);
    
    console.log('=== Working Scheme Codes (with NAV data) ===');
    for (const fund of workingFunds.rows) {
      console.log(`${fund.scheme_code}: ${fund.fund_name} (${fund.nav_records} records)`);
    }
    
    // 2. Test a few working scheme codes
    console.log('\n=== Testing Working Scheme Codes ===');
    for (let i = 0; i < Math.min(3, workingFunds.rows.length); i++) {
      const fund = workingFunds.rows[i];
      await testSchemeCode(fund.scheme_code, fund.fund_name);
    }
    
    // 3. Check sample of problematic funds
    const problematicFunds = await pool.query(`
      SELECT f.scheme_code, f.fund_name
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE nd.fund_id IS NULL
      AND f.scheme_code IS NOT NULL
      ORDER BY f.id
      LIMIT 5
    `);
    
    console.log('\n=== Testing Problematic Scheme Codes ===');
    for (const fund of problematicFunds.rows) {
      await testSchemeCode(fund.scheme_code, fund.fund_name);
    }
    
    // 4. Try some known working scheme codes from MFAPI documentation
    console.log('\n=== Testing Known MFAPI Examples ===');
    const knownCodes = ['120503', '118989', '120716']; // Common working codes
    for (const code of knownCodes) {
      await testSchemeCode(code, `Known working code ${code}`);
    }
    
  } catch (error) {
    console.error('Error investigating MFAPI connectivity:', error);
  } finally {
    await pool.end();
  }
}

async function testSchemeCode(schemeCode, fundName) {
  try {
    console.log(`Testing ${schemeCode}: ${fundName}`);
    
    const url = `https://api.mfapi.in/mf/${schemeCode}`;
    const response = await axios.get(url, {
      timeout: 10000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Fund-Analysis/1.0)',
        'Accept': 'application/json'
      }
    });
    
    if (response.data && response.data.data) {
      const dataCount = response.data.data.length;
      console.log(`  ✓ SUCCESS: ${dataCount} records available`);
      
      if (dataCount > 0) {
        const latest = response.data.data[0];
        console.log(`  Latest: ${latest.date} = ${latest.nav}`);
      }
      return true;
    } else {
      console.log(`  ⚠ UNEXPECTED FORMAT: ${JSON.stringify(response.data).substring(0, 100)}...`);
      return false;
    }
    
  } catch (error) {
    if (error.response) {
      console.log(`  ✗ HTTP ${error.response.status}: ${error.response.statusText}`);
    } else if (error.code === 'ENOTFOUND') {
      console.log(`  ✗ DNS ERROR: Cannot resolve api.mfapi.in`);
    } else if (error.code === 'ECONNREFUSED') {
      console.log(`  ✗ CONNECTION REFUSED: Service unavailable`);
    } else {
      console.log(`  ✗ ERROR: ${error.message}`);
    }
    return false;
  }
}

investigateMFAPIConnectivity();