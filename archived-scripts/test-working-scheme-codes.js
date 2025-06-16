/**
 * Test MFAPI with known working scheme codes vs problematic ones
 */

import axios from 'axios';

async function testWorkingSchemes() {
  console.log('Testing MFAPI connectivity with different scheme code patterns...\n');
  
  // Working scheme codes from our database
  const workingCodes = ['101206', '103140', '102008', '102007', '102009'];
  
  // Problematic codes from funds without data
  const problematicCodes = ['134829', '102668', '357464', '132659', '127297'];
  
  console.log('=== Testing Known Working Codes ===');
  for (const code of workingCodes) {
    await testSingleScheme(code, 'Working');
  }
  
  console.log('\n=== Testing Problematic Codes ===');
  for (const code of problematicCodes) {
    await testSingleScheme(code, 'Problematic');
  }
  
  // Test alternative URL patterns
  console.log('\n=== Testing Alternative MFAPI Endpoints ===');
  await testAlternativeEndpoints('101206');
}

async function testSingleScheme(schemeCode, type) {
  try {
    const url = `https://api.mfapi.in/mf/${schemeCode}`;
    console.log(`${type} ${schemeCode}: ${url}`);
    
    const response = await axios.get(url, {
      timeout: 8000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Fund-Analysis/1.0)',
        'Accept': 'application/json'
      }
    });
    
    if (response.data) {
      if (response.data.data && Array.isArray(response.data.data)) {
        console.log(`  ✓ SUCCESS: ${response.data.data.length} records`);
        if (response.data.data.length > 0) {
          const latest = response.data.data[0];
          console.log(`  Latest: ${latest.date} = ${latest.nav}`);
        }
      } else if (response.data.meta) {
        console.log(`  ⚠ META RESPONSE: ${JSON.stringify(response.data.meta)}`);
      } else {
        console.log(`  ⚠ UNEXPECTED: ${JSON.stringify(response.data).substring(0, 100)}`);
      }
    }
    
  } catch (error) {
    if (error.response) {
      console.log(`  ✗ HTTP ${error.response.status}: ${error.response.data ? JSON.stringify(error.response.data).substring(0, 100) : 'No data'}`);
    } else {
      console.log(`  ✗ ERROR: ${error.message}`);
    }
  }
}

async function testAlternativeEndpoints(schemeCode) {
  const endpoints = [
    `https://api.mfapi.in/mf/${schemeCode}`,
    `https://api.mfapi.in/mf/${schemeCode}/latest`,
    `https://www.amfiindia.com/spages/NAVAll.txt`
  ];
  
  for (const endpoint of endpoints) {
    try {
      console.log(`Testing: ${endpoint}`);
      const response = await axios.get(endpoint, { timeout: 5000 });
      console.log(`  ✓ Status: ${response.status}, Data type: ${typeof response.data}`);
      
      if (endpoint.includes('NAVAll.txt')) {
        const lines = response.data.split('\n');
        console.log(`  AMFI data: ${lines.length} lines, first 3 lines:`);
        lines.slice(0, 3).forEach((line, i) => console.log(`    ${i+1}: ${line.substring(0, 80)}`));
      }
      
    } catch (error) {
      console.log(`  ✗ Failed: ${error.message}`);
    }
  }
}

testWorkingSchemes();