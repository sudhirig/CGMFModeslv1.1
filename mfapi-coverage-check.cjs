/**
 * Simple MFAPI.in coverage check using curl and basic analysis
 */
const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);

async function checkMFAPICoverage() {
  console.log('=== MFAPI.in Coverage Analysis ===\n');

  try {
    // Step 1: Get available schemes from MFAPI.in
    console.log('📡 Fetching available schemes from MFAPI.in...');
    const { stdout: mfapiData } = await execAsync('curl -s "https://api.mfapi.in/mf"');
    
    let availableSchemes;
    try {
      availableSchemes = JSON.parse(mfapiData);
      console.log(`✅ Found ${availableSchemes.length} schemes available in MFAPI.in\n`);
    } catch (parseError) {
      console.error('❌ Error parsing MFAPI.in response');
      return;
    }

    // Create map of available scheme codes
    const availableSchemeMap = new Set();
    availableSchemes.forEach(scheme => {
      availableSchemeMap.add(scheme.schemeCode.toString());
    });

    // Step 2: Get sample of funds from database that need data
    console.log('🔍 Getting sample of funds that need historical data...');
    const { stdout: dbResult } = await execAsync(`
      curl -s "http://localhost:5000/api/funds/count" || echo '{"error": "API not available"}'
    `);

    // Step 3: Test some known scheme codes from your successful patterns
    console.log('🧪 Testing scheme codes from successful patterns...\n');
    
    const testCodes = [
      '119551', '119552', '119553', // 119xxx pattern
      '120001', '120002', '120003', // 120xxx pattern
      '118001', '118002', '118003', // 118xxx pattern
      '100001', '100002', '100003', // 100xxx pattern
      '101001', '101002', '101003', // 101xxx pattern
      '102001', '102002', '102003'  // 102xxx pattern
    ];

    let foundCount = 0;
    const foundCodes = [];
    const notFoundCodes = [];

    for (const code of testCodes) {
      if (availableSchemeMap.has(code)) {
        foundCount++;
        foundCodes.push(code);
        console.log(`✅ ${code} - Available in MFAPI.in`);
      } else {
        notFoundCodes.push(code);
        console.log(`❌ ${code} - NOT available in MFAPI.in`);
      }
    }

    // Step 4: Test actual data availability for found codes
    console.log('\n=== TESTING ACTUAL DATA AVAILABILITY ===');
    
    if (foundCodes.length > 0) {
      for (const code of foundCodes.slice(0, 3)) { // Test first 3 found codes
        try {
          console.log(`\n🧪 Testing ${code}...`);
          const { stdout: testData } = await execAsync(`curl -s "https://api.mfapi.in/mf/${code}"`);
          
          const parsed = JSON.parse(testData);
          if (parsed && parsed.data && parsed.data.length > 0) {
            console.log(`✅ ${code}: ${parsed.data.length} historical records available`);
            console.log(`   Scheme: ${parsed.meta?.scheme_name || 'Unknown'}`);
            console.log(`   Date range: ${parsed.data[parsed.data.length-1]?.date} to ${parsed.data[0]?.date}`);
          } else {
            console.log(`❌ ${code}: No historical data despite being listed`);
          }
        } catch (error) {
          console.log(`❌ ${code}: Error fetching data`);
        }
      }
    }

    // Step 5: Summary and recommendations
    console.log('\n=== SUMMARY ===');
    console.log(`Total schemes available in MFAPI.in: ${availableSchemes.length}`);
    console.log(`Test codes found: ${foundCount}/${testCodes.length}`);
    
    if (foundCount > 0) {
      console.log('\n🎯 WORKING SCHEME CODE PATTERNS:');
      foundCodes.forEach(code => {
        const scheme = availableSchemes.find(s => s.schemeCode.toString() === code);
        if (scheme) {
          console.log(`   ${code}: ${scheme.schemeName}`);
        }
      });
    }

    // Step 6: Show sample of all available schemes
    console.log('\n📋 SAMPLE AVAILABLE SCHEMES:');
    availableSchemes.slice(0, 10).forEach(scheme => {
      console.log(`   ${scheme.schemeCode}: ${scheme.schemeName}`);
    });

    return {
      totalAvailable: availableSchemes.length,
      testResults: {
        found: foundCount,
        total: testCodes.length,
        foundCodes,
        notFoundCodes
      }
    };

  } catch (error) {
    console.error('❌ Error during coverage analysis:', error.message);
    throw error;
  }
}

// Run the analysis
checkMFAPICoverage()
  .then(result => {
    console.log('\n✅ Coverage analysis completed');
  })
  .catch(error => {
    console.error('❌ Analysis failed:', error.message);
  });