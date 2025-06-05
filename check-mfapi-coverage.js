/**
 * Check MFAPI.in coverage for funds that need historical data
 */
import axios from 'axios';
import { executeRawQuery } from './server/db.js';

async function checkMFAPICoverage() {
  console.log('=== MFAPI.in Coverage Analysis ===\n');

  try {
    // Step 1: Get all available schemes from MFAPI.in
    console.log('📡 Fetching available schemes from MFAPI.in...');
    const mfapiResponse = await axios.get('https://api.mfapi.in/mf', {
      timeout: 30000
    });
    
    const availableSchemes = mfapiResponse.data;
    console.log(`✅ Found ${availableSchemes.length} schemes available in MFAPI.in\n`);

    // Create a map of available scheme codes for quick lookup
    const availableSchemeMap = new Map();
    availableSchemes.forEach(scheme => {
      availableSchemeMap.set(scheme.schemeCode.toString(), scheme);
    });

    // Step 2: Get funds from our database that need historical data
    console.log('🔍 Checking funds in database that need historical data...');
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.scheme_code, f.fund_name, f.category, COUNT(n.nav_date) as nav_count
      FROM funds f
      LEFT JOIN nav_data n ON f.id = n.fund_id
      GROUP BY f.id, f.scheme_code, f.fund_name, f.category
      HAVING COUNT(n.nav_date) <= 2
      ORDER BY f.category, f.id
      LIMIT 100
    `);

    const fundsNeedingData = fundsResult.rows;
    console.log(`📊 Found ${fundsNeedingData.length} funds that need historical data\n`);

    // Step 3: Check coverage
    let coveredCount = 0;
    let notCoveredCount = 0;
    const coveredFunds = [];
    const notCoveredFunds = [];

    console.log('🔍 Checking coverage...\n');
    
    for (const fund of fundsNeedingData) {
      const schemeCode = fund.scheme_code.toString();
      
      if (availableSchemeMap.has(schemeCode)) {
        const mfapiScheme = availableSchemeMap.get(schemeCode);
        coveredCount++;
        coveredFunds.push({
          ...fund,
          mfapiName: mfapiScheme.schemeName
        });
        console.log(`✅ ${fund.fund_name} (${schemeCode}) - Available in MFAPI.in`);
      } else {
        notCoveredCount++;
        notCoveredFunds.push(fund);
        console.log(`❌ ${fund.fund_name} (${schemeCode}) - NOT available in MFAPI.in`);
      }
    }

    // Step 4: Summary
    console.log('\n=== COVERAGE SUMMARY ===');
    console.log(`Total funds checked: ${fundsNeedingData.length}`);
    console.log(`Available in MFAPI.in: ${coveredCount} (${((coveredCount/fundsNeedingData.length)*100).toFixed(1)}%)`);
    console.log(`NOT available in MFAPI.in: ${notCoveredCount} (${((notCoveredCount/fundsNeedingData.length)*100).toFixed(1)}%)`);

    // Step 5: Sample working scheme codes for testing
    console.log('\n=== SAMPLE WORKING SCHEME CODES ===');
    const sampleWorking = availableSchemes.slice(0, 10);
    sampleWorking.forEach(scheme => {
      console.log(`${scheme.schemeCode}: ${scheme.schemeName}`);
    });

    // Step 6: Check if any of our covered funds actually have data
    if (coveredFunds.length > 0) {
      console.log('\n=== TESTING DATA AVAILABILITY ===');
      const testFund = coveredFunds[0];
      console.log(`🧪 Testing data availability for: ${testFund.fund_name} (${testFund.scheme_code})`);
      
      try {
        const testResponse = await axios.get(`https://api.mfapi.in/mf/${testFund.scheme_code}`, {
          timeout: 15000
        });
        
        if (testResponse.data && testResponse.data.data && testResponse.data.data.length > 0) {
          console.log(`✅ Historical data available: ${testResponse.data.data.length} records`);
          console.log(`📅 Date range: ${testResponse.data.data[testResponse.data.data.length-1].date} to ${testResponse.data.data[0].date}`);
        } else {
          console.log(`❌ No historical data found despite scheme being available`);
        }
      } catch (error) {
        console.log(`❌ Error fetching test data: ${error.message}`);
      }
    }

    return {
      totalChecked: fundsNeedingData.length,
      covered: coveredCount,
      notCovered: notCoveredCount,
      coveragePercentage: (coveredCount/fundsNeedingData.length)*100,
      coveredFunds,
      notCoveredFunds,
      availableSchemesCount: availableSchemes.length
    };

  } catch (error) {
    console.error('❌ Error during coverage analysis:', error.message);
    throw error;
  }
}

// Run the analysis
checkMFAPICoverage()
  .then(result => {
    console.log('\n✅ Coverage analysis completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('❌ Coverage analysis failed:', error.message);
    process.exit(1);
  });