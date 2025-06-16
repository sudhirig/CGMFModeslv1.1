/**
 * Comprehensive MFAPI.in Coverage Analysis
 * Determines total available data and what percentage we've imported
 */

import pg from 'pg';
const { Pool } = pg;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

/**
 * Get total funds available in MFAPI.in
 */
async function getMFAPITotalFunds() {
  try {
    console.log('🔍 Checking MFAPI.in total fund coverage...');
    
    // Get the current mutual fund list from MFAPI.in
    const response = await fetch('https://api.mfapi.in/mf');
    const funds = await response.json();
    
    console.log(`📊 MFAPI.in Total Funds Available: ${funds.length}`);
    
    // Analyze scheme code patterns in MFAPI.in
    const patterns = {};
    funds.forEach(fund => {
      // Handle different possible property names
      const schemeCode = fund.schemeCode || fund.scheme_code || fund.code || '';
      if (schemeCode && typeof schemeCode === 'string') {
        const prefix = schemeCode.substring(0, 3);
        patterns[prefix] = (patterns[prefix] || 0) + 1;
      }
    });
    
    console.log('\n📋 MFAPI.in Scheme Code Pattern Distribution:');
    Object.entries(patterns)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .forEach(([pattern, count]) => {
        console.log(`  ${pattern}xxx: ${count} funds`);
      });
    
    return {
      totalFunds: funds.length,
      patterns,
      allFunds: funds
    };
    
  } catch (error) {
    console.error('Error fetching MFAPI.in data:', error.message);
    return null;
  }
}

/**
 * Sample historical data availability
 */
async function sampleHistoricalAvailability(sampleFunds, sampleSize = 50) {
  console.log(`\n🔍 Sampling historical data availability (${sampleSize} funds)...`);
  
  let availableCount = 0;
  let totalDataPoints = 0;
  
  for (let i = 0; i < Math.min(sampleSize, sampleFunds.length); i++) {
    const fund = sampleFunds[i];
    
    try {
      // Test current year data availability
      const response = await fetch(`https://api.mfapi.in/mf/${fund.schemeCode}/latest/1000`);
      const data = await response.json();
      
      if (data.data && data.data.length > 0) {
        availableCount++;
        totalDataPoints += data.data.length;
        
        if (i < 5) { // Log first few for analysis
          console.log(`  ✓ ${fund.schemeName}: ${data.data.length} recent NAV records`);
        }
      } else {
        if (i < 5) {
          console.log(`  ✗ ${fund.schemeName}: No historical data`);
        }
      }
      
      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      if (i < 5) {
        console.log(`  ✗ ${fund.schemeName}: Error - ${error.message}`);
      }
    }
  }
  
  console.log(`\n📊 Historical Data Sample Results:`);
  console.log(`  Funds with data: ${availableCount}/${sampleSize} (${((availableCount/sampleSize)*100).toFixed(1)}%)`);
  console.log(`  Average NAV records per fund: ${Math.round(totalDataPoints/availableCount)} records`);
  console.log(`  Estimated total NAV records in MFAPI.in: ${(totalDataPoints/availableCount * availableCount * (sampleFunds.length/sampleSize)).toLocaleString()}`);
  
  return {
    fundsWithData: availableCount,
    sampleSize,
    avgRecordsPerFund: Math.round(totalDataPoints/availableCount),
    estimatedTotalRecords: Math.round(totalDataPoints/availableCount * availableCount * (sampleFunds.length/sampleSize))
  };
}

/**
 * Compare with our database
 */
async function compareWithDatabase(mfapiData, historicalSample) {
  console.log('\n🔍 Comparing with your database...');
  
  try {
    // Get our database stats
    const dbStats = await pool.query(`
      SELECT 
        COUNT(DISTINCT f.id) as our_total_funds,
        COUNT(DISTINCT nd.fund_id) as our_funds_with_nav,
        COUNT(*) as our_total_nav_records,
        MIN(nd.nav_date) as earliest_date,
        MAX(nd.nav_date) as latest_date
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
    `);
    
    const stats = dbStats.rows[0];
    
    console.log('\n📊 Coverage Comparison:');
    console.log('MFAPI.in vs Your Database:');
    console.log(`  Total Funds: ${mfapiData.totalFunds.toLocaleString()} vs ${parseInt(stats.our_total_funds).toLocaleString()}`);
    console.log(`  Funds with NAV data: ~${Math.round(mfapiData.totalFunds * (historicalSample.fundsWithData/historicalSample.sampleSize)).toLocaleString()} vs ${parseInt(stats.our_funds_with_nav).toLocaleString()}`);
    console.log(`  Estimated NAV records: ~${historicalSample.estimatedTotalRecords.toLocaleString()} vs ${parseInt(stats.our_total_nav_records).toLocaleString()}`);
    
    // Calculate coverage percentages
    const fundCoverage = (parseInt(stats.our_funds_with_nav) / Math.round(mfapiData.totalFunds * (historicalSample.fundsWithData/historicalSample.sampleSize))) * 100;
    const dataCoverage = (parseInt(stats.our_total_nav_records) / historicalSample.estimatedTotalRecords) * 100;
    
    console.log('\n📈 Your Import Coverage:');
    console.log(`  Fund Coverage: ${fundCoverage.toFixed(1)}% of available funds`);
    console.log(`  Data Coverage: ${dataCoverage.toFixed(1)}% of estimated NAV records`);
    console.log(`  Date Range: ${stats.earliest_date} to ${stats.latest_date}`);
    
    // Check scheme code overlap
    const ourSchemeCodes = await pool.query(`
      SELECT scheme_code, COUNT(*) as nav_count
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE scheme_code IS NOT NULL
      GROUP BY scheme_code
      ORDER BY nav_count DESC
      LIMIT 10
    `);
    
    console.log('\n📋 Your Top Scheme Codes by NAV Data:');
    ourSchemeCodes.rows.forEach(row => {
      console.log(`  ${row.scheme_code}: ${parseInt(row.nav_count).toLocaleString()} NAV records`);
    });
    
    return {
      fundCoverage: fundCoverage.toFixed(1),
      dataCoverage: dataCoverage.toFixed(1),
      ourStats: stats,
      mfapiEstimate: {
        totalFunds: mfapiData.totalFunds,
        fundsWithData: Math.round(mfapiData.totalFunds * (historicalSample.fundsWithData/historicalSample.sampleSize)),
        estimatedRecords: historicalSample.estimatedTotalRecords
      }
    };
    
  } catch (error) {
    console.error('Database comparison error:', error.message);
    return null;
  }
}

/**
 * Find missing data opportunities
 */
async function findMissingDataOpportunities(mfapiData) {
  console.log('\n🔍 Analyzing missing data opportunities...');
  
  try {
    // Get our scheme codes
    const ourCodes = await pool.query(`
      SELECT DISTINCT scheme_code 
      FROM funds 
      WHERE scheme_code IS NOT NULL
    `);
    
    const ourSchemeSet = new Set(ourCodes.rows.map(row => row.scheme_code));
    const mfapiSchemeSet = new Set(mfapiData.allFunds.map(fund => fund.schemeCode));
    
    // Find funds in MFAPI.in that we don't have
    const missingInOurDB = mfapiData.allFunds.filter(fund => !ourSchemeSet.has(fund.schemeCode));
    
    // Find funds we have that aren't in MFAPI.in
    const notInMFAPI = ourCodes.rows.filter(row => !mfapiSchemeSet.has(row.scheme_code));
    
    console.log('\n📊 Missing Data Analysis:');
    console.log(`  Funds in MFAPI.in we don't have: ${missingInOurDB.length}`);
    console.log(`  Our funds not in MFAPI.in: ${notInMFAPI.length}`);
    
    if (missingInOurDB.length > 0) {
      console.log('\n📋 Sample Missing Funds (first 10):');
      missingInOurDB.slice(0, 10).forEach(fund => {
        console.log(`  ${fund.schemeCode}: ${fund.schemeName}`);
      });
    }
    
    return {
      missingFromOurDB: missingInOurDB.length,
      notInMFAPI: notInMFAPI.length,
      missingFunds: missingInOurDB.slice(0, 50) // Return sample for potential import
    };
    
  } catch (error) {
    console.error('Missing data analysis error:', error.message);
    return null;
  }
}

/**
 * Main analysis function
 */
async function runComprehensiveCoverageAnalysis() {
  console.log('🚀 Starting Comprehensive MFAPI.in Coverage Analysis...\n');
  
  try {
    // Step 1: Get MFAPI.in total funds
    const mfapiData = await getMFAPITotalFunds();
    if (!mfapiData) {
      throw new Error('Failed to fetch MFAPI.in data');
    }
    
    // Step 2: Sample historical data availability
    const historicalSample = await sampleHistoricalAvailability(mfapiData.allFunds, 100);
    
    // Step 3: Compare with our database
    const comparison = await compareWithDatabase(mfapiData, historicalSample);
    
    // Step 4: Find missing opportunities
    const missingAnalysis = await findMissingDataOpportunities(mfapiData);
    
    console.log('\n🎯 SUMMARY:');
    console.log('='.repeat(50));
    console.log(`MFAPI.in Total Funds: ${mfapiData.totalFunds.toLocaleString()}`);
    console.log(`Your Fund Coverage: ${comparison?.fundCoverage}%`);
    console.log(`Your Data Coverage: ${comparison?.dataCoverage}%`);
    console.log(`Missing Opportunities: ${missingAnalysis?.missingFromOurDB || 0} funds`);
    console.log('='.repeat(50));
    
  } catch (error) {
    console.error('Analysis failed:', error.message);
  } finally {
    await pool.end();
  }
}

// Run the analysis
runComprehensiveCoverageAnalysis();