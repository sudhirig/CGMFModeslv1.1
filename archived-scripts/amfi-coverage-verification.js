/**
 * AMFI Coverage Verification
 * Checks how many AMFI-listed funds we have in our database with historical data
 */

import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

/**
 * Fetch current AMFI fund list (authoritative source)
 */
async function getAMFIFunds() {
  try {
    console.log('📋 Fetching AMFI authoritative fund list...');
    const response = await fetch('https://www.amfiindia.com/spages/NAVAll.txt');
    const text = await response.text();
    
    const lines = text.split('\n');
    const funds = [];
    
    for (const line of lines) {
      if (line.trim() && !line.startsWith('Scheme Code') && !line.includes('Open Ended Schemes')) {
        const parts = line.split(';');
        if (parts.length >= 6) {
          const schemeCode = parts[0]?.trim();
          const schemeName = parts[3]?.trim();
          
          if (schemeCode && schemeName) {
            funds.push({
              schemeCode,
              isinDivPayout: parts[1]?.trim(),
              isinDivReinvest: parts[2]?.trim(),
              schemeName,
              nav: parseFloat(parts[4]) || 0,
              navDate: parts[5]?.trim()
            });
          }
        }
      }
    }
    
    console.log(`✓ AMFI Authoritative List: ${funds.length} funds`);
    return funds;
    
  } catch (error) {
    console.error('Error fetching AMFI data:', error.message);
    return [];
  }
}

/**
 * Check coverage in our database
 */
async function checkDatabaseCoverage(amfiFunds) {
  console.log('\n🔍 Checking coverage in your database...');
  
  try {
    // Get all scheme codes from AMFI
    const amfiSchemeCodes = amfiFunds.map(f => f.schemeCode);
    
    // Check which AMFI funds exist in our database
    const coverageQuery = await pool.query(`
      SELECT 
        f.scheme_code,
        f.fund_name,
        f.category,
        COUNT(nd.*) as nav_records,
        MIN(nd.nav_date) as earliest_nav,
        MAX(nd.nav_date) as latest_nav,
        CASE 
          WHEN COUNT(nd.*) >= 252 THEN 'Sufficient (1+ years)'
          WHEN COUNT(nd.*) >= 63 THEN 'Limited (3+ months)'
          WHEN COUNT(nd.*) > 0 THEN 'Minimal (<3 months)'
          ELSE 'No Data'
        END as data_quality
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE f.scheme_code = ANY($1)
      GROUP BY f.id, f.scheme_code, f.fund_name, f.category
      ORDER BY COUNT(nd.*) DESC
    `, [amfiSchemeCodes]);
    
    const foundFunds = coverageQuery.rows;
    const foundSchemeCodes = new Set(foundFunds.map(f => f.scheme_code));
    
    console.log(`\n📊 Database Coverage Results:`);
    console.log(`  AMFI Listed Funds: ${amfiFunds.length}`);
    console.log(`  Found in Database: ${foundFunds.length}`);
    console.log(`  Coverage: ${((foundFunds.length / amfiFunds.length) * 100).toFixed(1)}%`);
    
    // Analyze data quality
    const qualityBreakdown = {};
    foundFunds.forEach(fund => {
      qualityBreakdown[fund.data_quality] = (qualityBreakdown[fund.data_quality] || 0) + 1;
    });
    
    console.log('\n📈 Data Quality Breakdown:');
    Object.entries(qualityBreakdown).forEach(([quality, count]) => {
      console.log(`  ${quality}: ${count} funds`);
    });
    
    // Find missing AMFI funds
    const missingFunds = amfiFunds.filter(f => !foundSchemeCodes.has(f.schemeCode));
    
    console.log(`\n❌ Missing AMFI Funds: ${missingFunds.length}`);
    if (missingFunds.length > 0) {
      console.log('Sample missing funds:');
      missingFunds.slice(0, 10).forEach(fund => {
        console.log(`  ${fund.schemeCode}: ${fund.schemeName}`);
      });
    }
    
    return {
      totalAmfi: amfiFunds.length,
      foundInDb: foundFunds.length,
      coveragePercent: ((foundFunds.length / amfiFunds.length) * 100).toFixed(1),
      qualityBreakdown,
      missingCount: missingFunds.length,
      missingFunds: missingFunds.slice(0, 100), // Sample for analysis
      foundFunds: foundFunds.filter(f => f.nav_records >= 252) // Funds with sufficient data
    };
    
  } catch (error) {
    console.error('Database coverage check failed:', error.message);
    return null;
  }
}

/**
 * Analyze gaps and recommendations
 */
function analyzeGapsAndRecommendations(coverage) {
  console.log('\n🎯 AMFI COVERAGE ANALYSIS:');
  console.log('='.repeat(50));
  
  const sufficient = coverage.qualityBreakdown['Sufficient (1+ years)'] || 0;
  const limited = coverage.qualityBreakdown['Limited (3+ months)'] || 0;
  const minimal = coverage.qualityBreakdown['Minimal (<3 months)'] || 0;
  const noData = coverage.qualityBreakdown['No Data'] || 0;
  
  console.log(`\nAMFI Compliance Status:`);
  console.log(`  Authoritative AMFI Funds: ${coverage.totalAmfi}`);
  console.log(`  Your Database Coverage: ${coverage.coveragePercent}% (${coverage.foundInDb} funds)`);
  console.log(`  Missing from AMFI List: ${coverage.missingCount} funds`);
  
  console.log(`\nData Quality Assessment:`);
  console.log(`  Analysis-Ready: ${sufficient} funds (${((sufficient/coverage.totalAmfi)*100).toFixed(1)}% of AMFI)`);
  console.log(`  Partial Data: ${limited + minimal} funds`);
  console.log(`  No Historical Data: ${noData} funds`);
  
  console.log(`\n📋 Recommendations:`);
  
  if (coverage.coveragePercent >= 80) {
    console.log(`  ✓ Excellent AMFI coverage - focus on data quality enhancement`);
  } else if (coverage.coveragePercent >= 60) {
    console.log(`  ⚠ Good AMFI coverage - identify and import missing high-priority funds`);
  } else {
    console.log(`  ❌ Limited AMFI coverage - systematic import needed for compliance`);
  }
  
  console.log(`\nPriority Actions:`);
  console.log(`  1. Import historical data for ${coverage.missingCount} missing AMFI funds`);
  console.log(`  2. Enhance data quality for ${limited + minimal} funds with partial data`);
  console.log(`  3. Validate ${sufficient} analysis-ready funds against AMFI standards`);
  console.log(`  4. Implement daily AMFI synchronization for current NAV updates`);
  
  return {
    status: coverage.coveragePercent >= 80 ? 'excellent' : coverage.coveragePercent >= 60 ? 'good' : 'needs_improvement',
    priorityActions: ['import_missing', 'enhance_partial', 'validate_existing', 'sync_daily'],
    missingCount: coverage.missingCount,
    analysisReady: sufficient
  };
}

/**
 * Main verification function
 */
async function runAMFICoverageVerification() {
  console.log('🚀 AMFI Coverage Verification (Authoritative Source)\n');
  
  try {
    // Get AMFI authoritative data
    const amfiFunds = await getAMFIFunds();
    if (amfiFunds.length === 0) {
      throw new Error('Failed to fetch AMFI data');
    }
    
    // Check database coverage
    const coverage = await checkDatabaseCoverage(amfiFunds);
    if (!coverage) {
      throw new Error('Database coverage check failed');
    }
    
    // Generate recommendations
    const analysis = analyzeGapsAndRecommendations(coverage);
    
    console.log(`\n📊 FINAL SUMMARY:`);
    console.log(`AMFI (Authoritative): ${coverage.totalAmfi} funds`);
    console.log(`Your Coverage: ${coverage.coveragePercent}% (${coverage.foundInDb} funds)`);
    console.log(`Analysis-Ready: ${analysis.analysisReady} funds`);
    console.log(`Status: ${analysis.status.toUpperCase()}`);
    
  } catch (error) {
    console.error('AMFI verification failed:', error.message);
  } finally {
    await pool.end();
  }
}

runAMFICoverageVerification();