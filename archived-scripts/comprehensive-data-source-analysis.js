/**
 * Comprehensive Analysis of AMFI, MFAPI.in, and Database Sources
 * Analyzes differences and provides matching recommendations
 */

import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

/**
 * Fetch AMFI mutual fund data
 */
async function getAMFIData() {
  try {
    console.log('🔍 Fetching AMFI data...');
    const response = await fetch('https://www.amfiindia.com/spages/NAVAll.txt');
    const text = await response.text();
    
    const lines = text.split('\n');
    const funds = [];
    
    for (const line of lines) {
      if (line.trim() && !line.startsWith('Scheme Code') && !line.includes('Open Ended Schemes')) {
        const parts = line.split(';');
        if (parts.length >= 6) {
          funds.push({
            schemeCode: parts[0]?.trim(),
            isinDivPayout: parts[1]?.trim(),
            isinDivReinvest: parts[2]?.trim(),
            schemeName: parts[3]?.trim(),
            nav: parseFloat(parts[4]) || 0,
            navDate: parts[5]?.trim()
          });
        }
      }
    }
    
    console.log(`📊 AMFI Total Funds: ${funds.length}`);
    return funds;
    
  } catch (error) {
    console.error('Error fetching AMFI data:', error.message);
    return [];
  }
}

/**
 * Get MFAPI.in data summary
 */
async function getMFAPIData() {
  try {
    console.log('🔍 Fetching MFAPI.in data...');
    const response = await fetch('https://api.mfapi.in/mf');
    const funds = await response.json();
    
    console.log(`📊 MFAPI.in Total Funds: ${funds.length}`);
    return funds;
    
  } catch (error) {
    console.error('Error fetching MFAPI.in data:', error.message);
    return [];
  }
}

/**
 * Analyze your database
 */
async function getDatabaseStats() {
  try {
    console.log('🔍 Analyzing your database...');
    
    const stats = await pool.query(`
      SELECT 
        COUNT(DISTINCT f.id) as total_funds,
        COUNT(DISTINCT nd.fund_id) as funds_with_nav,
        COUNT(*) as total_nav_records,
        COUNT(DISTINCT f.scheme_code) as unique_scheme_codes
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
    `);
    
    const schemeCodeSample = await pool.query(`
      SELECT scheme_code, fund_name, category, COUNT(nd.id) as nav_count
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE scheme_code IS NOT NULL
      GROUP BY scheme_code, fund_name, category
      ORDER BY nav_count DESC
      LIMIT 10
    `);
    
    console.log(`📊 Your Database: ${parseInt(stats.rows[0].total_funds)} funds`);
    return {
      stats: stats.rows[0],
      sampleCodes: schemeCodeSample.rows
    };
    
  } catch (error) {
    console.error('Database analysis error:', error.message);
    return null;
  }
}

/**
 * Cross-reference scheme codes across all sources
 */
async function crossReferenceSchemes(amfiData, mfapiData, dbData) {
  console.log('\n🔍 Cross-referencing scheme codes...');
  
  // Extract scheme codes from each source
  const amfiCodes = new Set(amfiData.map(f => f.schemeCode).filter(Boolean));
  const mfapiCodes = new Set(mfapiData.map(f => f.schemeCode?.toString()).filter(Boolean));
  const dbCodes = new Set(dbData.sampleCodes.map(f => f.scheme_code).filter(Boolean));
  
  console.log('\n📊 Scheme Code Coverage:');
  console.log(`  AMFI unique codes: ${amfiCodes.size.toLocaleString()}`);
  console.log(`  MFAPI.in unique codes: ${mfapiCodes.size.toLocaleString()}`);
  console.log(`  Your DB unique codes: ${parseInt(dbData.stats.unique_scheme_codes).toLocaleString()}`);
  
  // Find overlaps
  const amfiMfapiOverlap = [...amfiCodes].filter(code => mfapiCodes.has(code));
  const amfiDbOverlap = [...amfiCodes].filter(code => dbCodes.has(code));
  const mfapiDbOverlap = [...mfapiCodes].filter(code => dbCodes.has(code));
  const allThreeOverlap = [...amfiCodes].filter(code => mfapiCodes.has(code) && dbCodes.has(code));
  
  console.log('\n🔗 Cross-Source Overlaps:');
  console.log(`  AMFI ∩ MFAPI: ${amfiMfapiOverlap.length} codes`);
  console.log(`  AMFI ∩ Your DB: ${amfiDbOverlap.length} codes`);
  console.log(`  MFAPI ∩ Your DB: ${mfapiDbOverlap.length} codes`);
  console.log(`  All Three Sources: ${allThreeOverlap.length} codes`);
  
  return {
    amfiCodes,
    mfapiCodes,
    dbCodes,
    overlaps: {
      amfiMfapi: amfiMfapiOverlap,
      amfiDb: amfiDbOverlap,
      mfapiDb: mfapiDbOverlap,
      allThree: allThreeOverlap
    }
  };
}

/**
 * Analyze fund name matching potential
 */
async function analyzeFundNameMatching(amfiData, mfapiData, dbData) {
  console.log('\n🔍 Analyzing fund name matching potential...');
  
  // Sample fund names for analysis
  const amfiNames = amfiData.slice(0, 100).map(f => f.schemeName?.toLowerCase().trim()).filter(Boolean);
  const mfapiNames = mfapiData.slice(0, 100).map(f => f.schemeName?.toLowerCase().trim()).filter(Boolean);
  const dbNames = dbData.sampleCodes.map(f => f.fund_name?.toLowerCase().trim()).filter(Boolean);
  
  // Find exact name matches
  const amfiMfapiNameMatches = amfiNames.filter(name => 
    mfapiNames.some(mName => mName.includes(name.substring(0, 20)) || name.includes(mName.substring(0, 20)))
  );
  
  console.log('\n📋 Name Matching Analysis:');
  console.log(`  Sample exact name matches: ${amfiMfapiNameMatches.length}/100`);
  console.log(`  Name matching viable: ${amfiMfapiNameMatches.length > 10 ? 'Yes' : 'Limited'}`);
  
  return {
    nameMatchingViable: amfiMfapiNameMatches.length > 10,
    sampleMatches: amfiMfapiNameMatches.slice(0, 5)
  };
}

/**
 * Generate data integration recommendations
 */
function generateRecommendations(analysis, nameAnalysis) {
  console.log('\n🎯 DATA INTEGRATION RECOMMENDATIONS:');
  console.log('='.repeat(60));
  
  console.log('\n1. SCHEME CODE STANDARDIZATION:');
  if (analysis.overlaps.allThree.length > 1000) {
    console.log('   ✓ Strong scheme code overlap - use as primary matching key');
    console.log('   ✓ Focus on the ' + analysis.overlaps.allThree.length + ' funds present in all sources');
  } else if (analysis.overlaps.mfapiDb.length > 5000) {
    console.log('   ⚠ Limited three-way overlap - prioritize MFAPI ↔ Database matching');
    console.log('   ⚠ Use AMFI as supplementary data source');
  } else {
    console.log('   ❌ Poor scheme code overlap - implement fuzzy matching strategy');
  }
  
  console.log('\n2. DATA SOURCE PRIORITY:');
  console.log('   🥇 Primary: Your Database (20M+ authentic NAV records)');
  console.log('   🥈 Secondary: MFAPI.in (Historical NAV data)');
  console.log('   🥉 Tertiary: AMFI (Current NAV + Fund details)');
  
  console.log('\n3. MATCHING STRATEGY:');
  if (nameAnalysis.nameMatchingViable) {
    console.log('   ✓ Implement scheme code + name fuzzy matching');
    console.log('   ✓ Use ISIN codes from AMFI as additional matching key');
  } else {
    console.log('   ⚠ Focus on scheme code matching only');
    console.log('   ⚠ Manual verification needed for non-matching funds');
  }
  
  console.log('\n4. DATA ENHANCEMENT OPPORTUNITIES:');
  console.log('   • Use AMFI ISIN codes to enhance your fund records');
  console.log('   • Cross-validate NAV data between MFAPI and AMFI for quality');
  console.log('   • Fill missing fund details from AMFI comprehensive data');
  
  console.log('\n5. IMPLEMENTATION PRIORITY:');
  console.log('   1. Map your existing 14,313 funds with AMFI ISIN codes');
  console.log('   2. Cross-validate NAV data quality between sources');
  console.log('   3. Identify and import missing high-quality funds');
  console.log('   4. Implement ongoing data synchronization');
  
  return {
    primaryStrategy: analysis.overlaps.allThree.length > 1000 ? 'scheme_code_primary' : 'hybrid_matching',
    dataSourcePriority: ['database', 'mfapi', 'amfi'],
    enhancementOpportunities: ['isin_mapping', 'nav_validation', 'fund_details_enrichment']
  };
}

/**
 * Main analysis function
 */
async function runComprehensiveAnalysis() {
  console.log('🚀 Comprehensive Data Source Analysis\n');
  
  try {
    // Fetch all data sources
    const [amfiData, mfapiData, dbData] = await Promise.all([
      getAMFIData(),
      getMFAPIData(),
      getDatabaseStats()
    ]);
    
    if (!dbData) {
      throw new Error('Database analysis failed');
    }
    
    // Cross-reference schemes
    const analysis = await crossReferenceSchemes(amfiData, mfapiData, dbData);
    
    // Analyze name matching
    const nameAnalysis = await analyzeFundNameMatching(amfiData, mfapiData, dbData);
    
    // Generate recommendations
    const recommendations = generateRecommendations(analysis, nameAnalysis);
    
    console.log('\n📊 SUMMARY STATISTICS:');
    console.log('='.repeat(40));
    console.log(`AMFI Funds: ${amfiData.length.toLocaleString()}`);
    console.log(`MFAPI Funds: ${mfapiData.length.toLocaleString()}`);
    console.log(`Your Database: ${parseInt(dbData.stats.total_funds).toLocaleString()} funds`);
    console.log(`Common to All: ${analysis.overlaps.allThree.length} funds`);
    console.log(`Your Coverage: ${((parseInt(dbData.stats.funds_with_nav) / Math.max(amfiData.length, mfapiData.length)) * 100).toFixed(1)}%`);
    
  } catch (error) {
    console.error('Analysis failed:', error.message);
  } finally {
    await pool.end();
  }
}

runComprehensiveAnalysis();