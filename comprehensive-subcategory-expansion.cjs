/**
 * Comprehensive Subcategory Expansion Plan
 * Analyzes authentic fund data to create detailed subcategory quartile rankings
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function analyzeSubcategoryStructure() {
  try {
    console.log('=== Comprehensive Subcategory Analysis ===');
    
    // Get detailed subcategory breakdown with eligibility
    const subcategoryAnalysis = await pool.query(`
      SELECT 
        f.category,
        f.subcategory,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN nav_summary.nav_count >= 252 THEN 1 END) as eligible_funds,
        AVG(nav_summary.nav_count) as avg_nav_records,
        MIN(nav_summary.earliest_date) as earliest_data,
        MAX(nav_summary.latest_date) as latest_data
      FROM funds f
      LEFT JOIN (
        SELECT 
          fund_id, 
          COUNT(*) as nav_count,
          MIN(nav_date) as earliest_date,
          MAX(nav_date) as latest_date
        FROM nav_data 
        WHERE created_at > '2025-05-30 06:45:00'
        GROUP BY fund_id
      ) nav_summary ON f.id = nav_summary.fund_id
      WHERE f.subcategory IS NOT NULL 
        AND f.subcategory != ''
      GROUP BY f.category, f.subcategory
      HAVING COUNT(CASE WHEN nav_summary.nav_count >= 252 THEN 1 END) >= 4
      ORDER BY f.category, COUNT(CASE WHEN nav_summary.nav_count >= 252 THEN 1 END) DESC
    `);
    
    console.log('\nEligible Subcategories for Quartile Analysis:');
    console.log('Category'.padEnd(15) + 'Subcategory'.padEnd(25) + 'Total'.padEnd(8) + 'Eligible'.padEnd(10) + 'Avg NAV'.padEnd(10) + 'Data Span');
    console.log('-'.repeat(85));
    
    const subcategoryGroups = {};
    let totalEligibleSubcategories = 0;
    let totalEligibleFunds = 0;
    
    for (const row of subcategoryAnalysis.rows) {
      const category = row.category;
      const subcategory = row.subcategory || 'Other';
      const totalFunds = parseInt(row.total_funds);
      const eligibleFunds = parseInt(row.eligible_funds) || 0;
      const avgNav = Math.round(parseFloat(row.avg_nav_records) || 0);
      const earliestDate = row.earliest_data ? new Date(row.earliest_data).getFullYear() : 'N/A';
      const latestDate = row.latest_data ? new Date(row.latest_data).getFullYear() : 'N/A';
      
      if (!subcategoryGroups[category]) {
        subcategoryGroups[category] = [];
      }
      
      subcategoryGroups[category].push({
        subcategory,
        totalFunds,
        eligibleFunds,
        avgNav,
        dataSpan: `${earliestDate}-${latestDate}`
      });
      
      totalEligibleSubcategories++;
      totalEligibleFunds += eligibleFunds;
      
      console.log(
        category.padEnd(15) + 
        subcategory.padEnd(25) + 
        totalFunds.toString().padEnd(8) + 
        eligibleFunds.toString().padEnd(10) + 
        avgNav.toString().padEnd(10) + 
        `${earliestDate}-${latestDate}`
      );
    }
    
    console.log('-'.repeat(85));
    console.log(`Total: ${totalEligibleSubcategories} subcategories with ${totalEligibleFunds} eligible funds`);
    
    // Detailed breakdown by major categories
    console.log('\n=== Detailed Subcategory Breakdown ===');
    
    for (const [category, subcategories] of Object.entries(subcategoryGroups)) {
      console.log(`\n${category.toUpperCase()} CATEGORY:`);
      
      const categoryTotal = subcategories.reduce((sum, sub) => sum + sub.eligibleFunds, 0);
      console.log(`Total eligible funds: ${categoryTotal}`);
      
      subcategories.forEach((sub, index) => {
        const percentage = ((sub.eligibleFunds / categoryTotal) * 100).toFixed(1);
        console.log(`  ${index + 1}. ${sub.subcategory}: ${sub.eligibleFunds} funds (${percentage}%)`);
      });
    }
    
    // Generate implementation strategy
    console.log('\n=== Implementation Strategy ===');
    
    // Priority 1: High-volume subcategories (>100 eligible funds)
    const highVolumeSubcategories = subcategoryAnalysis.rows.filter(row => 
      parseInt(row.eligible_funds) >= 100
    );
    
    console.log('\nPriority 1 - High Volume Subcategories (100+ funds):');
    highVolumeSubcategories.forEach(row => {
      console.log(`  - ${row.category}/${row.subcategory}: ${row.eligible_funds} funds`);
    });
    
    // Priority 2: Medium-volume subcategories (20-99 eligible funds)
    const mediumVolumeSubcategories = subcategoryAnalysis.rows.filter(row => {
      const eligible = parseInt(row.eligible_funds);
      return eligible >= 20 && eligible < 100;
    });
    
    console.log('\nPriority 2 - Medium Volume Subcategories (20-99 funds):');
    mediumVolumeSubcategories.forEach(row => {
      console.log(`  - ${row.category}/${row.subcategory}: ${row.eligible_funds} funds`);
    });
    
    // Priority 3: Specialized subcategories (4-19 eligible funds)
    const specializedSubcategories = subcategoryAnalysis.rows.filter(row => {
      const eligible = parseInt(row.eligible_funds);
      return eligible >= 4 && eligible < 20;
    });
    
    console.log('\nPriority 3 - Specialized Subcategories (4-19 funds):');
    specializedSubcategories.forEach(row => {
      console.log(`  - ${row.category}/${row.subcategory}: ${row.eligible_funds} funds`);
    });
    
    // Create subcategory configuration
    const subcategoryConfig = {
      expansion_date: new Date().toISOString(),
      total_subcategories: totalEligibleSubcategories,
      total_eligible_funds: totalEligibleFunds,
      categories: subcategoryGroups,
      priority_tiers: {
        high_volume: highVolumeSubcategories.map(row => `${row.category}/${row.subcategory}`),
        medium_volume: mediumVolumeSubcategories.map(row => `${row.category}/${row.subcategory}`),
        specialized: specializedSubcategories.map(row => `${row.category}/${row.subcategory}`)
      }
    };
    
    // Performance impact analysis
    console.log('\n=== Performance Impact Analysis ===');
    console.log(`Current system: 8 categories`);
    console.log(`Proposed system: ${totalEligibleSubcategories} subcategories`);
    console.log(`Expansion factor: ${(totalEligibleSubcategories / 8).toFixed(1)}x increase`);
    console.log(`Average funds per subcategory: ${Math.round(totalEligibleFunds / totalEligibleSubcategories)}`);
    
    // Sample top performers in each major subcategory
    console.log('\n=== Sample Top Performers by Subcategory ===');
    
    const topPerformers = await pool.query(`
      SELECT DISTINCT ON (f.subcategory)
        f.category,
        f.subcategory,
        f.fund_name,
        nav_summary.nav_count,
        fs.total_score
      FROM funds f
      JOIN (
        SELECT fund_id, COUNT(*) as nav_count
        FROM nav_data 
        WHERE created_at > '2025-05-30 06:45:00'
        GROUP BY fund_id
        HAVING COUNT(*) >= 252
      ) nav_summary ON f.id = nav_summary.fund_id
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = CURRENT_DATE
      WHERE f.subcategory IS NOT NULL 
        AND f.subcategory != ''
        AND f.subcategory IN (${subcategoryAnalysis.rows.slice(0, 10).map((_, i) => `$${i + 1}`).join(',')})
      ORDER BY f.subcategory, nav_summary.nav_count DESC
    `, subcategoryAnalysis.rows.slice(0, 10).map(row => row.subcategory));
    
    topPerformers.rows.forEach(row => {
      const score = row.total_score ? ` (Score: ${row.total_score})` : '';
      console.log(`  ${row.category}/${row.subcategory}: ${row.fund_name}${score}`);
    });
    
    return subcategoryConfig;
    
  } catch (error) {
    console.error('Error in subcategory analysis:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  analyzeSubcategoryStructure()
    .then(config => {
      console.log('\n✓ Subcategory analysis completed');
      console.log(`Configuration generated for ${config.total_subcategories} subcategories`);
      process.exit(0);
    })
    .catch(error => {
      console.error('Analysis failed:', error);
      process.exit(1);
    });
}

module.exports = { analyzeSubcategoryStructure };