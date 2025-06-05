/**
 * Complete Category Ranking Implementation
 * Implements subcategory-based quartile rankings using authentic 100-point scores
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function completeCategoryRanking() {
  try {
    console.log('=== Complete Category Ranking Implementation ===');
    console.log('Implementing subcategory quartile rankings using authentic 100-point scores');
    
    // Step 1: Update subcategory field from funds table
    await updateSubcategoryField();
    
    // Step 2: Calculate subcategory rankings
    await calculateSubcategoryRankings();
    
    // Step 3: Calculate overall category rankings
    await calculateOverallRankings();
    
    // Step 4: Validate results
    await validateRankingResults();
    
    console.log('\n✓ Complete category ranking system implemented');
    
  } catch (error) {
    console.error('Category ranking implementation error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function updateSubcategoryField() {
  console.log('\n1. Updating Subcategory Field from Fund Data...');
  
  // Update subcategory in fund_scores from funds table
  const updateResult = await pool.query(`
    UPDATE fund_scores 
    SET subcategory = f.subcategory
    FROM funds f
    WHERE fund_scores.fund_id = f.id 
      AND fund_scores.score_date = CURRENT_DATE
      AND f.subcategory IS NOT NULL
  `);
  
  console.log(`  ✓ Updated subcategory for ${updateResult.rowCount} funds`);
  
  // Check updated distribution
  const distribution = await pool.query(`
    SELECT 
      subcategory,
      COUNT(*) as fund_count
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND subcategory IS NOT NULL
    GROUP BY subcategory
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('\n  Updated Subcategory Distribution:');
  for (const dist of distribution.rows) {
    console.log(`    ${dist.subcategory}: ${dist.fund_count} funds`);
  }
}

async function calculateSubcategoryRankings() {
  console.log('\n2. Calculating Subcategory Rankings...');
  
  // Get all subcategories with sufficient funds for ranking
  const subcategories = await pool.query(`
    SELECT 
      subcategory,
      COUNT(*) as fund_count
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND subcategory IS NOT NULL
      AND total_score IS NOT NULL
    GROUP BY subcategory
    HAVING COUNT(*) >= 3
    ORDER BY COUNT(*) DESC
  `);
  
  console.log(`  Processing ${subcategories.rows.length} subcategories for ranking...`);
  
  let totalRanked = 0;
  
  for (const subcategory of subcategories.rows) {
    try {
      const subcat = subcategory.subcategory;
      console.log(`\n    Processing: ${subcat} (${subcategory.fund_count} funds)`);
      
      // Get funds in this subcategory sorted by total score (descending = best first)
      const subcategoryFunds = await pool.query(`
        SELECT 
          fund_id,
          total_score,
          historical_returns_total,
          risk_grade_total,
          fundamentals_total
        FROM fund_scores 
        WHERE score_date = CURRENT_DATE
          AND subcategory = $1
          AND total_score IS NOT NULL
        ORDER BY total_score DESC, historical_returns_total DESC
      `, [subcat]);
      
      const funds = subcategoryFunds.rows;
      const fundCount = funds.length;
      
      // Calculate rankings and quartiles
      for (let i = 0; i < funds.length; i++) {
        const fund = funds[i];
        const rank = i + 1;
        
        // Calculate quartile (1 = top quartile, 4 = bottom quartile)
        let quartile;
        const percentile = rank / fundCount;
        if (percentile <= 0.25) quartile = 1;
        else if (percentile <= 0.50) quartile = 2;
        else if (percentile <= 0.75) quartile = 3;
        else quartile = 4;
        
        // Calculate percentile (0-100, where 100 = best)
        const percentileScore = Math.round((fundCount - rank) / (fundCount - 1) * 100);
        
        // Update fund_scores with subcategory ranking
        await pool.query(`
          UPDATE fund_scores SET
            subcategory_rank = $1,
            subcategory_quartile = $2,
            subcategory_percentile = $3,
            subcategory_total = $4
          WHERE fund_id = $5 AND score_date = CURRENT_DATE
        `, [rank, quartile, percentileScore, fundCount, fund.fund_id]);
      }
      
      totalRanked += funds.length;
      
      // Calculate quartile distribution for this subcategory
      const quartileStats = funds.reduce((acc, fund, index) => {
        const percentile = (index + 1) / funds.length;
        let quartile;
        if (percentile <= 0.25) quartile = 1;
        else if (percentile <= 0.50) quartile = 2;
        else if (percentile <= 0.75) quartile = 3;
        else quartile = 4;
        
        acc[quartile] = (acc[quartile] || 0) + 1;
        return acc;
      }, {});
      
      console.log(`      ✓ Ranked ${funds.length} funds: Q1=${quartileStats[1]||0}, Q2=${quartileStats[2]||0}, Q3=${quartileStats[3]||0}, Q4=${quartileStats[4]||0}`);
      
    } catch (error) {
      console.error(`      Error processing subcategory ${subcategory.subcategory}:`, error.message);
    }
  }
  
  console.log(`\n  ✓ Total funds ranked in subcategories: ${totalRanked}`);
}

async function calculateOverallRankings() {
  console.log('\n3. Calculating Overall Category Rankings...');
  
  // Calculate overall rankings across all funds
  const allFunds = await pool.query(`
    SELECT 
      fund_id,
      total_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND total_score IS NOT NULL
    ORDER BY total_score DESC
  `);
  
  const funds = allFunds.rows;
  const totalFunds = funds.length;
  
  console.log(`  Calculating overall rankings for ${totalFunds} funds...`);
  
  for (let i = 0; i < funds.length; i++) {
    const fund = funds[i];
    const rank = i + 1;
    
    // Calculate overall quartile
    let quartile;
    const percentile = rank / totalFunds;
    if (percentile <= 0.25) quartile = 1;
    else if (percentile <= 0.50) quartile = 2;
    else if (percentile <= 0.75) quartile = 3;
    else quartile = 4;
    
    // Update overall category ranking
    await pool.query(`
      UPDATE fund_scores SET
        category_rank = $1,
        quartile = $2,
        category_total = $3
      WHERE fund_id = $4 AND score_date = CURRENT_DATE
    `, [rank, quartile, totalFunds, fund.fund_id]);
  }
  
  console.log(`  ✓ Calculated overall rankings for all ${totalFunds} funds`);
}

async function validateRankingResults() {
  console.log('\n4. Validating Ranking Results...');
  
  // Overall validation
  const validation = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN category_rank IS NOT NULL THEN 1 END) as has_category_rank,
      COUNT(CASE WHEN quartile IS NOT NULL THEN 1 END) as has_quartile,
      COUNT(CASE WHEN subcategory_rank IS NOT NULL THEN 1 END) as has_subcategory_rank,
      COUNT(CASE WHEN subcategory_quartile IS NOT NULL THEN 1 END) as has_subcategory_quartile,
      COUNT(CASE WHEN subcategory IS NOT NULL THEN 1 END) as has_subcategory
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const result = validation.rows[0];
  
  console.log('  Ranking System Validation:');
  console.log(`    Total Funds: ${result.total_funds}`);
  console.log(`    Overall Rankings: ${result.has_category_rank}/${result.total_funds} (${Math.round(result.has_category_rank/result.total_funds*100)}%)`);
  console.log(`    Overall Quartiles: ${result.has_quartile}/${result.total_funds} (${Math.round(result.has_quartile/result.total_funds*100)}%)`);
  console.log(`    Subcategory Rankings: ${result.has_subcategory_rank}/${result.total_funds} (${Math.round(result.has_subcategory_rank/result.total_funds*100)}%)`);
  console.log(`    Subcategory Quartiles: ${result.has_subcategory_quartile}/${result.total_funds} (${Math.round(result.has_subcategory_quartile/result.total_funds*100)}%)`);
  console.log(`    Has Subcategory: ${result.has_subcategory}/${result.total_funds} (${Math.round(result.has_subcategory/result.total_funds*100)}%)`);
  
  // Overall quartile distribution
  const quartileDistribution = await pool.query(`
    SELECT 
      quartile,
      COUNT(*) as fund_count,
      ROUND(AVG(total_score), 2) as avg_score,
      ROUND(MIN(total_score), 2) as min_score,
      ROUND(MAX(total_score), 2) as max_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND quartile IS NOT NULL
    GROUP BY quartile
    ORDER BY quartile
  `);
  
  console.log('\n  Overall Quartile Distribution:');
  console.log('  Quartile'.padEnd(15) + 'Funds'.padEnd(8) + 'Avg Score'.padEnd(12) + 'Range');
  console.log('  ' + '-'.repeat(50));
  
  for (const q of quartileDistribution.rows) {
    const quartileName = ['', 'Top 25%', '2nd 25%', '3rd 25%', 'Bottom 25%'][q.quartile];
    console.log(
      `  ${quartileName}`.padEnd(15) +
      q.fund_count.toString().padEnd(8) +
      q.avg_score.toString().padEnd(12) +
      `${q.min_score}-${q.max_score}`
    );
  }
  
  // Subcategory analysis
  const subcategoryAnalysis = await pool.query(`
    SELECT 
      subcategory,
      COUNT(*) as fund_count,
      COUNT(DISTINCT subcategory_quartile) as quartile_spread,
      ROUND(AVG(total_score), 2) as avg_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND subcategory IS NOT NULL
      AND subcategory_rank IS NOT NULL
    GROUP BY subcategory
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('\n  Subcategory Ranking Analysis:');
  console.log('  Subcategory'.padEnd(20) + 'Funds'.padEnd(8) + 'Quartiles'.padEnd(12) + 'Avg Score');
  console.log('  ' + '-'.repeat(55));
  
  for (const sub of subcategoryAnalysis.rows) {
    console.log(
      `  ${sub.subcategory}`.padEnd(20) +
      sub.fund_count.toString().padEnd(8) +
      sub.quartile_spread.toString().padEnd(12) +
      sub.avg_score.toString()
    );
  }
  
  // Success metrics
  const successRate = Math.round(result.has_category_rank / result.total_funds * 100);
  const subcategoryRate = Math.round(result.has_subcategory_rank / result.total_funds * 100);
  
  console.log('\n  Success Metrics:');
  console.log(`    Overall Ranking Success: ${successRate}%`);
  console.log(`    Subcategory Ranking Success: ${subcategoryRate}%`);
  
  if (successRate === 100) {
    console.log('    ✓ ALL FUNDS HAVE COMPLETE CATEGORY RANKINGS');
  }
  
  if (subcategoryRate >= 80) {
    console.log('    ✓ EXCELLENT SUBCATEGORY COVERAGE');
  }
  
  // Top performers by category
  const topPerformers = await pool.query(`
    SELECT 
      subcategory,
      category_rank,
      subcategory_rank,
      total_score,
      quartile,
      subcategory_quartile
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND subcategory_rank = 1
    ORDER BY total_score DESC
    LIMIT 8
  `);
  
  console.log('\n  Top Performers by Subcategory:');
  console.log('  Subcategory'.padEnd(20) + 'Overall'.padEnd(10) + 'Score'.padEnd(8) + 'Quartiles');
  console.log('  ' + '-'.repeat(55));
  
  for (const top of topPerformers.rows) {
    console.log(
      `  ${top.subcategory || 'Unknown'}`.padEnd(20) +
      `#${top.category_rank}`.padEnd(10) +
      top.total_score.toString().padEnd(8) +
      `Q${top.quartile}/Q${top.subcategory_quartile}`
    );
  }
}

if (require.main === module) {
  completeCategoryRanking()
    .then(() => {
      console.log('\n✓ Complete category ranking implementation successful');
      process.exit(0);
    })
    .catch(error => {
      console.error('Category ranking implementation failed:', error);
      process.exit(1);
    });
}

module.exports = { completeCategoryRanking };