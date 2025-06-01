/**
 * Category Ranking Deep Dive Analysis
 * Comprehensive investigation of category_table and category_rank null values
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function categoryRankingDeepDive() {
  try {
    console.log('=== Category Ranking Deep Dive Analysis ===');
    console.log('Investigating category_table and category_rank null values');
    
    // Step 1: Examine current state
    await examineCurrentState();
    
    // Step 2: Analyze fund distribution by categories
    await analyzeFundDistribution();
    
    // Step 3: Check quartile calculation requirements
    await checkQuartileRequirements();
    
    // Step 4: Implement category ranking calculation
    await implementCategoryRanking();
    
    // Step 5: Validate the results
    await validateResults();
    
    console.log('\n✓ Category ranking analysis completed');
    
  } catch (error) {
    console.error('Category ranking analysis error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function examineCurrentState() {
  console.log('\n1. Examining Current Category Ranking State...');
  
  // Check null values in category fields
  const nullAnalysis = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN category_table IS NULL THEN 1 END) as null_category_table,
      COUNT(CASE WHEN category_rank IS NULL THEN 1 END) as null_category_rank,
      COUNT(CASE WHEN quartile IS NULL THEN 1 END) as null_quartile,
      COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as has_total_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const state = nullAnalysis.rows[0];
  console.log('  Current State Analysis:');
  console.log(`    Total Funds: ${state.total_funds}`);
  console.log(`    Null category_table: ${state.null_category_table} (${Math.round(state.null_category_table/state.total_funds*100)}%)`);
  console.log(`    Null category_rank: ${state.null_category_rank} (${Math.round(state.null_category_rank/state.total_funds*100)}%)`);
  console.log(`    Null quartile: ${state.null_quartile} (${Math.round(state.null_quartile/state.total_funds*100)}%)`);
  console.log(`    Has total_score: ${state.has_total_score}`);
  
  // Check if any funds have category rankings
  const hasRankings = await pool.query(`
    SELECT DISTINCT category_table, COUNT(*) as fund_count
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND category_table IS NOT NULL
    GROUP BY category_table
    ORDER BY fund_count DESC
  `);
  
  console.log('\n  Existing Category Rankings:');
  if (hasRankings.rows.length > 0) {
    for (const ranking of hasRankings.rows) {
      console.log(`    ${ranking.category_table}: ${ranking.fund_count} funds`);
    }
  } else {
    console.log('    No category rankings found - this explains the null values');
  }
}

async function analyzeFundDistribution() {
  console.log('\n2. Analyzing Fund Distribution by Categories...');
  
  // Analyze fund categories and subcategories
  const categoryDistribution = await pool.query(`
    SELECT 
      f.category,
      f.subcategory,
      COUNT(*) as fund_count,
      COUNT(CASE WHEN fs.total_score IS NOT NULL THEN 1 END) as scored_funds
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
    GROUP BY f.category, f.subcategory
    HAVING COUNT(*) >= 3
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('  Fund Distribution (categories with 3+ funds):');
  console.log('  Category'.padEnd(20) + 'Subcategory'.padEnd(25) + 'Funds'.padEnd(8) + 'Scored');
  console.log('  ' + '-'.repeat(70));
  
  for (const dist of categoryDistribution.rows) {
    console.log(
      `  ${dist.category || 'Unknown'}`.padEnd(20) +
      `${dist.subcategory || 'None'}`.padEnd(25) +
      dist.fund_count.toString().padEnd(8) +
      dist.scored_funds.toString()
    );
  }
  
  // Identify which categories are suitable for quartile ranking
  const suitableCategories = await pool.query(`
    SELECT 
      f.subcategory as category_name,
      COUNT(*) as fund_count,
      COUNT(CASE WHEN fs.total_score IS NOT NULL THEN 1 END) as scored_funds,
      ROUND(AVG(fs.total_score), 2) as avg_score,
      ROUND(MIN(fs.total_score), 2) as min_score,
      ROUND(MAX(fs.total_score), 2) as max_score
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.total_score IS NOT NULL
      AND f.subcategory IS NOT NULL
    GROUP BY f.subcategory
    HAVING COUNT(*) >= 4
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('\n  Categories Suitable for Quartile Ranking (4+ scored funds):');
  console.log('  Category'.padEnd(25) + 'Funds'.padEnd(8) + 'Avg Score'.padEnd(12) + 'Range');
  console.log('  ' + '-'.repeat(60));
  
  for (const cat of suitableCategories.rows) {
    console.log(
      `  ${cat.category_name}`.padEnd(25) +
      cat.fund_count.toString().padEnd(8) +
      cat.avg_score.toString().padEnd(12) +
      `${cat.min_score}-${cat.max_score}`
    );
  }
  
  console.log(`\n  Found ${suitableCategories.rows.length} categories suitable for quartile ranking`);
}

async function checkQuartileRequirements() {
  console.log('\n3. Checking Quartile Calculation Requirements...');
  
  // Check if quartile_rankings table exists
  const tableExists = await pool.query(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_name = 'quartile_rankings'
    )
  `);
  
  console.log(`  Quartile Rankings Table Exists: ${tableExists.rows[0].exists}`);
  
  if (!tableExists.rows[0].exists) {
    console.log('  Creating quartile_rankings table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS quartile_rankings (
        id SERIAL PRIMARY KEY,
        category_name VARCHAR(255) NOT NULL,
        fund_id INTEGER REFERENCES funds(id),
        total_score NUMERIC(5,2),
        category_rank INTEGER,
        quartile INTEGER,
        calculation_date DATE DEFAULT CURRENT_DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(category_name, fund_id, calculation_date)
      )
    `);
    console.log('  ✓ Quartile rankings table created');
  }
  
  // Check existing quartile data
  const existingQuartiles = await pool.query(`
    SELECT 
      category_name,
      COUNT(*) as ranked_funds,
      COUNT(DISTINCT quartile) as quartile_count
    FROM quartile_rankings 
    WHERE calculation_date = CURRENT_DATE
    GROUP BY category_name
    ORDER BY ranked_funds DESC
  `);
  
  console.log('\n  Existing Quartile Rankings:');
  if (existingQuartiles.rows.length > 0) {
    for (const quartile of existingQuartiles.rows) {
      console.log(`    ${quartile.category_name}: ${quartile.ranked_funds} funds, ${quartile.quartile_count} quartiles`);
    }
  } else {
    console.log('    No existing quartile rankings found');
  }
}

async function implementCategoryRanking() {
  console.log('\n4. Implementing Category Ranking Calculation...');
  
  // Get all categories with sufficient funds for ranking
  const categories = await pool.query(`
    SELECT 
      f.subcategory as category_name,
      COUNT(*) as fund_count
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.total_score IS NOT NULL
      AND f.subcategory IS NOT NULL
    GROUP BY f.subcategory
    HAVING COUNT(*) >= 4
    ORDER BY COUNT(*) DESC
  `);
  
  console.log(`  Processing ${categories.rows.length} categories for ranking...`);
  
  let totalRanked = 0;
  
  for (const category of categories.rows) {
    try {
      const categoryName = category.category_name;
      console.log(`\n    Processing category: ${categoryName} (${category.fund_count} funds)`);
      
      // Get funds in this category sorted by total score
      const categoryFunds = await pool.query(`
        SELECT 
          fs.fund_id,
          fs.total_score,
          f.fund_name
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE fs.score_date = CURRENT_DATE
          AND fs.total_score IS NOT NULL
          AND f.subcategory = $1
        ORDER BY fs.total_score DESC
      `, [categoryName]);
      
      // Calculate ranks and quartiles
      const funds = categoryFunds.rows;
      const fundCount = funds.length;
      
      // Clear existing rankings for this category
      await pool.query(`
        DELETE FROM quartile_rankings 
        WHERE category_name = $1 AND calculation_date = CURRENT_DATE
      `, [categoryName]);
      
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
        
        // Insert into quartile_rankings table
        await pool.query(`
          INSERT INTO quartile_rankings 
          (category_name, fund_id, total_score, category_rank, quartile, calculation_date)
          VALUES ($1, $2, $3, $4, $5, CURRENT_DATE)
          ON CONFLICT (category_name, fund_id, calculation_date)
          DO UPDATE SET 
            total_score = EXCLUDED.total_score,
            category_rank = EXCLUDED.category_rank,
            quartile = EXCLUDED.quartile
        `, [categoryName, fund.fund_id, fund.total_score, rank, quartile]);
        
        // Update fund_scores with category ranking info
        await pool.query(`
          UPDATE fund_scores SET
            category_table = $1,
            category_rank = $2,
            quartile = $3
          WHERE fund_id = $4 AND score_date = CURRENT_DATE
        `, [categoryName, rank, quartile, fund.fund_id]);
      }
      
      totalRanked += funds.length;
      console.log(`      ✓ Ranked ${funds.length} funds in quartiles`);
      
    } catch (error) {
      console.error(`      Error processing category ${category.category_name}:`, error.message);
    }
  }
  
  console.log(`\n  ✓ Total funds ranked: ${totalRanked} across ${categories.rows.length} categories`);
}

async function validateResults() {
  console.log('\n5. Validating Category Ranking Results...');
  
  // Final validation
  const validation = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN category_table IS NOT NULL THEN 1 END) as has_category_table,
      COUNT(CASE WHEN category_rank IS NOT NULL THEN 1 END) as has_category_rank,
      COUNT(CASE WHEN quartile IS NOT NULL THEN 1 END) as has_quartile,
      COUNT(CASE WHEN category_table IS NOT NULL AND category_rank IS NOT NULL AND quartile IS NOT NULL THEN 1 END) as complete_ranking
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const result = validation.rows[0];
  
  console.log('  Final Validation Results:');
  console.log(`    Total Funds: ${result.total_funds}`);
  console.log(`    Has category_table: ${result.has_category_table} (${Math.round(result.has_category_table/result.total_funds*100)}%)`);
  console.log(`    Has category_rank: ${result.has_category_rank} (${Math.round(result.has_category_rank/result.total_funds*100)}%)`);
  console.log(`    Has quartile: ${result.has_quartile} (${Math.round(result.has_quartile/result.total_funds*100)}%)`);
  console.log(`    Complete ranking: ${result.complete_ranking} (${Math.round(result.complete_ranking/result.total_funds*100)}%)`);
  
  // Show quartile distribution
  const quartileDistribution = await pool.query(`
    SELECT 
      quartile,
      COUNT(*) as fund_count,
      ROUND(AVG(total_score), 2) as avg_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND quartile IS NOT NULL
    GROUP BY quartile
    ORDER BY quartile
  `);
  
  console.log('\n  Quartile Distribution:');
  console.log('  Quartile'.padEnd(12) + 'Funds'.padEnd(8) + 'Avg Score');
  console.log('  ' + '-'.repeat(35));
  
  for (const q of quartileDistribution.rows) {
    const quartileName = ['', 'Top 25%', '2nd 25%', '3rd 25%', 'Bottom 25%'][q.quartile];
    console.log(
      `  ${quartileName}`.padEnd(12) +
      q.fund_count.toString().padEnd(8) +
      q.avg_score.toString()
    );
  }
  
  // Show top categories by fund count
  const topCategories = await pool.query(`
    SELECT 
      category_table,
      COUNT(*) as fund_count,
      COUNT(DISTINCT quartile) as quartile_spread
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND category_table IS NOT NULL
    GROUP BY category_table
    ORDER BY COUNT(*) DESC
    LIMIT 8
  `);
  
  console.log('\n  Top Categories by Fund Count:');
  console.log('  Category'.padEnd(25) + 'Funds'.padEnd(8) + 'Quartiles');
  console.log('  ' + '-'.repeat(45));
  
  for (const cat of topCategories.rows) {
    console.log(
      `  ${cat.category_table}`.padEnd(25) +
      cat.fund_count.toString().padEnd(8) +
      cat.quartile_spread.toString()
    );
  }
  
  if (result.complete_ranking === result.total_funds) {
    console.log('\n  ✓ SUCCESS: All funds have complete category rankings');
  } else {
    const unranked = result.total_funds - result.complete_ranking;
    console.log(`\n  ⚠️  ${unranked} funds still need category rankings (likely in small categories)`);
  }
}

if (require.main === module) {
  categoryRankingDeepDive()
    .then(() => {
      console.log('\n✓ Category ranking deep dive completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Category ranking analysis failed:', error);
      process.exit(1);
    });
}

module.exports = { categoryRankingDeepDive };