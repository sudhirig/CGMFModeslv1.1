/**
 * Implement Category-Based Scoring System
 * Restructures scoring to align with original documentation's category-level peer comparison
 */

const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

async function implementCategoryBasedScoringSystem() {
  try {
    console.log('🔄 IMPLEMENTING CATEGORY-BASED SCORING SYSTEM');
    console.log('='.repeat(60));
    
    // Step 1: Add category column to fund_scores_corrected for efficient queries
    console.log('\n1. Adding category column to fund_scores_corrected...');
    
    await pool.query(`
      ALTER TABLE fund_scores_corrected 
      ADD COLUMN IF NOT EXISTS category VARCHAR(50)
    `);
    
    // Populate category column from funds table
    await pool.query(`
      UPDATE fund_scores_corrected fsc
      SET category = f.category
      FROM funds f
      WHERE fsc.fund_id = f.id AND fsc.category IS NULL
    `);
    
    console.log('✓ Category column added and populated');
    
    // Step 2: Calculate category-based rankings and quartiles
    console.log('\n2. Calculating category-based rankings and quartiles...');
    
    const scoreDate = new Date().toISOString().split('T')[0];
    
    // Get category statistics
    const categoryStats = await pool.query(`
      SELECT 
        category,
        COUNT(*) as total_funds,
        AVG(total_score) as avg_score,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score
      FROM fund_scores_corrected 
      WHERE score_date = $1 AND total_score IS NOT NULL
      GROUP BY category
      ORDER BY total_funds DESC
    `, [scoreDate]);
    
    console.log('\nCategory Statistics:');
    categoryStats.rows.forEach(cat => {
      console.log(`  ${cat.category}: ${cat.total_funds} funds (${cat.min_score}-${cat.max_score}, avg: ${parseFloat(cat.avg_score).toFixed(2)})`);
    });
    
    // Step 3: Implement category-based quartile calculation
    console.log('\n3. Implementing category-based quartile system...');
    
    for (const category of categoryStats.rows) {
      if (parseInt(category.total_funds) < 4) {
        console.log(`  Skipping ${category.category} - insufficient funds for quartiles`);
        continue;
      }
      
      console.log(`  Processing ${category.category} (${category.total_funds} funds)...`);
      
      // Calculate category-specific rankings and quartiles
      await pool.query(`
        WITH category_rankings AS (
          SELECT 
            fund_id,
            total_score,
            ROW_NUMBER() OVER (ORDER BY total_score DESC) as category_rank,
            COUNT(*) OVER () as category_total,
            PERCENT_RANK() OVER (ORDER BY total_score DESC) as category_percentile
          FROM fund_scores_corrected 
          WHERE score_date = $1 AND category = $2 AND total_score IS NOT NULL
        )
        UPDATE fund_scores_corrected 
        SET 
          category_rank = cr.category_rank,
          category_total = cr.category_total,
          quartile = CASE 
            WHEN cr.category_percentile <= 0.25 THEN 1
            WHEN cr.category_percentile <= 0.50 THEN 2
            WHEN cr.category_percentile <= 0.75 THEN 3
            ELSE 4
          END
        FROM category_rankings cr
        WHERE fund_scores_corrected.fund_id = cr.fund_id 
          AND fund_scores_corrected.score_date = $1
          AND fund_scores_corrected.category = $2
      `, [scoreDate, category.category]);
    }
    
    console.log('✓ Category-based quartiles calculated');
    
    // Step 4: Update recommendation logic based on category quartiles
    console.log('\n4. Updating recommendation logic based on category quartiles...');
    
    await pool.query(`
      UPDATE fund_scores_corrected 
      SET recommendation = CASE quartile
        WHEN 1 THEN 
          CASE 
            WHEN category_rank <= GREATEST(1, ROUND(category_total * 0.10)) THEN 'STRONG_BUY'
            ELSE 'BUY'
          END
        WHEN 2 THEN 'HOLD'
        WHEN 3 THEN 
          CASE 
            WHEN category_rank >= LEAST(category_total, ROUND(category_total * 0.90)) THEN 'SELL'
            ELSE 'HOLD'
          END
        WHEN 4 THEN 
          CASE 
            WHEN category_rank >= LEAST(category_total, ROUND(category_total * 0.99)) THEN 'STRONG_SELL'
            ELSE 'SELL'
          END
        ELSE 'HOLD'
      END
      WHERE score_date = $1 AND quartile IS NOT NULL
    `, [scoreDate]);
    
    console.log('✓ Recommendation logic updated based on category quartiles');
    
    // Step 5: Validate category-based distribution
    console.log('\n5. Validating category-based quartile distribution...');
    
    const categoryDistribution = await pool.query(`
      SELECT 
        category,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN quartile = 1 THEN 1 END) as q1_count,
        COUNT(CASE WHEN quartile = 2 THEN 1 END) as q2_count,
        COUNT(CASE WHEN quartile = 3 THEN 1 END) as q3_count,
        COUNT(CASE WHEN quartile = 4 THEN 1 END) as q4_count,
        ROUND(COUNT(CASE WHEN quartile = 1 THEN 1 END) * 100.0 / COUNT(*), 1) as q1_percent,
        ROUND(COUNT(CASE WHEN quartile = 2 THEN 1 END) * 100.0 / COUNT(*), 1) as q2_percent,
        ROUND(COUNT(CASE WHEN quartile = 3 THEN 1 END) * 100.0 / COUNT(*), 1) as q3_percent,
        ROUND(COUNT(CASE WHEN quartile = 4 THEN 1 END) * 100.0 / COUNT(*), 1) as q4_percent
      FROM fund_scores_corrected 
      WHERE score_date = $1 AND quartile IS NOT NULL
      GROUP BY category
      ORDER BY total_funds DESC
    `, [scoreDate]);
    
    console.log('\nCategory-Based Quartile Distribution:');
    categoryDistribution.rows.forEach(cat => {
      console.log(`  ${cat.category}:`);
      console.log(`    Total: ${cat.total_funds} funds`);
      console.log(`    Q1: ${cat.q1_count} (${cat.q1_percent}%) | Q2: ${cat.q2_count} (${cat.q2_percent}%) | Q3: ${cat.q3_count} (${cat.q3_percent}%) | Q4: ${cat.q4_count} (${cat.q4_percent}%)`);
    });
    
    // Step 6: Validate overall system health
    const overallValidation = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN quartile IS NOT NULL THEN 1 END) as funds_with_quartiles,
        COUNT(CASE WHEN category_rank IS NOT NULL THEN 1 END) as funds_with_category_rank,
        COUNT(CASE WHEN recommendation IS NOT NULL THEN 1 END) as funds_with_recommendations,
        AVG(total_score)::numeric(5,2) as avg_total_score,
        
        -- Category distribution
        COUNT(CASE WHEN quartile = 1 THEN 1 END) as overall_q1,
        COUNT(CASE WHEN quartile = 2 THEN 1 END) as overall_q2,
        COUNT(CASE WHEN quartile = 3 THEN 1 END) as overall_q3,
        COUNT(CASE WHEN quartile = 4 THEN 1 END) as overall_q4,
        
        -- Recommendation distribution
        COUNT(CASE WHEN recommendation = 'STRONG_BUY' THEN 1 END) as strong_buy_count,
        COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy_count,
        COUNT(CASE WHEN recommendation = 'HOLD' THEN 1 END) as hold_count,
        COUNT(CASE WHEN recommendation = 'SELL' THEN 1 END) as sell_count,
        COUNT(CASE WHEN recommendation = 'STRONG_SELL' THEN 1 END) as strong_sell_count
        
      FROM fund_scores_corrected 
      WHERE score_date = $1
    `, [scoreDate]);
    
    const validation = overallValidation.rows[0];
    
    console.log('\n=== CATEGORY-BASED SYSTEM VALIDATION ===');
    console.log(`Total Funds: ${validation.total_funds}`);
    console.log(`Funds with Quartiles: ${validation.funds_with_quartiles} (${((validation.funds_with_quartiles/validation.total_funds)*100).toFixed(1)}%)`);
    console.log(`Funds with Category Rankings: ${validation.funds_with_category_rank}`);
    console.log(`Funds with Recommendations: ${validation.funds_with_recommendations}`);
    console.log(`Average Total Score: ${validation.avg_total_score}`);
    
    console.log('\nOverall Quartile Distribution:');
    console.log(`Q1 (Top): ${validation.overall_q1} funds`);
    console.log(`Q2 (Above Avg): ${validation.overall_q2} funds`);
    console.log(`Q3 (Below Avg): ${validation.overall_q3} funds`);
    console.log(`Q4 (Bottom): ${validation.overall_q4} funds`);
    
    console.log('\nRecommendation Distribution:');
    console.log(`STRONG_BUY: ${validation.strong_buy_count}`);
    console.log(`BUY: ${validation.buy_count}`);
    console.log(`HOLD: ${validation.hold_count}`);
    console.log(`SELL: ${validation.sell_count}`);
    console.log(`STRONG_SELL: ${validation.strong_sell_count}`);
    
    // Step 7: Test category-based sample queries
    console.log('\n7. Testing category-based sample queries...');
    
    // Top performers by category
    const topPerformersByCategory = await pool.query(`
      SELECT 
        f.category,
        f.fund_name,
        fsc.total_score,
        fsc.category_rank,
        fsc.quartile,
        fsc.recommendation
      FROM fund_scores_corrected fsc
      JOIN funds f ON fsc.fund_id = f.id
      WHERE fsc.score_date = $1 AND fsc.category_rank <= 3
      ORDER BY f.category, fsc.category_rank
    `, [scoreDate]);
    
    console.log('\nTop 3 Performers by Category:');
    let currentCategory = '';
    topPerformersByCategory.rows.forEach(fund => {
      if (fund.category !== currentCategory) {
        console.log(`\n${fund.category}:`);
        currentCategory = fund.category;
      }
      console.log(`  ${fund.category_rank}. ${fund.fund_name} (Score: ${fund.total_score}, Q${fund.quartile}, ${fund.recommendation})`);
    });
    
    console.log('\n✅ CATEGORY-BASED SCORING SYSTEM IMPLEMENTATION COMPLETE');
    console.log('\nKey Changes Made:');
    console.log('• Added category column to fund_scores_corrected table');
    console.log('• Implemented category-based quartile calculation');
    console.log('• Updated recommendation logic based on category performance');
    console.log('• Validated proper quartile distribution within each category');
    console.log('• Aligned system with original documentation specifications');
    
    console.log('\n📊 System now uses category-level peer comparison as intended in original framework');
    
  } catch (error) {
    console.error('❌ Error implementing category-based scoring system:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

// Execute the implementation
implementCategoryBasedScoringSystem()
  .then(() => {
    console.log('\n🎉 Category-based scoring system implementation completed successfully!');
    process.exit(0);
  })
  .catch(error => {
    console.error('💥 Implementation failed:', error);
    process.exit(1);
  });