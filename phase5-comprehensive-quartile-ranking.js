/**
 * Phase 5: Comprehensive Category-Based Quartile Ranking
 * Target: Implement sophisticated subcategory-based quartile rankings
 * - subcategory_rank (position within subcategory)
 * - subcategory_quartile (1st, 2nd, 3rd, 4th quartile)
 * - category_rank (position within broader category)
 * - category_quartile (quartile within category)
 * - peer_percentile (percentile ranking among peers)
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementComprehensiveQuartileRanking() {
  try {
    console.log('Starting Phase 5: Comprehensive Category-Based Quartile Ranking...\n');
    
    let totalCategoriesProcessed = 0;
    let totalSubcategoriesProcessed = 0;
    let totalFundsRanked = 0;
    
    // Get all distinct categories and subcategories
    const categoriesData = await pool.query(`
      SELECT DISTINCT 
        f.category,
        f.subcategory,
        COUNT(*) as fund_count
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.overall_rating IS NOT NULL OR fs.return_1y_score IS NOT NULL
      GROUP BY f.category, f.subcategory
      HAVING COUNT(*) >= 10
      ORDER BY f.category, f.subcategory
    `);
    
    console.log(`Found ${categoriesData.rows.length} category-subcategory combinations to process`);
    
    for (const categoryData of categoriesData.rows) {
      const category = categoryData.category;
      const subCategory = categoryData.subcategory;
      const fundCount = parseInt(categoryData.fund_count);
      
      console.log(`\nProcessing: ${category} > ${subCategory} (${fundCount} funds)`);
      
      // Process subcategory rankings
      const subcategoryResult = await processSubcategoryRankings(category, subCategory);
      
      // Process broader category rankings
      const categoryResult = await processCategoryRankings(category);
      
      totalFundsRanked += subcategoryResult.rankedFunds;
      totalSubcategoriesProcessed++;
      
      console.log(`  ✓ Subcategory: ${subcategoryResult.rankedFunds} funds ranked`);
      console.log(`  ✓ Category: ${categoryResult.rankedFunds} funds updated`);
    }
    
    // Calculate cross-category percentiles
    console.log('\nCalculating cross-category percentiles...');
    const percentileResult = await calculateCrossCategoryPercentiles();
    
    // Generate comprehensive ranking report
    const finalReport = await generateComprehensiveRankingReport();
    
    console.log(`\n=== PHASE 5 COMPLETE: COMPREHENSIVE QUARTILE RANKING ===`);
    console.log(`Categories processed: ${totalCategoriesProcessed}`);
    console.log(`Subcategories processed: ${totalSubcategoriesProcessed}`);
    console.log(`Total funds ranked: ${totalFundsRanked}`);
    console.log(`Cross-category percentiles: ${percentileResult.totalFunds} funds`);
    console.log(`\nRanking Coverage by Category:`);
    finalReport.forEach(cat => {
      console.log(`  ${cat.category}: ${cat.ranked_funds}/${cat.total_funds} (${cat.coverage_pct}%)`);
    });
    
    return {
      success: true,
      totalFundsRanked,
      totalSubcategoriesProcessed,
      totalCategoriesProcessed,
      finalReport
    };
    
  } catch (error) {
    console.error('Error in comprehensive quartile ranking implementation:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function processSubcategoryRankings(category, subCategory) {
  try {
    // Get funds in this subcategory with available scores
    const subcategoryFunds = await pool.query(`
      SELECT 
        fs.fund_id,
        fs.overall_rating,
        fs.return_1y_score,
        fs.return_3y_score,
        fs.return_5y_score,
        fs.std_dev_1y_score,
        fs.max_drawdown_score,
        fs.consistency_score,
        fs.sharpe_ratio_score,
        -- Calculate composite ranking score
        COALESCE(
          (COALESCE(fs.overall_rating, 0) * 0.4 +
           COALESCE(fs.return_1y_score, 0) * 0.25 +
           COALESCE(fs.return_3y_score, 0) * 0.15 +
           COALESCE(fs.std_dev_1y_score, 0) * 0.1 +
           COALESCE(fs.consistency_score, 0) * 0.1),
          COALESCE(fs.return_1y_score, fs.return_3y_score, fs.return_5y_score, 50)
        ) as composite_score
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE f.category = $1 
      AND COALESCE(f.subcategory, 'General') = COALESCE($2, 'General')
      ORDER BY composite_score DESC, fs.return_1y_score DESC NULLS LAST
    `, [category, subCategory]);
    
    const funds = subcategoryFunds.rows;
    if (funds.length === 0) return { rankedFunds: 0 };
    
    // Calculate quartile boundaries
    const totalFunds = funds.length;
    const quartileSize = Math.ceil(totalFunds / 4);
    
    // Assign subcategory ranks and quartiles
    for (let i = 0; i < funds.length; i++) {
      const fund = funds[i];
      const rank = i + 1;
      const quartile = Math.min(4, Math.ceil(rank / quartileSize));
      const percentile = Math.round(((totalFunds - rank) / (totalFunds - 1)) * 100);
      
      await pool.query(`
        UPDATE fund_scores 
        SET 
          subcategory_rank = $1,
          subcategory_quartile = $2,
          subcategory_percentile = $3,
          subcategory_total_funds = $4,
          ranking_calculation_date = CURRENT_DATE
        WHERE fund_id = $5
      `, [rank, quartile, percentile, totalFunds, fund.fund_id]);
    }
    
    return { rankedFunds: funds.length };
    
  } catch (error) {
    console.error(`Error processing subcategory ${category} > ${subCategory}:`, error);
    return { rankedFunds: 0 };
  }
}

async function processCategoryRankings(category) {
  try {
    // Get all funds in this broader category
    const categoryFunds = await pool.query(`
      SELECT 
        fs.fund_id,
        fs.overall_rating,
        fs.return_1y_score,
        fs.return_3y_score,
        fs.return_5y_score,
        fs.std_dev_1y_score,
        -- Calculate category composite score
        COALESCE(
          (COALESCE(fs.overall_rating, 0) * 0.4 +
           COALESCE(fs.return_1y_score, 0) * 0.3 +
           COALESCE(fs.return_3y_score, 0) * 0.2 +
           COALESCE(fs.std_dev_1y_score, 0) * 0.1),
          COALESCE(fs.return_1y_score, fs.return_3y_score, fs.return_5y_score, 50)
        ) as category_composite_score
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE f.category = $1
      ORDER BY category_composite_score DESC, fs.return_1y_score DESC NULLS LAST
    `, [category]);
    
    const funds = categoryFunds.rows;
    if (funds.length === 0) return { rankedFunds: 0 };
    
    // Calculate category quartile boundaries
    const totalFunds = funds.length;
    const quartileSize = Math.ceil(totalFunds / 4);
    
    // Assign category ranks and quartiles
    for (let i = 0; i < funds.length; i++) {
      const fund = funds[i];
      const rank = i + 1;
      const quartile = Math.min(4, Math.ceil(rank / quartileSize));
      const percentile = Math.round(((totalFunds - rank) / (totalFunds - 1)) * 100);
      
      await pool.query(`
        UPDATE fund_scores 
        SET 
          category_rank = $1,
          category_quartile = $2,
          category_percentile = $3,
          category_total_funds = $4
        WHERE fund_id = $5
      `, [rank, quartile, percentile, totalFunds, fund.fund_id]);
    }
    
    return { rankedFunds: funds.length };
    
  } catch (error) {
    console.error(`Error processing category ${category}:`, error);
    return { rankedFunds: 0 };
  }
}

async function calculateCrossCategoryPercentiles() {
  try {
    // Calculate universe-wide percentiles for comprehensive comparison
    const allFunds = await pool.query(`
      SELECT 
        fs.fund_id,
        COALESCE(
          (COALESCE(fs.overall_rating, 0) * 0.5 +
           COALESCE(fs.return_1y_score, 0) * 0.3 +
           COALESCE(fs.return_3y_score, 0) * 0.2),
          COALESCE(fs.return_1y_score, fs.return_3y_score, fs.return_5y_score, 50)
        ) as universe_composite_score
      FROM fund_scores fs
      WHERE fs.overall_rating IS NOT NULL OR fs.return_1y_score IS NOT NULL
      ORDER BY universe_composite_score DESC
    `);
    
    const funds = allFunds.rows;
    const totalFunds = funds.length;
    
    // Assign universe percentiles
    for (let i = 0; i < funds.length; i++) {
      const fund = funds[i];
      const universeRank = i + 1;
      const universePercentile = Math.round(((totalFunds - universeRank) / (totalFunds - 1)) * 100);
      
      await pool.query(`
        UPDATE fund_scores 
        SET 
          universe_rank = $1,
          universe_percentile = $2,
          universe_total_funds = $3
        WHERE fund_id = $4
      `, [universeRank, universePercentile, totalFunds, fund.fund_id]);
    }
    
    return { totalFunds };
    
  } catch (error) {
    console.error('Error calculating cross-category percentiles:', error);
    return { totalFunds: 0 };
  }
}

async function generateComprehensiveRankingReport() {
  try {
    const reportData = await pool.query(`
      SELECT 
        f.category,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN fs.subcategory_rank IS NOT NULL THEN 1 END) as ranked_funds,
        ROUND(COUNT(CASE WHEN fs.subcategory_rank IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as coverage_pct,
        COUNT(DISTINCT COALESCE(f.subcategory, 'General')) as subcategories,
        AVG(CASE WHEN fs.overall_rating IS NOT NULL THEN fs.overall_rating END) as avg_rating,
        MAX(fs.subcategory_rank) as max_subcategory_rank
      FROM funds f
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id
      GROUP BY f.category
      ORDER BY ranked_funds DESC
    `);
    
    return reportData.rows;
    
  } catch (error) {
    console.error('Error generating comprehensive ranking report:', error);
    return [];
  }
}

implementComprehensiveQuartileRanking();