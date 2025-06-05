/**
 * Implement Subcategory Quartile System
 * Updates the scoring system to analyze 25 subcategories for precise peer comparisons
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementSubcategoryQuartiles() {
  try {
    console.log('=== Implementing Subcategory Quartile System ===');
    
    // Define all eligible subcategories based on analysis
    const subcategoryConfig = {
      'Debt': [
        'Liquid', 'Overnight', 'Ultra Short Duration', 'Short Duration',
        'Banking and PSU', 'Gilt', 'Corporate Bond', 'Dynamic Bond',
        'Credit Risk', 'Long Duration', 'Medium Duration'
      ],
      'Equity': [
        'Index', 'Flexi Cap', 'ELSS', 'Mid Cap', 'Multi Cap',
        'Value', 'Focused', 'Small Cap', 'Large Cap'
      ],
      'Hybrid': [
        'Balanced', 'Aggressive', 'Balanced Advantage', 
        'Conservative', 'Multi Asset'
      ]
    };
    
    console.log(`Processing ${Object.values(subcategoryConfig).flat().length} subcategories...`);
    
    // Update fund_scores table to support subcategory analysis
    await pool.query(`
      ALTER TABLE fund_scores 
      ADD COLUMN IF NOT EXISTS subcategory VARCHAR(100),
      ADD COLUMN IF NOT EXISTS subcategory_rank INTEGER,
      ADD COLUMN IF NOT EXISTS subcategory_total INTEGER,
      ADD COLUMN IF NOT EXISTS subcategory_quartile INTEGER,
      ADD COLUMN IF NOT EXISTS subcategory_percentile DECIMAL(5,2)
    `);
    
    console.log('✓ Updated fund_scores table structure');
    
    // Process each subcategory
    let totalProcessed = 0;
    const subcategoryResults = [];
    
    for (const [category, subcategories] of Object.entries(subcategoryConfig)) {
      console.log(`\n--- Processing ${category} Category ---`);
      
      for (const subcategory of subcategories) {
        console.log(`\nProcessing ${subcategory} subcategory...`);
        
        // Get eligible funds in this subcategory
        const eligibleFunds = await pool.query(`
          SELECT f.id, f.fund_name, f.category, f.subcategory
          FROM funds f
          JOIN (
            SELECT fund_id, COUNT(*) as nav_count
            FROM nav_data 
            WHERE created_at > '2025-05-30 06:45:00'
            GROUP BY fund_id
            HAVING COUNT(*) >= 252
          ) nav_summary ON f.id = nav_summary.fund_id
          WHERE f.category = $1 AND f.subcategory = $2
          ORDER BY f.id
        `, [category, subcategory]);
        
        const fundCount = eligibleFunds.rows.length;
        console.log(`  Found ${fundCount} eligible funds`);
        
        if (fundCount < 4) {
          console.log(`  Skipping - insufficient funds for quartile analysis`);
          continue;
        }
        
        // Score funds in this subcategory using existing scoring system
        let scoredCount = 0;
        const fundScores = [];
        
        for (const fund of eligibleFunds.rows) {
          try {
            // Check if fund needs scoring
            const existingScore = await pool.query(`
              SELECT total_score FROM fund_scores 
              WHERE fund_id = $1 AND score_date >= CURRENT_DATE - INTERVAL '7 days'
            `, [fund.id]);
            
            let totalScore;
            if (existingScore.rows.length > 0) {
              totalScore = parseFloat(existingScore.rows[0].total_score);
            } else {
              // Calculate new score for this fund
              totalScore = await calculateSubcategoryScore(fund.id, subcategory);
              if (totalScore === null) continue;
            }
            
            fundScores.push({
              fundId: fund.id,
              fundName: fund.fund_name,
              totalScore
            });
            
            scoredCount++;
            
            if (scoredCount % 10 === 0) {
              console.log(`    Scored ${scoredCount}/${fundCount} funds`);
            }
            
          } catch (error) {
            console.error(`    Error scoring fund ${fund.id}: ${error.message}`);
          }
        }
        
        // Calculate subcategory quartiles
        if (fundScores.length >= 4) {
          fundScores.sort((a, b) => b.totalScore - a.totalScore);
          
          const quartileSize = Math.ceil(fundScores.length / 4);
          
          for (let i = 0; i < fundScores.length; i++) {
            const fund = fundScores[i];
            const rank = i + 1;
            const percentile = ((fundScores.length - i) / fundScores.length) * 100;
            
            let quartile;
            if (i < quartileSize) quartile = 1;
            else if (i < quartileSize * 2) quartile = 2;
            else if (i < quartileSize * 3) quartile = 3;
            else quartile = 4;
            
            // Update fund_scores with subcategory rankings
            await pool.query(`
              UPDATE fund_scores 
              SET 
                subcategory = $1,
                subcategory_rank = $2,
                subcategory_total = $3,
                subcategory_quartile = $4,
                subcategory_percentile = $5
              WHERE fund_id = $6 AND score_date = CURRENT_DATE
            `, [subcategory, rank, fundScores.length, quartile, percentile, fund.fundId]);
          }
          
          // Generate subcategory summary
          const q1Count = fundScores.filter((_, i) => i < quartileSize).length;
          const q2Count = fundScores.filter((_, i) => i >= quartileSize && i < quartileSize * 2).length;
          const q3Count = fundScores.filter((_, i) => i >= quartileSize * 2 && i < quartileSize * 3).length;
          const q4Count = fundScores.filter((_, i) => i >= quartileSize * 3).length;
          
          const avgScore = fundScores.reduce((sum, f) => sum + f.totalScore, 0) / fundScores.length;
          
          subcategoryResults.push({
            category,
            subcategory,
            totalFunds: fundScores.length,
            avgScore: Math.round(avgScore * 100) / 100,
            q1Count,
            q2Count,
            q3Count,
            q4Count,
            topFund: fundScores[0].fundName
          });
          
          console.log(`  ✓ Completed: ${fundScores.length} funds ranked, avg score: ${avgScore.toFixed(2)}`);
          totalProcessed += fundScores.length;
        }
        
        // Brief pause between subcategories
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    // Generate comprehensive summary
    console.log('\n=== Subcategory Quartile Implementation Summary ===');
    console.log('Subcategory'.padEnd(25) + 'Funds'.padEnd(8) + 'Avg Score'.padEnd(12) + 'Q1'.padEnd(5) + 'Q2'.padEnd(5) + 'Q3'.padEnd(5) + 'Q4'.padEnd(5) + 'Top Fund');
    console.log('-'.repeat(100));
    
    for (const result of subcategoryResults) {
      console.log(
        `${result.category}/${result.subcategory}`.padEnd(25) + 
        result.totalFunds.toString().padEnd(8) + 
        result.avgScore.toString().padEnd(12) + 
        result.q1Count.toString().padEnd(5) + 
        result.q2Count.toString().padEnd(5) + 
        result.q3Count.toString().padEnd(5) + 
        result.q4Count.toString().padEnd(5) + 
        result.topFund.substring(0, 35)
      );
    }
    
    console.log('-'.repeat(100));
    console.log(`Total: ${subcategoryResults.length} subcategories processed, ${totalProcessed} funds ranked`);
    
    // Update database metadata
    await pool.query(`
      INSERT INTO automation_config (config_name, config_data, created_at)
      VALUES ('subcategory_quartiles', $1, NOW())
      ON CONFLICT (config_name) DO UPDATE SET
        config_data = EXCLUDED.config_data,
        updated_at = NOW()
    `, [JSON.stringify({
      implementation_date: new Date().toISOString(),
      subcategories_processed: subcategoryResults.length,
      total_funds_ranked: totalProcessed,
      subcategory_config: subcategoryConfig
    })]);
    
    console.log('\n✓ Subcategory quartile system implementation completed successfully!');
    
    return {
      success: true,
      subcategoriesProcessed: subcategoryResults.length,
      totalFundsRanked: totalProcessed,
      results: subcategoryResults
    };
    
  } catch (error) {
    console.error('Error implementing subcategory quartiles:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

// Calculate score for a fund within its subcategory context
async function calculateSubcategoryScore(fundId, subcategory) {
  try {
    // Get NAV data for the fund
    const navData = await pool.query(`
      SELECT nav_value, nav_date 
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
      ORDER BY nav_date DESC 
      LIMIT 1000
    `, [fundId]);
    
    if (navData.rows.length < 252) return null;
    
    const navValues = navData.rows.map(row => parseFloat(row.nav_value));
    const latest = navValues[0];
    
    // Calculate returns for different periods
    const returns = {
      '3m': navValues.length >= 90 ? ((latest - navValues[89]) / navValues[89]) * 100 : null,
      '6m': navValues.length >= 180 ? ((latest - navValues[179]) / navValues[179]) * 100 : null,
      '1y': navValues.length >= 252 ? ((latest - navValues[251]) / navValues[251]) * 100 : null
    };
    
    // Subcategory-specific scoring adjustments
    let baseScore = 60;
    let bonusScore = 0;
    
    // Debt subcategory scoring
    if (['Liquid', 'Overnight'].includes(subcategory)) {
      if (returns['1y'] && returns['1y'] > 3) bonusScore += 20;
      if (returns['6m'] && returns['6m'] > 2) bonusScore += 10;
      baseScore = 70; // Higher base for liquid funds
    } else if (['Ultra Short Duration', 'Short Duration'].includes(subcategory)) {
      if (returns['1y'] && returns['1y'] > 5) bonusScore += 20;
      if (returns['1y'] && returns['1y'] > 7) bonusScore += 10;
    } else if (['Credit Risk', 'Corporate Bond'].includes(subcategory)) {
      if (returns['1y'] && returns['1y'] > 8) bonusScore += 25;
      if (returns['3y'] && returns['3y'] > 6) bonusScore += 15;
    }
    
    // Equity subcategory scoring
    else if (['Large Cap'].includes(subcategory)) {
      if (returns['1y'] && returns['1y'] > 12) bonusScore += 25;
      if (returns['1y'] && returns['1y'] > 15) bonusScore += 15;
    } else if (['Mid Cap', 'Small Cap'].includes(subcategory)) {
      if (returns['1y'] && returns['1y'] > 15) bonusScore += 30;
      if (returns['1y'] && returns['1y'] > 20) bonusScore += 15;
    } else if (['Index'].includes(subcategory)) {
      if (returns['1y'] && returns['1y'] > 10) bonusScore += 20;
      baseScore = 75; // Higher base for index funds (lower fees)
    }
    
    const totalScore = Math.min(100, baseScore + bonusScore);
    
    // Store the score
    await pool.query(`
      INSERT INTO fund_scores (
        fund_id, score_date, subcategory,
        return_1y_score, total_score, created_at
      ) VALUES ($1, CURRENT_DATE, $2, $3, $4, NOW())
      ON CONFLICT (fund_id, score_date) DO UPDATE SET
        subcategory = EXCLUDED.subcategory,
        return_1y_score = EXCLUDED.return_1y_score,
        total_score = EXCLUDED.total_score
    `, [fundId, subcategory, returns['1y'] || 0, totalScore]);
    
    return totalScore;
    
  } catch (error) {
    console.error(`Error calculating score for fund ${fundId}:`, error.message);
    return null;
  }
}

if (require.main === module) {
  implementSubcategoryQuartiles()
    .then(result => {
      console.log('\n✓ Implementation completed:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('Implementation failed:', error);
      process.exit(1);
    });
}

module.exports = { implementSubcategoryQuartiles };