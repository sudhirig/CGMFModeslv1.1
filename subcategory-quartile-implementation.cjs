/**
 * Subcategory Quartile Implementation
 * Efficient implementation using existing scoring system with subcategory enhancements
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementSubcategoryQuartiles() {
  try {
    console.log('=== Implementing Subcategory Quartile System ===');
    
    // Process subcategories in phases for better performance
    const phases = [
      // Phase 1: High-volume debt subcategories
      {
        name: 'High-Volume Debt',
        subcategories: [
          { category: 'Debt', subcategory: 'Liquid' },
          { category: 'Debt', subcategory: 'Overnight' },
          { category: 'Debt', subcategory: 'Ultra Short Duration' }
        ]
      },
      // Phase 2: Major equity subcategories  
      {
        name: 'Major Equity',
        subcategories: [
          { category: 'Equity', subcategory: 'Index' },
          { category: 'Equity', subcategory: 'Large Cap' },
          { category: 'Equity', subcategory: 'Mid Cap' }
        ]
      },
      // Phase 3: Specialized categories
      {
        name: 'Specialized',
        subcategories: [
          { category: 'Debt', subcategory: 'Banking and PSU' },
          { category: 'Equity', subcategory: 'ELSS' },
          { category: 'Hybrid', subcategory: 'Balanced' }
        ]
      }
    ];
    
    let totalProcessed = 0;
    const results = [];
    
    for (const phase of phases) {
      console.log(`\n--- Phase: ${phase.name} ---`);
      
      for (const { category, subcategory } of phase.subcategories) {
        console.log(`\nProcessing ${category}/${subcategory}...`);
        
        // Use existing scoring with subcategory context
        const result = await processSubcategoryScoring(category, subcategory);
        
        if (result) {
          results.push(result);
          totalProcessed += result.fundsProcessed;
          
          console.log(`  ✓ ${subcategory}: ${result.fundsProcessed} funds, avg score: ${result.avgScore}`);
        }
        
        // Brief pause between subcategories
        await new Promise(resolve => setTimeout(resolve, 3000));
      }
    }
    
    // Generate final quartile assignments across all processed subcategories
    await assignFinalQuartiles();
    
    // Summary
    console.log('\n=== Subcategory Implementation Summary ===');
    console.log('Subcategory'.padEnd(30) + 'Funds'.padEnd(8) + 'Avg Score'.padEnd(12) + 'Top Quartile');
    console.log('-'.repeat(65));
    
    for (const result of results) {
      console.log(
        `${result.category}/${result.subcategory}`.padEnd(30) + 
        result.fundsProcessed.toString().padEnd(8) + 
        result.avgScore.toString().padEnd(12) + 
        result.topQuartileCount.toString()
      );
    }
    
    console.log('-'.repeat(65));
    console.log(`Total: ${results.length} subcategories, ${totalProcessed} funds processed`);
    
    return {
      success: true,
      subcategoriesProcessed: results.length,
      totalFundsProcessed: totalProcessed
    };
    
  } catch (error) {
    console.error('Error implementing subcategory quartiles:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function processSubcategoryScoring(category, subcategory) {
  try {
    // Get top eligible funds in this subcategory
    const eligibleFunds = await pool.query(`
      SELECT f.id, f.fund_name 
      FROM funds f
      JOIN (
        SELECT fund_id, COUNT(*) as nav_count
        FROM nav_data 
        WHERE created_at > '2025-05-30 06:45:00'
        GROUP BY fund_id
        HAVING COUNT(*) >= 252
      ) nav_summary ON f.id = nav_summary.fund_id
      WHERE f.category = $1 AND f.subcategory = $2
      ORDER BY nav_summary.nav_count DESC
      LIMIT 25
    `, [category, subcategory]);
    
    if (eligibleFunds.rows.length < 4) {
      console.log(`    Insufficient funds (${eligibleFunds.rows.length}) for ${subcategory}`);
      return null;
    }
    
    const scores = [];
    let processedCount = 0;
    
    for (const fund of eligibleFunds.rows) {
      try {
        // Calculate subcategory-aware score
        const score = await calculateSubcategoryScore(fund.id, category, subcategory);
        
        if (score !== null) {
          scores.push({
            fundId: fund.id,
            fundName: fund.fund_name,
            score: score
          });
          processedCount++;
        }
        
      } catch (error) {
        console.error(`    Error scoring fund ${fund.id}: ${error.message}`);
      }
    }
    
    if (scores.length === 0) return null;
    
    // Calculate statistics
    const avgScore = scores.reduce((sum, s) => sum + s.score, 0) / scores.length;
    const topQuartileCount = Math.ceil(scores.length / 4);
    
    return {
      category,
      subcategory,
      fundsProcessed: processedCount,
      avgScore: Math.round(avgScore * 100) / 100,
      topQuartileCount
    };
    
  } catch (error) {
    console.error(`Error processing ${category}/${subcategory}:`, error);
    return null;
  }
}

async function calculateSubcategoryScore(fundId, category, subcategory) {
  try {
    // Get recent NAV data
    const navData = await pool.query(`
      SELECT nav_value, nav_date 
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
      ORDER BY nav_date DESC 
      LIMIT 500
    `, [fundId]);
    
    if (navData.rows.length < 252) return null;
    
    const navValues = navData.rows.map(row => parseFloat(row.nav_value));
    const latest = navValues[0];
    
    // Calculate 1-year return
    const oneYearNav = navValues[Math.min(252, navValues.length - 1)];
    const oneYearReturn = ((latest - oneYearNav) / oneYearNav) * 100;
    
    // Base scoring with subcategory adjustments
    let score = 60; // Base score
    
    // Debt subcategory scoring
    if (category === 'Debt') {
      if (subcategory === 'Liquid' || subcategory === 'Overnight') {
        if (oneYearReturn > 4) score += 25;
        else if (oneYearReturn > 3) score += 15;
        score += 5; // Bonus for safety
      } else if (subcategory === 'Ultra Short Duration') {
        if (oneYearReturn > 6) score += 25;
        else if (oneYearReturn > 4) score += 15;
      } else if (subcategory === 'Banking and PSU') {
        if (oneYearReturn > 8) score += 30;
        else if (oneYearReturn > 6) score += 20;
      }
    }
    
    // Equity subcategory scoring
    else if (category === 'Equity') {
      if (subcategory === 'Index') {
        if (oneYearReturn > 12) score += 25;
        else if (oneYearReturn > 8) score += 15;
        score += 10; // Bonus for low cost
      } else if (subcategory === 'Large Cap') {
        if (oneYearReturn > 15) score += 30;
        else if (oneYearReturn > 10) score += 20;
      } else if (subcategory === 'Mid Cap') {
        if (oneYearReturn > 20) score += 35;
        else if (oneYearReturn > 15) score += 25;
      } else if (subcategory === 'ELSS') {
        if (oneYearReturn > 18) score += 30;
        else if (oneYearReturn > 12) score += 20;
        score += 5; // Tax benefit bonus
      }
    }
    
    // Hybrid subcategory scoring
    else if (category === 'Hybrid') {
      if (subcategory === 'Balanced') {
        if (oneYearReturn > 12) score += 25;
        else if (oneYearReturn > 8) score += 15;
        score += 5; // Stability bonus
      }
    }
    
    const finalScore = Math.min(100, Math.max(0, score));
    
    // Store score with subcategory info
    await pool.query(`
      INSERT INTO fund_scores (
        fund_id, score_date, 
        subcategory, return_1y_score, total_score, 
        created_at
      ) VALUES ($1, CURRENT_DATE, $2, $3, $4, NOW())
      ON CONFLICT (fund_id, score_date) DO UPDATE SET
        subcategory = EXCLUDED.subcategory,
        return_1y_score = EXCLUDED.return_1y_score,
        total_score = EXCLUDED.total_score
    `, [fundId, subcategory, oneYearReturn, finalScore]);
    
    return finalScore;
    
  } catch (error) {
    console.error(`Error calculating score for fund ${fundId}:`, error.message);
    return null;
  }
}

async function assignFinalQuartiles() {
  try {
    console.log('\n--- Assigning Final Quartiles ---');
    
    // Update quartiles and recommendations based on scores
    const quartileUpdate = await pool.query(`
      UPDATE fund_scores 
      SET 
        quartile = (
          CASE 
            WHEN total_score >= 85 THEN 1
            WHEN total_score >= 70 THEN 2  
            WHEN total_score >= 55 THEN 3
            ELSE 4
          END
        ),
        recommendation = (
          CASE 
            WHEN total_score >= 85 THEN 'STRONG_BUY'
            WHEN total_score >= 70 THEN 'BUY'
            WHEN total_score >= 55 THEN 'HOLD'
            ELSE 'SELL'
          END
        )
      WHERE score_date = CURRENT_DATE 
        AND subcategory IS NOT NULL
      RETURNING fund_id
    `);
    
    console.log(`✓ Updated quartiles for ${quartileUpdate.rows.length} funds`);
    
  } catch (error) {
    console.error('Error assigning final quartiles:', error);
  }
}

if (require.main === module) {
  implementSubcategoryQuartiles()
    .then(result => {
      console.log('\n✓ Subcategory implementation completed:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('Implementation failed:', error);
      process.exit(1);
    });
}

module.exports = { implementSubcategoryQuartiles };