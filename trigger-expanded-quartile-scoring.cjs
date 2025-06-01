/**
 * Trigger Expanded Quartile Scoring
 * Processes all eligible fund categories for comprehensive quartile analysis
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function triggerExpandedQuartileScoring() {
  try {
    console.log('=== Starting Expanded Quartile Scoring ===');
    
    // Define expanded categories based on eligibility analysis
    const expandedCategories = [
      'Equity',      // 7,281 eligible funds
      'Debt',        // 2,792 eligible funds  
      'Other',       // 1,401 eligible funds
      'Hybrid',      // 322 eligible funds
      'Index Fund',  // 26 eligible funds
      'Fund of Funds', // 46 eligible funds
      'Gold ETF',    // 19 eligible funds
      'Silver ETF'   // 22 eligible funds
    ];
    
    console.log(`Processing ${expandedCategories.length} fund categories...`);
    
    // Process each category separately for better performance
    for (const category of expandedCategories) {
      console.log(`\n--- Processing ${category} Category ---`);
      
      // Get eligible funds in this category
      const eligibleFunds = await pool.query(`
        SELECT f.id, f.fund_name, COUNT(n.nav_date) as nav_count
        FROM funds f
        JOIN nav_data n ON f.id = n.fund_id
        WHERE f.category = $1 
          AND n.created_at > '2025-05-30 06:45:00'
        GROUP BY f.id, f.fund_name
        HAVING COUNT(n.nav_date) >= 252
        ORDER BY COUNT(n.nav_date) DESC
        LIMIT 50
      `, [category]);
      
      console.log(`Found ${eligibleFunds.rows.length} eligible funds in ${category}`);
      
      if (eligibleFunds.rows.length < 4) {
        console.log(`Skipping ${category} - insufficient funds for quartile analysis`);
        continue;
      }
      
      // Trigger scoring for this category batch
      let processedCount = 0;
      for (const fund of eligibleFunds.rows) {
        try {
          // Check if fund already has recent scores
          const existingScore = await pool.query(`
            SELECT id FROM fund_scores 
            WHERE fund_id = $1 AND score_date >= CURRENT_DATE - INTERVAL '7 days'
          `, [fund.id]);
          
          if (existingScore.rows.length === 0) {
            console.log(`  Scoring: ${fund.fund_name.substring(0, 50)}...`);
            
            // Call the scoring API endpoint
            const response = await fetch('http://localhost:5000/api/funds/score-batch', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ 
                category: category,
                limit: 1,
                fundIds: [fund.id]
              })
            });
            
            if (response.ok) {
              processedCount++;
              console.log(`    ✓ Scored (${processedCount}/${eligibleFunds.rows.length})`);
            } else {
              console.log(`    ✗ Scoring failed: ${response.statusText}`);
            }
            
            // Small delay to avoid overwhelming the system
            await new Promise(resolve => setTimeout(resolve, 2000));
          } else {
            console.log(`  Skipping: ${fund.fund_name.substring(0, 50)}... (recently scored)`);
          }
          
        } catch (error) {
          console.error(`Error scoring fund ${fund.id}:`, error.message);
        }
      }
      
      console.log(`${category} completed: ${processedCount} funds newly scored`);
      
      // Brief pause between categories
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
    
    // Final quartile assignment across all categories
    console.log('\n=== Assigning Final Quartile Rankings ===');
    
    const quarterAssignment = await pool.query(`
      UPDATE fund_scores 
      SET quartile = (
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
      RETURNING fund_id, quartile, recommendation
    `);
    
    console.log(`✓ Updated quartile assignments for ${quarterAssignment.rows.length} funds`);
    
    // Generate summary statistics
    const summary = await pool.query(`
      SELECT 
        COUNT(*) as total_scored,
        COUNT(DISTINCT f.category) as categories_processed,
        ROUND(AVG(fs.total_score), 2) as avg_score,
        COUNT(CASE WHEN fs.quartile = 1 THEN 1 END) as q1_count,
        COUNT(CASE WHEN fs.quartile = 2 THEN 1 END) as q2_count,
        COUNT(CASE WHEN fs.quartile = 3 THEN 1 END) as q3_count,
        COUNT(CASE WHEN fs.quartile = 4 THEN 1 END) as q4_count
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.score_date = CURRENT_DATE
    `);
    
    const stats = summary.rows[0];
    
    console.log('\n=== Expanded Quartile Scoring Summary ===');
    console.log(`Total funds scored: ${stats.total_scored}`);
    console.log(`Categories processed: ${stats.categories_processed}`);
    console.log(`Average score: ${stats.avg_score}`);
    console.log(`Quartile distribution:`);
    console.log(`  Q1 (STRONG_BUY): ${stats.q1_count} funds`);
    console.log(`  Q2 (BUY): ${stats.q2_count} funds`);
    console.log(`  Q3 (HOLD): ${stats.q3_count} funds`);
    console.log(`  Q4 (SELL): ${stats.q4_count} funds`);
    
    console.log('\n✓ Expanded quartile scoring completed successfully!');
    
    return stats;
    
  } catch (error) {
    console.error('Error in expanded quartile scoring:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  triggerExpandedQuartileScoring()
    .then(result => {
      console.log('\nScoring completed:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('Scoring failed:', error);
      process.exit(1);
    });
}

module.exports = { triggerExpandedQuartileScoring };