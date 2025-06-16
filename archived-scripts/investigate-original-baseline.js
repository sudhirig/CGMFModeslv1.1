/**
 * Investigate Original Baseline Distribution
 * Find out what the original authentic recommendation distribution was before our changes
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function investigateOriginalBaseline() {
  console.log('Investigating Original Baseline Recommendation Distribution...\n');
  
  try {
    // Step 1: Apply the ORIGINAL documentation logic to see what it produces
    console.log('Testing original documentation logic from implement-recommendation-system.js...');
    
    // Create a temporary test to see what the original logic would produce
    const originalLogicTest = await pool.query(`
      SELECT 
        CASE 
          WHEN total_score >= 70 OR (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) THEN 'STRONG_BUY'
          WHEN total_score >= 60 OR (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20) THEN 'BUY'
          WHEN total_score >= 50 OR (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20) THEN 'HOLD'
          WHEN total_score >= 35 OR (total_score >= 30 AND risk_grade_total >= 15) THEN 'SELL'
          ELSE 'STRONG_SELL'
        END as original_recommendation,
        COUNT(*) as fund_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
      GROUP BY 1
      ORDER BY 
        CASE 
          WHEN (CASE 
            WHEN total_score >= 70 OR (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) THEN 'STRONG_BUY'
            WHEN total_score >= 60 OR (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20) THEN 'BUY'
            WHEN total_score >= 50 OR (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20) THEN 'HOLD'
            WHEN total_score >= 35 OR (total_score >= 30 AND risk_grade_total >= 15) THEN 'SELL'
            ELSE 'STRONG_SELL'
          END) = 'STRONG_BUY' THEN 1
          WHEN (CASE 
            WHEN total_score >= 70 OR (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) THEN 'STRONG_BUY'
            WHEN total_score >= 60 OR (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20) THEN 'BUY'
            WHEN total_score >= 50 OR (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20) THEN 'HOLD'
            WHEN total_score >= 35 OR (total_score >= 30 AND risk_grade_total >= 15) THEN 'SELL'
            ELSE 'STRONG_SELL'
          END) = 'BUY' THEN 2
          WHEN (CASE 
            WHEN total_score >= 70 OR (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) THEN 'STRONG_BUY'
            WHEN total_score >= 60 OR (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20) THEN 'BUY'
            WHEN total_score >= 50 OR (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20) THEN 'HOLD'
            WHEN total_score >= 35 OR (total_score >= 30 AND risk_grade_total >= 15) THEN 'SELL'
            ELSE 'STRONG_SELL'
          END) = 'HOLD' THEN 3
          WHEN (CASE 
            WHEN total_score >= 70 OR (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) THEN 'STRONG_BUY'
            WHEN total_score >= 60 OR (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20) THEN 'BUY'
            WHEN total_score >= 50 OR (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20) THEN 'HOLD'
            WHEN total_score >= 35 OR (total_score >= 30 AND risk_grade_total >= 15) THEN 'SELL'
            ELSE 'STRONG_SELL'
          END) = 'SELL' THEN 4
          ELSE 5
        END
    `);
    
    console.log('Original Documentation Logic Results:');
    let totalOriginal = 0;
    for (const row of originalLogicTest.rows) {
      totalOriginal += parseInt(row.fund_count);
      console.log(`${row.original_recommendation}: ${row.fund_count} funds (${row.percentage}%)`);
    }
    console.log(`Total: ${totalOriginal} funds\n`);
    
    // Step 2: Check score distribution to understand thresholds
    console.log('Score and component distribution analysis:');
    const scoreAnalysis = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN total_score >= 70 THEN 1 END) as score_70_plus,
        COUNT(CASE WHEN total_score >= 60 THEN 1 END) as score_60_plus,
        COUNT(CASE WHEN total_score >= 50 THEN 1 END) as score_50_plus,
        COUNT(CASE WHEN total_score >= 35 THEN 1 END) as score_35_plus,
        COUNT(CASE WHEN quartile = 1 THEN 1 END) as quartile_1,
        COUNT(CASE WHEN quartile IN (1,2) THEN 1 END) as quartile_1_2,
        COUNT(CASE WHEN quartile IN (1,2,3) THEN 1 END) as quartile_1_2_3,
        COUNT(CASE WHEN risk_grade_total >= 25 THEN 1 END) as risk_25_plus,
        COUNT(CASE WHEN risk_grade_total >= 20 THEN 1 END) as risk_20_plus,
        COUNT(CASE WHEN risk_grade_total >= 15 THEN 1 END) as risk_15_plus,
        COUNT(CASE WHEN fundamentals_total >= 20 THEN 1 END) as fundamentals_20_plus
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
    `);
    
    const analysis = scoreAnalysis.rows[0];
    console.log(`Total funds: ${analysis.total_funds}`);
    console.log(`Score >= 70: ${analysis.score_70_plus} funds`);
    console.log(`Score >= 60: ${analysis.score_60_plus} funds`);
    console.log(`Score >= 50: ${analysis.score_50_plus} funds`);
    console.log(`Score >= 35: ${analysis.score_35_plus} funds`);
    console.log(`Quartile 1: ${analysis.quartile_1} funds`);
    console.log(`Quartile 1-2: ${analysis.quartile_1_2} funds`);
    console.log(`Quartile 1-3: ${analysis.quartile_1_2_3} funds`);
    console.log(`Risk Grade >= 25: ${analysis.risk_25_plus} funds`);
    console.log(`Risk Grade >= 20: ${analysis.risk_20_plus} funds`);
    console.log(`Risk Grade >= 15: ${analysis.risk_15_plus} funds`);
    console.log(`Fundamentals >= 20: ${analysis.fundamentals_20_plus} funds\n`);
    
    // Step 3: Test different threshold combinations to understand original intent
    console.log('Testing simplified threshold combinations...');
    
    const simpleThresholds = [
      { name: 'Simple Score-Based', logic: 'total_score >= 75: STRONG_BUY, >= 60: BUY, >= 45: HOLD, >= 30: SELL, else: STRONG_SELL' },
      { name: 'Original Score-Based', logic: 'total_score >= 70: STRONG_BUY, >= 60: BUY, >= 50: HOLD, >= 35: SELL, else: STRONG_SELL' },
      { name: 'Conservative', logic: 'total_score >= 80: STRONG_BUY, >= 65: BUY, >= 50: HOLD, >= 35: SELL, else: STRONG_SELL' }
    ];
    
    for (const test of simpleThresholds) {
      let query = '';
      if (test.name === 'Simple Score-Based') {
        query = `
          SELECT 
            CASE 
              WHEN total_score >= 75 THEN 'STRONG_BUY'
              WHEN total_score >= 60 THEN 'BUY'
              WHEN total_score >= 45 THEN 'HOLD'
              WHEN total_score >= 30 THEN 'SELL'
              ELSE 'STRONG_SELL'
            END as recommendation,
            COUNT(*) as fund_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
          FROM fund_scores_corrected 
          WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
          GROUP BY 1
        `;
      } else if (test.name === 'Original Score-Based') {
        query = `
          SELECT 
            CASE 
              WHEN total_score >= 70 THEN 'STRONG_BUY'
              WHEN total_score >= 60 THEN 'BUY'
              WHEN total_score >= 50 THEN 'HOLD'
              WHEN total_score >= 35 THEN 'SELL'
              ELSE 'STRONG_SELL'
            END as recommendation,
            COUNT(*) as fund_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
          FROM fund_scores_corrected 
          WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
          GROUP BY 1
        `;
      } else {
        query = `
          SELECT 
            CASE 
              WHEN total_score >= 80 THEN 'STRONG_BUY'
              WHEN total_score >= 65 THEN 'BUY'
              WHEN total_score >= 50 THEN 'HOLD'
              WHEN total_score >= 35 THEN 'SELL'
              ELSE 'STRONG_SELL'
            END as recommendation,
            COUNT(*) as fund_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
          FROM fund_scores_corrected 
          WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
          GROUP BY 1
        `;
      }
      
      const result = await pool.query(query);
      console.log(`\n${test.name} Distribution:`);
      for (const row of result.rows) {
        console.log(`  ${row.recommendation}: ${row.fund_count} funds (${row.percentage}%)`);
      }
    }
    
    // Step 4: Check current fund_performance_metrics for comparison
    console.log('\n=== CURRENT STATE COMPARISON ===');
    
    const currentState = await pool.query(`
      SELECT 
        'fund_performance_metrics' as source,
        recommendation,
        COUNT(*) as fund_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
      FROM fund_performance_metrics 
      WHERE recommendation IS NOT NULL
      GROUP BY recommendation
      ORDER BY 
        CASE recommendation
          WHEN 'STRONG_BUY' THEN 1
          WHEN 'BUY' THEN 2
          WHEN 'HOLD' THEN 3
          WHEN 'SELL' THEN 4
          WHEN 'STRONG_SELL' THEN 5
        END
      
      UNION ALL
      
      SELECT 
        'fund_scores_corrected' as source,
        recommendation,
        COUNT(*) as fund_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND recommendation IS NOT NULL
      GROUP BY recommendation
      ORDER BY 
        CASE recommendation
          WHEN 'STRONG_BUY' THEN 1
          WHEN 'BUY' THEN 2
          WHEN 'HOLD' THEN 3
          WHEN 'SELL' THEN 4
          WHEN 'STRONG_SELL' THEN 5
        END
    `);
    
    console.log('Current State in Both Tables:');
    for (const row of currentState.rows) {
      console.log(`${row.source} - ${row.recommendation}: ${row.fund_count} funds (${row.percentage}%)`);
    }
    
    console.log('\n=== BASELINE INVESTIGATION COMPLETE ===');
    console.log('This shows what the original documentation logic would have produced');
    console.log('versus what we currently have after our modifications.');
    
  } catch (error) {
    console.error('Error investigating original baseline:', error);
  } finally {
    await pool.end();
  }
}

// Run the investigation
investigateOriginalBaseline().catch(console.error);