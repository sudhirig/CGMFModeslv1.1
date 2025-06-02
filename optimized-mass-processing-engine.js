/**
 * Optimized Mass Processing Engine
 * High-performance system for processing thousands of eligible funds
 * Maintains strict authentic data requirements at scale
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  max: 20,
  connectionTimeoutMillis: 3000,
  idleTimeoutMillis: 8000,
  query_timeout: 15000
});

async function optimizedMassProcessingEngine() {
  try {
    console.log('=== OPTIMIZED MASS PROCESSING ENGINE ===\n');
    
    const startTime = Date.now();
    let totalProcessed = 0;
    
    // Parallel processing of multiple phases
    const processingPromises = [
      processMassive5YReturns(),
      processMassiveYTDReturns(),
      processMassiveRiskMetrics(),
      processMassiveAdvancedRatios(),
      processMassiveQualityMetrics()
    ];
    
    console.log('Starting parallel mass processing across all phases...\n');
    
    const results = await Promise.allSettled(processingPromises);
    
    // Calculate totals
    results.forEach((result, index) => {
      const phaseNames = ['5Y Returns', 'YTD Returns', 'Risk Metrics', 'Advanced Ratios', 'Quality Metrics'];
      if (result.status === 'fulfilled') {
        totalProcessed += result.value.processed;
        console.log(`✅ ${phaseNames[index]}: ${result.value.processed} funds processed`);
      } else {
        console.log(`❌ ${phaseNames[index]}: Processing failed - ${result.reason}`);
      }
    });
    
    // Final comprehensive status
    const finalStatus = await pool.query(`
      SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as final_5y_coverage,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as final_ytd_coverage,
        COUNT(CASE WHEN updown_capture_1y_score IS NOT NULL THEN 1 END) as final_risk_coverage,
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as final_sharpe_coverage,
        COUNT(CASE WHEN aum_size_score IS NOT NULL THEN 1 END) as final_aum_coverage,
        COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as final_complete_scores,
        -- Data integrity verification
        COUNT(CASE WHEN return_5y_score = 50 OR return_ytd_score = 50 OR 
               return_6m_score = 50 OR return_3y_score = 50 OR 
               return_1y_score = 50 OR return_3m_score = 50 THEN 1 END) as synthetic_contamination,
        -- Coverage efficiency
        ROUND((COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END)::FLOAT / COUNT(*) * 100), 2) as coverage_5y_percent,
        ROUND((COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END)::FLOAT / COUNT(*) * 100), 2) as coverage_ytd_percent
      FROM fund_scores
    `);
    
    const final = finalStatus.rows[0];
    const processingTime = Math.round((Date.now() - startTime) / 1000);
    
    console.log('\n=== MASS PROCESSING RESULTS ===');
    console.log(`Processing time: ${processingTime} seconds`);
    console.log(`Total funds processed: ${totalProcessed}`);
    console.log(`Processing rate: ${Math.round(totalProcessed / processingTime * 60)} funds/minute`);
    
    console.log('\nFinal Coverage Status:');
    console.log(`- 5Y Returns: ${final.final_5y_coverage} funds (${final.coverage_5y_percent}%)`);
    console.log(`- YTD Returns: ${final.final_ytd_coverage} funds (${final.coverage_ytd_percent}%)`);
    console.log(`- Risk Metrics: ${final.final_risk_coverage} funds`);
    console.log(`- Advanced Ratios: ${final.final_sharpe_coverage} funds`);
    console.log(`- Quality Metrics: ${final.final_aum_coverage} funds`);
    console.log(`- Complete Scores: ${final.final_complete_scores} funds`);
    
    const integrityStatus = final.synthetic_contamination === '0' ? 'PERFECT' : 'COMPROMISED';
    console.log(`\nData Integrity: ${integrityStatus} (${final.synthetic_contamination} synthetic patterns)`);
    
    // Calculate remaining opportunities
    const remainingWork = await pool.query(`
      SELECT 
        COUNT(CASE WHEN EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 300
        ) AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.return_5y_score IS NOT NULL
        ) THEN 1 END) as remaining_5y_eligible,
        
        COUNT(CASE WHEN EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 30
        ) AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.return_ytd_score IS NOT NULL
        ) THEN 1 END) as remaining_ytd_eligible
      FROM funds f
    `);
    
    const remaining = remainingWork.rows[0];
    console.log(`\nRemaining Processing Opportunities:`);
    console.log(`- 5Y eligible: ${remaining.remaining_5y_eligible} funds`);
    console.log(`- YTD eligible: ${remaining.remaining_ytd_eligible} funds`);
    
    return {
      success: true,
      totalProcessed,
      processingTime,
      processingRate: Math.round(totalProcessed / processingTime * 60),
      finalCoverage: final,
      remainingWork: remaining,
      dataIntegrityMaintained: final.synthetic_contamination === '0'
    };
    
  } catch (error) {
    console.error('Error in mass processing engine:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function processMassive5YReturns() {
  try {
    console.log('Processing massive 5Y returns batch...');
    
    const batchResults = await pool.query(`
      WITH eligible_funds AS (
        SELECT f.id as fund_id, f.fund_name
        FROM funds f
        WHERE EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 250
        )
        AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.return_5y_score IS NOT NULL
        )
        LIMIT 1000
      ),
      nav_calculations AS (
        SELECT 
          ef.fund_id,
          (
            SELECT nav_value 
            FROM nav_data nd1 
            WHERE nd1.fund_id = ef.fund_id 
            AND nd1.nav_date <= CURRENT_DATE - INTERVAL '5 years'
            AND nd1.nav_value > 0
            ORDER BY nd1.nav_date ASC
            LIMIT 1
          ) as start_nav,
          (
            SELECT nav_value 
            FROM nav_data nd2 
            WHERE nd2.fund_id = ef.fund_id 
            AND nd2.nav_date >= CURRENT_DATE - INTERVAL '1 week'
            AND nd2.nav_value > 0
            ORDER BY nd2.nav_date DESC
            LIMIT 1
          ) as end_nav
        FROM eligible_funds ef
      ),
      score_calculations AS (
        SELECT 
          fund_id,
          CASE 
            WHEN start_nav > 0 AND end_nav > 0 THEN
              CASE 
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 25 THEN 100
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 20 THEN 95
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 15 THEN 85
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 10 THEN 75
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 5 THEN 65
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 0 THEN 55
                WHEN ((end_nav - start_nav) / start_nav * 100) >= -5 THEN 45
                WHEN ((end_nav - start_nav) / start_nav * 100) >= -10 THEN 35
                ELSE 25
              END
            ELSE NULL
          END as calculated_score
        FROM nav_calculations
        WHERE start_nav > 0 AND end_nav > 0
      )
      INSERT INTO fund_scores (fund_id, score_date, return_5y_score)
      SELECT fund_id, CURRENT_DATE, calculated_score
      FROM score_calculations
      WHERE calculated_score IS NOT NULL
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET return_5y_score = EXCLUDED.return_5y_score
      RETURNING fund_id
    `);
    
    console.log(`  5Y Returns: ${batchResults.rowCount} funds processed`);
    return { processed: batchResults.rowCount };
    
  } catch (error) {
    console.error('Error in massive 5Y processing:', error);
    return { processed: 0 };
  }
}

async function processMassiveYTDReturns() {
  try {
    console.log('Processing massive YTD returns batch...');
    
    const batchResults = await pool.query(`
      WITH eligible_funds AS (
        SELECT f.id as fund_id, f.fund_name
        FROM funds f
        WHERE EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 25
        )
        AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.return_ytd_score IS NOT NULL
        )
        LIMIT 800
      ),
      nav_calculations AS (
        SELECT 
          ef.fund_id,
          (
            SELECT nav_value 
            FROM nav_data nd1 
            WHERE nd1.fund_id = ef.fund_id 
            AND nd1.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
            AND nd1.nav_value > 0
            ORDER BY nd1.nav_date ASC
            LIMIT 1
          ) as start_nav,
          (
            SELECT nav_value 
            FROM nav_data nd2 
            WHERE nd2.fund_id = ef.fund_id 
            AND nd2.nav_date >= CURRENT_DATE - INTERVAL '1 week'
            AND nd2.nav_value > 0
            ORDER BY nd2.nav_date DESC
            LIMIT 1
          ) as end_nav
        FROM eligible_funds ef
      ),
      score_calculations AS (
        SELECT 
          fund_id,
          CASE 
            WHEN start_nav > 0 AND end_nav > 0 THEN
              CASE 
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 20 THEN 100
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 15 THEN 90
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 10 THEN 80
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 5 THEN 70
                WHEN ((end_nav - start_nav) / start_nav * 100) >= 0 THEN 60
                WHEN ((end_nav - start_nav) / start_nav * 100) >= -5 THEN 50
                WHEN ((end_nav - start_nav) / start_nav * 100) >= -10 THEN 40
                ELSE 30
              END
            ELSE NULL
          END as calculated_score
        FROM nav_calculations
        WHERE start_nav > 0 AND end_nav > 0
      )
      INSERT INTO fund_scores (fund_id, score_date, return_ytd_score)
      SELECT fund_id, CURRENT_DATE, calculated_score
      FROM score_calculations
      WHERE calculated_score IS NOT NULL
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET return_ytd_score = EXCLUDED.return_ytd_score
      RETURNING fund_id
    `);
    
    console.log(`  YTD Returns: ${batchResults.rowCount} funds processed`);
    return { processed: batchResults.rowCount };
    
  } catch (error) {
    console.error('Error in massive YTD processing:', error);
    return { processed: 0 };
  }
}

async function processMassiveRiskMetrics() {
  try {
    console.log('Processing massive risk metrics batch...');
    
    const batchResults = await pool.query(`
      WITH eligible_funds AS (
        SELECT f.id as fund_id
        FROM funds f
        WHERE EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '12 months'
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 120
        )
        AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.updown_capture_1y_score IS NOT NULL
        )
        LIMIT 600
      ),
      risk_calculations AS (
        SELECT 
          ef.fund_id,
          CASE 
            WHEN COUNT(CASE WHEN ret > 0 THEN 1 END) > 0 AND COUNT(CASE WHEN ret < 0 THEN 1 END) > 0 
            THEN
              CASE 
                WHEN AVG(CASE WHEN ret > 0 THEN ABS(ret) END) >= AVG(CASE WHEN ret < 0 THEN ABS(ret) END) THEN 85
                WHEN AVG(CASE WHEN ret > 0 THEN ABS(ret) END) >= AVG(CASE WHEN ret < 0 THEN ABS(ret) END) * 0.8 THEN 75
                WHEN AVG(CASE WHEN ret > 0 THEN ABS(ret) END) >= AVG(CASE WHEN ret < 0 THEN ABS(ret) END) * 0.6 THEN 65
                ELSE 55
              END
            ELSE NULL
          END as capture_score
        FROM eligible_funds ef
        CROSS JOIN LATERAL (
          SELECT (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
                 LAG(nav_value) OVER (ORDER BY nav_date) as ret
          FROM nav_data 
          WHERE fund_id = ef.fund_id 
          AND nav_date >= CURRENT_DATE - INTERVAL '12 months'
          AND nav_value > 0
        ) returns
        WHERE ABS(ret) < 0.08 AND ret IS NOT NULL
        GROUP BY ef.fund_id
        HAVING COUNT(*) >= 80
      )
      INSERT INTO fund_scores (fund_id, score_date, updown_capture_1y_score)
      SELECT fund_id, CURRENT_DATE, capture_score
      FROM risk_calculations
      WHERE capture_score IS NOT NULL
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET updown_capture_1y_score = EXCLUDED.updown_capture_1y_score
      RETURNING fund_id
    `);
    
    console.log(`  Risk Metrics: ${batchResults.rowCount} funds processed`);
    return { processed: batchResults.rowCount };
    
  } catch (error) {
    console.error('Error in massive risk processing:', error);
    return { processed: 0 };
  }
}

async function processMassiveAdvancedRatios() {
  try {
    console.log('Processing massive advanced ratios batch...');
    
    const batchResults = await pool.query(`
      WITH eligible_funds AS (
        SELECT f.id as fund_id
        FROM funds f
        WHERE EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '18 months'
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 200
        )
        AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.sharpe_ratio_score IS NOT NULL
        )
        LIMIT 400
      ),
      ratio_calculations AS (
        SELECT 
          ef.fund_id,
          CASE 
            WHEN STDDEV(ret) > 0 THEN
              CASE 
                WHEN (AVG(ret) * 252 - 0.06) / (STDDEV(ret) * SQRT(252)) >= 2.0 THEN 95
                WHEN (AVG(ret) * 252 - 0.06) / (STDDEV(ret) * SQRT(252)) >= 1.5 THEN 88
                WHEN (AVG(ret) * 252 - 0.06) / (STDDEV(ret) * SQRT(252)) >= 1.0 THEN 80
                WHEN (AVG(ret) * 252 - 0.06) / (STDDEV(ret) * SQRT(252)) >= 0.5 THEN 70
                WHEN (AVG(ret) * 252 - 0.06) / (STDDEV(ret) * SQRT(252)) >= 0.0 THEN 55
                ELSE 35
              END
            ELSE NULL
          END as sharpe_score,
          CASE 
            WHEN STDDEV(ret) > 0 THEN
              CASE 
                WHEN (STDDEV(ret) * SQRT(252) / 0.18) BETWEEN 0.8 AND 1.2 THEN 95
                WHEN (STDDEV(ret) * SQRT(252) / 0.18) BETWEEN 0.6 AND 1.5 THEN 85
                WHEN (STDDEV(ret) * SQRT(252) / 0.18) BETWEEN 0.4 AND 1.8 THEN 75
                ELSE 65
              END
            ELSE NULL
          END as beta_score,
          ROUND((AVG(ret) * 252 - 0.06) / (STDDEV(ret) * SQRT(252)), 3) as sharpe_ratio,
          ROUND(LEAST(2.5, GREATEST(0.3, STDDEV(ret) * SQRT(252) / 0.18)), 3) as beta_ratio
        FROM eligible_funds ef
        CROSS JOIN LATERAL (
          SELECT (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
                 LAG(nav_value) OVER (ORDER BY nav_date) as ret
          FROM nav_data 
          WHERE fund_id = ef.fund_id 
          AND nav_date >= CURRENT_DATE - INTERVAL '18 months'
          AND nav_value > 0
        ) returns
        WHERE ABS(ret) < 0.12 AND ret IS NOT NULL
        GROUP BY ef.fund_id
        HAVING COUNT(*) >= 150 AND STDDEV(ret) > 0
      )
      INSERT INTO fund_scores (fund_id, score_date, sharpe_ratio_score, beta_score, sharpe_ratio_1y, beta_1y)
      SELECT fund_id, CURRENT_DATE, sharpe_score, beta_score, sharpe_ratio, beta_ratio
      FROM ratio_calculations
      WHERE sharpe_score IS NOT NULL AND beta_score IS NOT NULL
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET 
        sharpe_ratio_score = EXCLUDED.sharpe_ratio_score,
        beta_score = EXCLUDED.beta_score,
        sharpe_ratio_1y = EXCLUDED.sharpe_ratio_1y,
        beta_1y = EXCLUDED.beta_1y
      RETURNING fund_id
    `);
    
    console.log(`  Advanced Ratios: ${batchResults.rowCount} funds processed`);
    return { processed: batchResults.rowCount };
    
  } catch (error) {
    console.error('Error in massive ratio processing:', error);
    return { processed: 0 };
  }
}

async function processMassiveQualityMetrics() {
  try {
    console.log('Processing massive quality metrics batch...');
    
    const batchResults = await pool.query(`
      WITH eligible_funds AS (
        SELECT f.id as fund_id
        FROM funds f
        WHERE EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '24 months'
          AND nd.aum_cr IS NOT NULL
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 100
        )
        AND NOT EXISTS (
          SELECT 1 FROM fund_scores fs 
          WHERE fs.fund_id = f.id AND fs.aum_size_score IS NOT NULL
        )
        LIMIT 300
      ),
      quality_calculations AS (
        SELECT 
          ef.fund_id,
          MAX(nd.aum_cr) as latest_aum,
          CASE 
            WHEN MAX(nd.aum_cr) >= 10000 THEN 95
            WHEN MAX(nd.aum_cr) >= 5000 THEN 85
            WHEN MAX(nd.aum_cr) >= 1000 THEN 75
            WHEN MAX(nd.aum_cr) >= 500 THEN 65
            WHEN MAX(nd.aum_cr) >= 100 THEN 55
            ELSE 45
          END as aum_score
        FROM eligible_funds ef
        JOIN nav_data nd ON nd.fund_id = ef.fund_id
        WHERE nd.nav_date >= CURRENT_DATE - INTERVAL '6 months'
        AND nd.aum_cr IS NOT NULL
        GROUP BY ef.fund_id
      )
      INSERT INTO fund_scores (fund_id, score_date, aum_size_score, fund_aum)
      SELECT fund_id, CURRENT_DATE, aum_score, latest_aum
      FROM quality_calculations
      WHERE aum_score IS NOT NULL
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET 
        aum_size_score = EXCLUDED.aum_size_score,
        fund_aum = EXCLUDED.fund_aum
      RETURNING fund_id
    `);
    
    console.log(`  Quality Metrics: ${batchResults.rowCount} funds processed`);
    return { processed: batchResults.rowCount };
    
  } catch (error) {
    console.error('Error in massive quality processing:', error);
    return { processed: 0 };
  }
}

optimizedMassProcessingEngine();