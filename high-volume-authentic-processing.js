/**
 * High-Volume Authentic Processing System
 * Optimized for processing thousands of eligible funds efficiently
 * Maintains strict data integrity while achieving scale
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  max: 10,
  connectionTimeoutMillis: 5000,
  idleTimeoutMillis: 10000
});

async function highVolumeAuthenticProcessing() {
  try {
    console.log('=== HIGH-VOLUME AUTHENTIC PROCESSING ===\n');
    
    // Phase 1: Bulk 5Y Processing
    console.log('Processing 5Y returns in high-volume batches...');
    const batch5Y = await processBulk5YReturns();
    
    // Phase 1: Bulk YTD Processing  
    console.log('Processing YTD returns in high-volume batches...');
    const batchYTD = await processBulkYTDReturns();
    
    // Phase 2: Bulk Risk Processing
    console.log('Processing risk metrics in high-volume batches...');
    const batchRisk = await processBulkRiskMetrics();
    
    // Verification and reporting
    const finalVerification = await pool.query(`
      SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as final_5y_coverage,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as final_ytd_coverage,
        COUNT(CASE WHEN updown_capture_1y_score IS NOT NULL THEN 1 END) as final_capture_coverage,
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as final_sharpe_coverage,
        COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as final_complete_scores,
        COUNT(CASE WHEN return_5y_score = 50 OR return_ytd_score = 50 OR 
               return_6m_score = 50 OR return_3y_score = 50 OR 
               return_1y_score = 50 OR return_3m_score = 50 THEN 1 END) as synthetic_verification
      FROM fund_scores
    `);
    
    const final = finalVerification.rows[0];
    
    console.log('\n=== HIGH-VOLUME PROCESSING RESULTS ===');
    console.log(`5Y Returns: ${batch5Y.processed} processed → Total: ${final.final_5y_coverage}`);
    console.log(`YTD Returns: ${batchYTD.processed} processed → Total: ${final.final_ytd_coverage}`);
    console.log(`Risk Metrics: ${batchRisk.processed} processed → Total: ${final.final_capture_coverage}`);
    console.log(`Complete Scores: ${final.final_complete_scores}`);
    console.log(`Data Integrity: ${final.synthetic_verification === '0' ? 'CLEAN' : 'CONTAMINATED'}`);
    
    return {
      success: true,
      total5Y: batch5Y.processed,
      totalYTD: batchYTD.processed,
      totalRisk: batchRisk.processed,
      finalCoverage: final,
      dataIntegrityMaintained: final.synthetic_verification === '0'
    };
    
  } catch (error) {
    console.error('Error in high-volume processing:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function processBulk5YReturns() {
  try {
    const eligibleFunds = await pool.query(`
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
      ORDER BY f.id
      LIMIT 500
    `);
    
    console.log(`Found ${eligibleFunds.rows.length} funds eligible for 5Y processing`);
    
    let processed = 0;
    const batchSize = 25;
    
    for (let i = 0; i < eligibleFunds.rows.length; i += batchSize) {
      const batch = eligibleFunds.rows.slice(i, i + batchSize);
      
      const batchPromises = batch.map(fund => processIndividual5Y(fund));
      const results = await Promise.allSettled(batchPromises);
      
      const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      processed += successful;
      
      if (i % 100 === 0) {
        console.log(`  5Y Progress: ${processed} completed, ${i + batchSize} processed`);
      }
    }
    
    return { processed, eligible: eligibleFunds.rows.length };
    
  } catch (error) {
    console.error('Error in bulk 5Y processing:', error);
    return { processed: 0, eligible: 0 };
  }
}

async function processBulkYTDReturns() {
  try {
    const eligibleFunds = await pool.query(`
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
      ORDER BY f.id
      LIMIT 400
    `);
    
    console.log(`Found ${eligibleFunds.rows.length} funds eligible for YTD processing`);
    
    let processed = 0;
    const batchSize = 30;
    
    for (let i = 0; i < eligibleFunds.rows.length; i += batchSize) {
      const batch = eligibleFunds.rows.slice(i, i + batchSize);
      
      const batchPromises = batch.map(fund => processIndividualYTD(fund));
      const results = await Promise.allSettled(batchPromises);
      
      const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      processed += successful;
      
      if (i % 90 === 0) {
        console.log(`  YTD Progress: ${processed} completed, ${i + batchSize} processed`);
      }
    }
    
    return { processed, eligible: eligibleFunds.rows.length };
    
  } catch (error) {
    console.error('Error in bulk YTD processing:', error);
    return { processed: 0, eligible: 0 };
  }
}

async function processBulkRiskMetrics() {
  try {
    const eligibleFunds = await pool.query(`
      SELECT f.id as fund_id, f.fund_name
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
      ORDER BY f.id
      LIMIT 300
    `);
    
    console.log(`Found ${eligibleFunds.rows.length} funds eligible for risk processing`);
    
    let processed = 0;
    const batchSize = 20;
    
    for (let i = 0; i < eligibleFunds.rows.length; i += batchSize) {
      const batch = eligibleFunds.rows.slice(i, i + batchSize);
      
      const batchPromises = batch.map(fund => processIndividualRisk(fund));
      const results = await Promise.allSettled(batchPromises);
      
      const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      processed += successful;
      
      if (i % 60 === 0) {
        console.log(`  Risk Progress: ${processed} completed, ${i + batchSize} processed`);
      }
    }
    
    return { processed, eligible: eligibleFunds.rows.length };
    
  } catch (error) {
    console.error('Error in bulk risk processing:', error);
    return { processed: 0, eligible: 0 };
  }
}

async function processIndividual5Y(fund) {
  try {
    const navData = await pool.query(`
      WITH nav_5y AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= CURRENT_DATE - INTERVAL '5 years 3 months'
        AND nav_value > 0
        ORDER BY nav_date
      )
      SELECT 
        FIRST_VALUE(nav_value) OVER (ORDER BY nav_date) as start_nav,
        LAST_VALUE(nav_value) OVER (ORDER BY nav_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_nav,
        COUNT(*) OVER () as record_count
      FROM nav_5y
      WHERE nav_date <= CURRENT_DATE - INTERVAL '5 years' OR nav_date >= CURRENT_DATE - INTERVAL '1 week'
      LIMIT 1
    `, [fund.fund_id]);
    
    if (navData.rows.length === 0 || navData.rows[0].record_count < 200) return false;
    
    const startNav = parseFloat(navData.rows[0].start_nav);
    const endNav = parseFloat(navData.rows[0].end_nav);
    
    if (!startNav || !endNav || startNav <= 0 || endNav <= 0) return false;
    
    const return5Y = ((endNav - startNav) / startNav) * 100;
    
    if (!isFinite(return5Y)) return false;
    
    const score = return5Y >= 25 ? 100 : return5Y >= 20 ? 95 : return5Y >= 15 ? 85 : 
                 return5Y >= 10 ? 75 : return5Y >= 5 ? 65 : return5Y >= 0 ? 55 : 
                 return5Y >= -5 ? 45 : return5Y >= -10 ? 35 : 25;
    
    await pool.query(`
      INSERT INTO fund_scores (fund_id, score_date, return_5y_score)
      VALUES ($1, CURRENT_DATE, $2)
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET return_5y_score = $2
    `, [fund.fund_id, score]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

async function processIndividualYTD(fund) {
  try {
    const navData = await pool.query(`
      WITH nav_ytd AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '1 week'
        AND nav_value > 0
        ORDER BY nav_date
      )
      SELECT 
        FIRST_VALUE(nav_value) OVER (ORDER BY nav_date) as start_nav,
        LAST_VALUE(nav_value) OVER (ORDER BY nav_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as end_nav,
        COUNT(*) OVER () as record_count
      FROM nav_ytd
      LIMIT 1
    `, [fund.fund_id]);
    
    if (navData.rows.length === 0 || navData.rows[0].record_count < 15) return false;
    
    const startNav = parseFloat(navData.rows[0].start_nav);
    const endNav = parseFloat(navData.rows[0].end_nav);
    
    if (!startNav || !endNav || startNav <= 0 || endNav <= 0) return false;
    
    const returnYTD = ((endNav - startNav) / startNav) * 100;
    
    if (!isFinite(returnYTD)) return false;
    
    const score = returnYTD >= 20 ? 100 : returnYTD >= 15 ? 90 : returnYTD >= 10 ? 80 : 
                 returnYTD >= 5 ? 70 : returnYTD >= 0 ? 60 : returnYTD >= -5 ? 50 : 
                 returnYTD >= -10 ? 40 : 30;
    
    await pool.query(`
      INSERT INTO fund_scores (fund_id, score_date, return_ytd_score)
      VALUES ($1, CURRENT_DATE, $2)
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET return_ytd_score = $2
    `, [fund.fund_id, score]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

async function processIndividualRisk(fund) {
  try {
    const riskData = await pool.query(`
      WITH daily_returns AS (
        SELECT (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
               LAG(nav_value) OVER (ORDER BY nav_date) as ret
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= CURRENT_DATE - INTERVAL '12 months'
        AND nav_value > 0
        ORDER BY nav_date
      )
      SELECT 
        AVG(CASE WHEN ret > 0 THEN ret END) as avg_up,
        AVG(CASE WHEN ret < 0 THEN ret END) as avg_down,
        COUNT(*) as total_returns
      FROM daily_returns 
      WHERE ret IS NOT NULL AND ABS(ret) < 0.08
    `, [fund.fund_id]);
    
    if (riskData.rows.length === 0 || riskData.rows[0].total_returns < 80) return false;
    
    const avgUp = parseFloat(riskData.rows[0].avg_up) || 0;
    const avgDown = parseFloat(riskData.rows[0].avg_down) || 0;
    
    if (avgUp <= 0 || avgDown >= 0) return false;
    
    const upCapture = Math.abs(avgUp) * 252 * 100;
    const downCapture = Math.abs(avgDown) * 252 * 100;
    
    const captureScore = upCapture >= downCapture ? 85 : upCapture >= downCapture * 0.8 ? 75 : 
                        upCapture >= downCapture * 0.6 ? 65 : 55;
    
    await pool.query(`
      INSERT INTO fund_scores (fund_id, score_date, updown_capture_1y_score)
      VALUES ($1, CURRENT_DATE, $2)
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET updown_capture_1y_score = $2
    `, [fund.fund_id, captureScore]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

highVolumeAuthenticProcessing();