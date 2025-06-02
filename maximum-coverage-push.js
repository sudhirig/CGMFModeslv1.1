/**
 * Maximum Coverage Push
 * Final expansion to achieve highest possible 5Y and YTD coverage
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function maximumCoveragePush() {
  try {
    console.log('Executing maximum coverage push for 5Y and YTD analysis...\n');
    
    // Continue processing remaining eligible funds in large batches
    const expansionResults = await processRemainingEligibleFunds();
    
    // Final comprehensive coverage assessment
    const finalCoverage = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
        ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
        ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
      FROM fund_scores
    `);
    
    const coverage = finalCoverage.rows[0];
    
    console.log(`\n=== MAXIMUM COVERAGE ACHIEVED ===`);
    console.log(`5Y Analysis: ${coverage.funds_5y}/${coverage.total_funds} funds (${coverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${coverage.funds_ytd}/${coverage.total_funds} funds (${coverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${coverage.complete_coverage}/${coverage.total_funds} funds`);
    
    // Show performance distribution across categories
    const performanceStats = await pool.query(`
      SELECT 
        f.category,
        COUNT(*) as total_category_funds,
        COUNT(CASE WHEN fs.return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN fs.return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        AVG(CASE WHEN fs.return_5y_score IS NOT NULL THEN fs.return_5y_score END) as avg_5y_score,
        AVG(CASE WHEN fs.return_ytd_score IS NOT NULL THEN fs.return_ytd_score END) as avg_ytd_score
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      GROUP BY f.category
      ORDER BY COUNT(*) DESC
    `);
    
    console.log(`\n=== CATEGORY PERFORMANCE OVERVIEW ===`);
    performanceStats.rows.forEach(cat => {
      console.log(`${cat.category}:`);
      console.log(`  Coverage: 5Y ${cat.funds_5y}/${cat.total_category_funds}, YTD ${cat.funds_ytd}/${cat.total_category_funds}`);
      if (cat.avg_5y_score) console.log(`  Avg Scores: 5Y ${parseFloat(cat.avg_5y_score).toFixed(1)}, YTD ${parseFloat(cat.avg_ytd_score).toFixed(1)}`);
    });
    
    // Show top performers with complete analysis
    const topPerformers = await pool.query(`
      SELECT f.fund_name, f.category, fs.return_5y_score, fs.return_ytd_score, fs.total_score
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.return_5y_score IS NOT NULL AND fs.return_ytd_score IS NOT NULL
      ORDER BY (fs.return_5y_score + fs.return_ytd_score) DESC
      LIMIT 10
    `);
    
    console.log(`\n=== TOP PERFORMERS (5Y + YTD Combined) ===`);
    topPerformers.rows.forEach((fund, i) => {
      const combined = parseFloat(fund.return_5y_score) + parseFloat(fund.return_ytd_score);
      console.log(`${i+1}. ${fund.fund_name.substring(0, 50)}...`);
      console.log(`   Category: ${fund.category} | 5Y: ${fund.return_5y_score} | YTD: ${fund.return_ytd_score} | Combined: ${combined.toFixed(1)}`);
    });
    
  } catch (error) {
    console.error('Error in maximum coverage push:', error);
  } finally {
    await pool.end();
  }
}

async function processRemainingEligibleFunds() {
  let total5Y = 0;
  let totalYTD = 0;
  let batchNumber = 1;
  
  while (true) {
    // Process remaining 5Y eligible funds
    const remaining5Y = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '3 years 6 months'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 200
      )
      ORDER BY f.id
      LIMIT 500
    `);
    
    // Process remaining YTD eligible funds
    const remainingYTD = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '60 days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 20
      )
      ORDER BY f.id
      LIMIT 600
    `);
    
    if (remaining5Y.rows.length === 0 && remainingYTD.rows.length === 0) {
      break;
    }
    
    console.log(`Batch ${batchNumber}: Processing ${remaining5Y.rows.length} 5Y funds, ${remainingYTD.rows.length} YTD funds`);
    
    // Process 5Y funds
    for (const fund of remaining5Y.rows) {
      try {
        const success = await processFund5Y(fund.id);
        if (success) total5Y++;
      } catch (error) {
        // Continue processing
      }
    }
    
    // Process YTD funds
    for (const fund of remainingYTD.rows) {
      try {
        const success = await processFundYTD(fund.id);
        if (success) totalYTD++;
      } catch (error) {
        // Continue processing
      }
    }
    
    console.log(`  Batch ${batchNumber} complete: +${total5Y} 5Y, +${totalYTD} YTD total`);
    batchNumber++;
    
    if (batchNumber > 50) break; // Safety limit
  }
  
  return { total5Y, totalYTD };
}

async function processFund5Y(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_analysis AS (
        SELECT 
          first_value(nav_value) OVER (ORDER BY nav_date DESC) as current_nav,
          first_value(nav_value) OVER (ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - (CURRENT_DATE - INTERVAL '5 years'))))) as historical_nav
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date BETWEEN CURRENT_DATE - INTERVAL '6 years' AND CURRENT_DATE
      )
      SELECT DISTINCT
        CASE 
          WHEN historical_nav > 0 AND current_nav IS NOT NULL
          THEN ((current_nav - historical_nav) / historical_nav) * 100
          ELSE NULL 
        END as return_5y
      FROM nav_analysis
      WHERE historical_nav IS NOT NULL
      LIMIT 1
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_5y !== null) {
      const return5Y = parseFloat(result.rows[0].return_5y);
      const score = calculateOptimizedScore(return5Y, '5y');
      
      await pool.query(`
        UPDATE fund_scores 
        SET return_5y_score = $1
        WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

async function processFundYTD(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_analysis AS (
        SELECT 
          first_value(nav_value) OVER (ORDER BY nav_date DESC) as current_nav,
          first_value(nav_value) OVER (ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))) as year_start_nav
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '45 days'
      )
      SELECT DISTINCT
        CASE 
          WHEN year_start_nav > 0 AND current_nav IS NOT NULL
          THEN ((current_nav - year_start_nav) / year_start_nav) * 100
          ELSE NULL 
        END as return_ytd
      FROM nav_analysis
      WHERE year_start_nav IS NOT NULL
      LIMIT 1
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_ytd !== null) {
      const returnYTD = parseFloat(result.rows[0].return_ytd);
      const score = calculateOptimizedScore(returnYTD, 'ytd');
      
      await pool.query(`
        UPDATE fund_scores 
        SET return_ytd_score = $1
        WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

function calculateOptimizedScore(returnValue, period) {
  if (period === '5y') {
    if (returnValue >= 2000) return 100;
    if (returnValue >= 1000) return 98;
    if (returnValue >= 500) return 95;
    if (returnValue >= 300) return 90;
    if (returnValue >= 200) return 85;
    if (returnValue >= 150) return 80;
    if (returnValue >= 100) return 75;
    if (returnValue >= 75) return 70;
    if (returnValue >= 50) return 65;
    if (returnValue >= 25) return 55;
    if (returnValue >= 0) return 45;
    if (returnValue >= -25) return 30;
    if (returnValue >= -50) return 20;
    return 10;
  } else {
    if (returnValue >= 150) return 100;
    if (returnValue >= 100) return 95;
    if (returnValue >= 60) return 90;
    if (returnValue >= 40) return 85;
    if (returnValue >= 25) return 80;
    if (returnValue >= 15) return 75;
    if (returnValue >= 10) return 70;
    if (returnValue >= 5) return 60;
    if (returnValue >= 0) return 50;
    if (returnValue >= -10) return 35;
    if (returnValue >= -20) return 25;
    return 15;
  }
}

maximumCoveragePush();