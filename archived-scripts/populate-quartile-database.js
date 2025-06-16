const { executeRawQuery } = require('./server/db');

/**
 * Phase 1: Populate Performance Metrics Database with Authentic Data
 * This script calculates and stores performance metrics for all funds with sufficient historical NAV data
 */
async function populateQuartileDatabase() {
  try {
    console.log('=== Phase 1: Populating Quartile Database with Authentic Data ===');

    // Step 1: Get all funds with sufficient historical NAV data (1+ year) across ALL categories
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.fund_name, f.category, f.subcategory, COUNT(n.nav_date) as nav_count,
             MIN(n.nav_date) as start_date, MAX(n.nav_date) as end_date
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id
      WHERE n.created_at > '2025-05-30 06:45:00'  -- Only authentic imported data
      GROUP BY f.id, f.fund_name, f.category, f.subcategory
      HAVING COUNT(n.nav_date) >= 252  -- At least 1 year of data
      ORDER BY f.category, COUNT(n.nav_date) DESC
    `);

    const eligibleFunds = fundsResult.rows;
    console.log(`Found ${eligibleFunds.length} funds with sufficient authentic historical data`);
    console.log(`Categories found: ${[...new Set(eligibleFunds.map(f => f.category))].join(', ')}`);

    if (eligibleFunds.length === 0) {
      console.log('No funds with sufficient data found. Historical import may still be in progress.');
      return;
    }

    // Step 2: Calculate simple performance metrics and store in database
    let processedCount = 0;
    let successCount = 0;
    const calculationDate = new Date();

    for (const fund of eligibleFunds) {
      try {
        console.log(`Processing ${fund.fund_name} (${fund.category})...`);

        // Calculate basic performance metrics using authentic NAV data
        const metricsResult = await executeRawQuery(`
          WITH nav_analysis AS (
            SELECT 
              (SELECT nav_value FROM nav_data WHERE fund_id = $1 ORDER BY nav_date ASC LIMIT 1) as first_nav,
              (SELECT nav_value FROM nav_data WHERE fund_id = $1 ORDER BY nav_date DESC LIMIT 1) as latest_nav,
              (SELECT nav_date FROM nav_data WHERE fund_id = $1 ORDER BY nav_date ASC LIMIT 1) as start_date,
              (SELECT nav_date FROM nav_data WHERE fund_id = $1 ORDER BY nav_date DESC LIMIT 1) as end_date,
              COUNT(*) as total_records
            FROM nav_data WHERE fund_id = $1
          )
          SELECT 
            first_nav,
            latest_nav,
            start_date,
            end_date,
            total_records,
            ROUND(((latest_nav - first_nav) / first_nav * 100)::numeric, 4) as total_return_pct,
            ROUND(((end_date - start_date) / 365.0)::numeric, 2) as years_of_data,
            ROUND((POWER((latest_nav / first_nav), 365.0 / (end_date - start_date)) - 1) * 100, 4) as annualized_return_pct
          FROM nav_analysis
        `, [fund.id]);

        if (metricsResult.rows.length > 0) {
          const metrics = metricsResult.rows[0];
          
          // Calculate data quality score
          const dataQuality = Math.min(1.0, parseFloat(metrics.total_records) / (5 * 252)); // 5 years = 1.0
          const compositeScore = parseFloat(metrics.annualized_return_pct) || 0;

          // Store performance metrics in database
          await executeRawQuery(`
            INSERT INTO fund_performance_metrics (
              fund_id, calculation_date, returns_1y, returns_3y, returns_5y,
              volatility, sharpe_ratio, max_drawdown, consistency_score,
              alpha, beta, information_ratio, total_nav_records,
              data_quality_score, composite_score
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
              returns_1y = EXCLUDED.returns_1y,
              composite_score = EXCLUDED.composite_score,
              total_nav_records = EXCLUDED.total_nav_records,
              data_quality_score = EXCLUDED.data_quality_score
          `, [
            fund.id, calculationDate,
            metrics.annualized_return_pct, // returns_1y (simplified)
            null, null, // returns_3y, returns_5y (to be calculated later)
            null, null, null, null, // volatility, sharpe_ratio, max_drawdown, consistency_score
            null, null, null, // alpha, beta, information_ratio
            metrics.total_records,
            dataQuality,
            compositeScore
          ]);

          successCount++;
          console.log(`✓ Stored performance metrics for ${fund.fund_name} (${metrics.annualized_return_pct}% annual return)`);
        }

        processedCount++;
      } catch (error) {
        console.error(`Error processing fund ${fund.id}:`, error.message);
        processedCount++;
      }
    }

    console.log(`\n=== Performance Metrics Storage Complete ===`);
    console.log(`Processed: ${processedCount} funds`);
    console.log(`Successfully stored: ${successCount} fund metrics`);

    // Step 3: Calculate and store quartile rankings by category (including ALL categories)
    await calculateAndStoreQuartileRankings(calculationDate);

    console.log('\n=== Phase 1 Complete: Quartile Database Populated with Authentic Data ===');

  } catch (error) {
    console.error('Error populating quartile database:', error.message);
  }
}

/**
 * Calculate quartile rankings for ALL categories and store in database
 */
async function calculateAndStoreQuartileRankings(calculationDate) {
  console.log('\n=== Calculating Quartile Rankings by Category (Including All Categories) ===');

  // Get ALL categories that have performance metrics
  const categoriesResult = await executeRawQuery(`
    SELECT DISTINCT f.category, COUNT(*) as fund_count
    FROM funds f
    JOIN fund_performance_metrics pm ON f.id = pm.fund_id
    WHERE pm.calculation_date = $1
    GROUP BY f.category
    HAVING COUNT(*) >= 1  -- Include all categories, even with just 1 fund
    ORDER BY COUNT(*) DESC
  `, [calculationDate]);

  console.log(`Found ${categoriesResult.rows.length} categories with funds for quartile analysis:`);
  categoriesResult.rows.forEach(row => {
    console.log(`  ${row.category}: ${row.fund_count} funds`);
  });

  for (const categoryRow of categoriesResult.rows) {
    const category = categoryRow.category;
    const fundCount = categoryRow.fund_count;

    console.log(`\nCalculating quartiles for ${category} category (${fundCount} funds)...`);

    // Get funds in this category sorted by composite score (annualized return)
    const categoryFundsResult = await executeRawQuery(`
      SELECT f.id, f.fund_name, pm.composite_score
      FROM funds f
      JOIN fund_performance_metrics pm ON f.id = pm.fund_id
      WHERE f.category = $1 AND pm.calculation_date = $2
      ORDER BY pm.composite_score DESC
    `, [category, calculationDate]);

    const categoryFunds = categoryFundsResult.rows;
    const totalFunds = categoryFunds.length;

    // Calculate quartile thresholds (adapted for small samples)
    const q1Threshold = Math.max(1, Math.ceil(totalFunds * 0.25));
    const q2Threshold = Math.max(1, Math.ceil(totalFunds * 0.50));
    const q3Threshold = Math.max(1, Math.ceil(totalFunds * 0.75));

    // Assign quartiles and store rankings
    for (let i = 0; i < categoryFunds.length; i++) {
      const fund = categoryFunds[i];
      const rank = i + 1;
      const percentile = (rank / totalFunds) * 100;

      let quartile;
      let quartileLabel;

      if (rank <= q1Threshold) {
        quartile = 1;
        quartileLabel = 'BUY';
      } else if (rank <= q2Threshold) {
        quartile = 2;
        quartileLabel = 'HOLD';
      } else if (rank <= q3Threshold) {
        quartile = 3;
        quartileLabel = 'REVIEW';
      } else {
        quartile = 4;
        quartileLabel = 'SELL';
      }

      // Store quartile ranking
      await executeRawQuery(`
        INSERT INTO quartile_rankings (
          fund_id, category, calculation_date, quartile, quartile_label,
          rank, total_funds, percentile, composite_score
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (fund_id, category, calculation_date) DO UPDATE SET
          quartile = EXCLUDED.quartile,
          quartile_label = EXCLUDED.quartile_label,
          rank = EXCLUDED.rank,
          total_funds = EXCLUDED.total_funds,
          percentile = EXCLUDED.percentile,
          composite_score = EXCLUDED.composite_score
      `, [
        fund.id, category, calculationDate, quartile, quartileLabel,
        rank, totalFunds, percentile, parseFloat(fund.composite_score)
      ]);
    }

    // Log quartile distribution
    const distributionResult = await executeRawQuery(`
      SELECT quartile_label, COUNT(*) as count
      FROM quartile_rankings
      WHERE category = $1 AND calculation_date = $2
      GROUP BY quartile_label
      ORDER BY 
        CASE quartile_label 
          WHEN 'BUY' THEN 1 
          WHEN 'HOLD' THEN 2 
          WHEN 'REVIEW' THEN 3 
          WHEN 'SELL' THEN 4 
        END
    `, [category, calculationDate]);

    console.log(`${category} quartile distribution:`);
    for (const dist of distributionResult.rows) {
      console.log(`  ${dist.quartile_label}: ${dist.count} funds`);
    }
  }
}

// Execute the population script
populateQuartileDatabase().then(() => {
  console.log('Script completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script failed:', error);
  process.exit(1);
});