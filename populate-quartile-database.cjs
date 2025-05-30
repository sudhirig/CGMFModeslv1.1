const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function executeRawQuery(sql, params = []) {
  const client = await pool.connect();
  try {
    const result = await client.query(sql, params);
    return result;
  } finally {
    client.release();
  }
}

/**
 * Phase 1: Populate Performance Metrics Database with Authentic Data
 */
async function populateQuartileDatabase() {
  try {
    console.log('=== Populating Quartile Database with Authentic Historical NAV Data ===');

    // Get all funds with sufficient historical NAV data across ALL categories
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.fund_name, f.category, f.subcategory, COUNT(n.nav_date) as nav_count
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id
      WHERE n.created_at > '2025-05-30 06:45:00'
      GROUP BY f.id, f.fund_name, f.category, f.subcategory
      HAVING COUNT(n.nav_date) >= 252
      ORDER BY f.category, COUNT(n.nav_date) DESC
    `);

    const eligibleFunds = fundsResult.rows;
    console.log(`Found ${eligibleFunds.length} funds with sufficient authentic data`);
    console.log(`Categories: ${[...new Set(eligibleFunds.map(f => f.category))].join(', ')}`);

    if (eligibleFunds.length === 0) {
      console.log('No funds with sufficient data found');
      return;
    }

    // Calculate performance metrics and store in database
    let successCount = 0;
    const calculationDate = new Date();

    for (const fund of eligibleFunds) {
      try {
        console.log(`Processing ${fund.fund_name} (${fund.category})`);

        // Calculate basic performance metrics
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
            total_records,
            ROUND((POWER((latest_nav / first_nav), 365.0 / (end_date - start_date)) - 1) * 100, 4) as annualized_return_pct
          FROM nav_analysis
        `, [fund.id]);

        if (metricsResult.rows.length > 0) {
          const metrics = metricsResult.rows[0];
          const dataQuality = Math.min(1.0, parseFloat(metrics.total_records) / (5 * 252));
          const compositeScore = parseFloat(metrics.annualized_return_pct) || 0;

          // Store performance metrics
          await executeRawQuery(`
            INSERT INTO fund_performance_metrics (
              fund_id, calculation_date, returns_1y, total_nav_records,
              data_quality_score, composite_score
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
              returns_1y = EXCLUDED.returns_1y,
              composite_score = EXCLUDED.composite_score,
              total_nav_records = EXCLUDED.total_nav_records,
              data_quality_score = EXCLUDED.data_quality_score
          `, [
            fund.id, calculationDate, metrics.annualized_return_pct,
            metrics.total_records, dataQuality, compositeScore
          ]);

          successCount++;
          console.log(`✓ ${fund.fund_name}: ${metrics.annualized_return_pct}% annual return`);
        }
      } catch (error) {
        console.error(`Error processing fund ${fund.id}:`, error.message);
      }
    }

    console.log(`\n=== Stored performance metrics for ${successCount} funds ===`);

    // Calculate quartile rankings for ALL categories
    await calculateQuartileRankings(calculationDate);

    console.log('\n=== Quartile Database Population Complete ===');

  } catch (error) {
    console.error('Error populating quartile database:', error.message);
  }
}

async function calculateQuartileRankings(calculationDate) {
  console.log('\n=== Calculating Quartile Rankings for ALL Categories ===');

  // Get ALL categories with performance metrics
  const categoriesResult = await executeRawQuery(`
    SELECT DISTINCT f.category, COUNT(*) as fund_count
    FROM funds f
    JOIN fund_performance_metrics pm ON f.id = pm.fund_id
    WHERE pm.calculation_date = $1
    GROUP BY f.category
    ORDER BY COUNT(*) DESC
  `, [calculationDate]);

  console.log(`Categories with quartile rankings:`);
  categoriesResult.rows.forEach(row => {
    console.log(`  ${row.category}: ${row.fund_count} funds`);
  });

  for (const categoryRow of categoriesResult.rows) {
    const category = categoryRow.category;
    console.log(`\nCalculating quartiles for ${category}...`);

    // Get funds sorted by performance
    const categoryFundsResult = await executeRawQuery(`
      SELECT f.id, f.fund_name, pm.composite_score
      FROM funds f
      JOIN fund_performance_metrics pm ON f.id = pm.fund_id
      WHERE f.category = $1 AND pm.calculation_date = $2
      ORDER BY pm.composite_score DESC
    `, [category, calculationDate]);

    const categoryFunds = categoryFundsResult.rows;
    const totalFunds = categoryFunds.length;

    // Calculate quartile thresholds
    const q1Threshold = Math.max(1, Math.ceil(totalFunds * 0.25));
    const q2Threshold = Math.max(1, Math.ceil(totalFunds * 0.50));
    const q3Threshold = Math.max(1, Math.ceil(totalFunds * 0.75));

    let quartileDistribution = { BUY: 0, HOLD: 0, REVIEW: 0, SELL: 0 };

    // Assign quartiles
    for (let i = 0; i < categoryFunds.length; i++) {
      const fund = categoryFunds[i];
      const rank = i + 1;
      const percentile = (rank / totalFunds) * 100;

      let quartile, quartileLabel;
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

      quartileDistribution[quartileLabel]++;

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

    // Log distribution
    console.log(`${category} quartile distribution:`);
    Object.entries(quartileDistribution).forEach(([label, count]) => {
      console.log(`  ${label}: ${count} funds`);
    });
  }
}

// Execute the script
populateQuartileDatabase().then(() => {
  console.log('Quartile database population completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script failed:', error);
  process.exit(1);
});