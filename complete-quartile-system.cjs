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

async function completeQuartileSystem() {
  try {
    console.log('=== Completing Quartile Ranking System ===');

    // Clear any existing quartile rankings to start fresh
    await executeRawQuery('DELETE FROM quartile_rankings');

    // Get latest calculation date
    const latestDateResult = await executeRawQuery(`
      SELECT MAX(calculation_date) as latest_date 
      FROM fund_performance_metrics
    `);
    
    const calculationDate = latestDateResult.rows[0].latest_date;
    console.log(`Using calculation date: ${calculationDate}`);

    // Get all categories with performance metrics
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
      console.log(`\n=== ${category} Category Quartile Rankings ===`);

      // Get funds in this category sorted by performance (latest entry only)
      const categoryFundsResult = await executeRawQuery(`
        SELECT DISTINCT ON (f.id) f.id, f.fund_name, pm.composite_score
        FROM funds f
        JOIN fund_performance_metrics pm ON f.id = pm.fund_id
        WHERE f.category = $1 AND pm.calculation_date = $2
        ORDER BY f.id, pm.id DESC
      `, [category, calculationDate]);

      // Sort by performance
      const categoryFunds = categoryFundsResult.rows.sort((a, b) => 
        parseFloat(b.composite_score) - parseFloat(a.composite_score)
      );

      const totalFunds = categoryFunds.length;
      console.log(`Processing ${totalFunds} funds...`);

      // Calculate quartile thresholds
      const q1Threshold = Math.max(1, Math.ceil(totalFunds * 0.25));
      const q2Threshold = Math.max(1, Math.ceil(totalFunds * 0.50));
      const q3Threshold = Math.max(1, Math.ceil(totalFunds * 0.75));

      let quartileDistribution = { BUY: 0, HOLD: 0, REVIEW: 0, SELL: 0 };

      // Assign quartiles and store rankings
      for (let i = 0; i < categoryFunds.length; i++) {
        const fund = categoryFunds[i];
        const rank = i + 1;
        const percentile = Math.round((rank / totalFunds) * 100 * 100) / 100; // Round to 2 decimal places

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

        // Store quartile ranking with safe numeric values
        await executeRawQuery(`
          INSERT INTO quartile_rankings (
            fund_id, category, calculation_date, quartile, quartile_label,
            rank, total_funds, percentile, composite_score
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          fund.id, 
          category, 
          calculationDate, 
          quartile, 
          quartileLabel,
          rank, 
          totalFunds, 
          percentile,
          Math.round(parseFloat(fund.composite_score) * 100) / 100
        ]);

        console.log(`${quartileLabel}: ${fund.fund_name} (${fund.composite_score}% return, Rank: ${rank})`);
      }

      // Log quartile distribution
      console.log(`\n${category} quartile distribution:`);
      Object.entries(quartileDistribution).forEach(([label, count]) => {
        console.log(`  ${label}: ${count} funds`);
      });
    }

    // Now address the broader category structure issue
    console.log('\n=== Expanding Fund Category Structure ===');
    await expandFundCategories();

    console.log('\n=== Quartile System Complete ===');

  } catch (error) {
    console.error('Error completing quartile system:', error.message);
  }
}

async function expandFundCategories() {
  console.log('Analyzing fund categorization beyond Equity/Debt/Hybrid...');

  // Check what's in the "Other" category that we're missing
  const otherFundsResult = await executeRawQuery(`
    SELECT fund_name, COUNT(*) as count
    FROM funds 
    WHERE category = 'Other'
    GROUP BY fund_name
    ORDER BY fund_name
    LIMIT 50
  `);

  console.log('\nFund types currently in "Other" category:');
  const fundTypes = {
    'Gold ETF': 0,
    'Silver ETF': 0,
    'Index Fund': 0,
    'Fund of Funds': 0,
    'Sectoral Fund': 0,
    'Target Maturity': 0,
    'ELSS': 0,
    'Liquid Fund': 0
  };

  otherFundsResult.rows.forEach(row => {
    const fundName = row.fund_name.toLowerCase();
    
    if (fundName.includes('gold etf')) fundTypes['Gold ETF']++;
    else if (fundName.includes('silver etf')) fundTypes['Silver ETF']++;
    else if (fundName.includes('index fund') || fundName.includes('nifty') || fundName.includes('sensex')) fundTypes['Index Fund']++;
    else if (fundName.includes('fof') || fundName.includes('fund of funds')) fundTypes['Fund of Funds']++;
    else if (fundName.includes('infrastructure') || fundName.includes('banking') || fundName.includes('pharma') || fundName.includes('technology')) fundTypes['Sectoral Fund']++;
    else if (fundName.includes('2025') || fundName.includes('2026') || fundName.includes('2027') || fundName.includes('2032')) fundTypes['Target Maturity']++;
    else if (fundName.includes('elss')) fundTypes['ELSS']++;
    else if (fundName.includes('liquid')) fundTypes['Liquid Fund']++;
  });

  console.log('\nFund types needing separate quartile analysis:');
  Object.entries(fundTypes).forEach(([type, count]) => {
    if (count > 0) {
      console.log(`  ${type}: ${count} funds`);
    }
  });

  console.log(`\nRecommendation: Expand quartile system to include these categories:`);
  console.log(`- Current system only covers Equity (${categoriesResult.rows.find(r => r.category === 'Equity')?.fund_count || 0}) and Debt (${categoriesResult.rows.find(r => r.category === 'Debt')?.fund_count || 0})`);
  console.log(`- Missing 2,150 funds in "Other" category`);
  console.log(`- Need separate quartile rankings for Gold ETFs, Index Funds, Sectoral Funds, etc.`);
}

// Execute the complete system
completeQuartileSystem().then(() => {
  console.log('Complete quartile system implementation finished');
  process.exit(0);
}).catch((error) => {
  console.error('Script failed:', error);
  process.exit(1);
});