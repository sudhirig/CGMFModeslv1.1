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

async function fixQuartileSystem() {
  try {
    console.log('=== Fixing Quartile System with Complete Category Structure ===');

    // Clear existing quartile rankings
    await executeRawQuery('DELETE FROM quartile_rankings');

    // Get latest calculation date
    const latestDateResult = await executeRawQuery(`
      SELECT MAX(calculation_date) as latest_date 
      FROM fund_performance_metrics
    `);
    
    const calculationDate = latestDateResult.rows[0].latest_date;
    console.log(`Using calculation date: ${calculationDate}`);

    // Process existing categories (Equity and Debt)
    await processExistingCategories(calculationDate);
    
    // Expand category structure for "Other" funds
    await expandCategoryStructure();

    console.log('\n=== Quartile System Fixed and Expanded ===');

  } catch (error) {
    console.error('Error fixing quartile system:', error.message);
  }
}

async function processExistingCategories(calculationDate) {
  const categoriesResult = await executeRawQuery(`
    SELECT DISTINCT f.category, COUNT(*) as fund_count
    FROM funds f
    JOIN fund_performance_metrics pm ON f.id = pm.fund_id
    WHERE pm.calculation_date = $1
    GROUP BY f.category
    ORDER BY COUNT(*) DESC
  `, [calculationDate]);

  for (const categoryRow of categoriesResult.rows) {
    const category = categoryRow.category;
    console.log(`\n=== ${category} Category Quartile Rankings ===`);

    // Get unique funds in this category
    const categoryFundsResult = await executeRawQuery(`
      SELECT DISTINCT ON (f.id) 
        f.id, f.fund_name, pm.composite_score
      FROM funds f
      JOIN fund_performance_metrics pm ON f.id = pm.fund_id
      WHERE f.category = $1 AND pm.calculation_date = $2
      ORDER BY f.id, pm.composite_score DESC
    `, [category, calculationDate]);

    // Sort by performance
    const categoryFunds = categoryFundsResult.rows.sort((a, b) => 
      parseFloat(b.composite_score) - parseFloat(a.composite_score)
    );

    const totalFunds = categoryFunds.length;
    console.log(`Processing ${totalFunds} funds...`);

    if (totalFunds === 0) continue;

    // Calculate quartile thresholds
    const q1Threshold = Math.max(1, Math.ceil(totalFunds * 0.25));
    const q2Threshold = Math.max(1, Math.ceil(totalFunds * 0.50));
    const q3Threshold = Math.max(1, Math.ceil(totalFunds * 0.75));

    let quartileDistribution = { BUY: 0, HOLD: 0, REVIEW: 0, SELL: 0 };

    // Assign quartiles with safe numeric values
    for (let i = 0; i < categoryFunds.length; i++) {
      const fund = categoryFunds[i];
      const rank = i + 1;
      const percentile = parseFloat((rank / totalFunds * 100).toFixed(2));

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

      // Store with safe numeric precision
      const safeCompositeScore = Math.min(9999.99, parseFloat(parseFloat(fund.composite_score).toFixed(2)));
      
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
        safeCompositeScore
      ]);

      console.log(`${quartileLabel}: ${fund.fund_name} (${fund.composite_score}% return, Rank: ${rank})`);
    }

    // Log quartile distribution
    console.log(`\n${category} quartile distribution:`);
    Object.entries(quartileDistribution).forEach(([label, count]) => {
      console.log(`  ${label}: ${count} funds`);
    });
  }
}

async function expandCategoryStructure() {
  console.log('\n=== Expanding Category Structure for All Fund Types ===');

  // Analyze and categorize "Other" funds by fund type
  const otherFundsResult = await executeRawQuery(`
    SELECT id, fund_name, category, subcategory
    FROM funds 
    WHERE category = 'Other'
    ORDER BY fund_name
    LIMIT 100
  `);

  const fundTypeCategories = {
    'Gold ETF': [],
    'Silver ETF': [],
    'Index Fund': [],
    'Fund of Funds': [],
    'Sectoral Fund': [],
    'Target Maturity Fund': [],
    'ELSS Fund': [],
    'Liquid Fund': [],
    'International Fund': [],
    'Infrastructure Fund': []
  };

  // Categorize based on fund names
  otherFundsResult.rows.forEach(fund => {
    const fundName = fund.fund_name.toLowerCase();
    
    if (fundName.includes('gold etf') || fundName.includes('gold exchange')) {
      fundTypeCategories['Gold ETF'].push(fund);
    } else if (fundName.includes('silver etf') || fundName.includes('silver exchange')) {
      fundTypeCategories['Silver ETF'].push(fund);
    } else if (fundName.includes('index fund') || fundName.includes('nifty') || fundName.includes('sensex') || fundName.includes('bse')) {
      fundTypeCategories['Index Fund'].push(fund);
    } else if (fundName.includes('fof') || fundName.includes('fund of funds') || fundName.includes('allocator')) {
      fundTypeCategories['Fund of Funds'].push(fund);
    } else if (fundName.includes('infrastructure') || fundName.includes('banking') || fundName.includes('pharma') || fundName.includes('technology') || fundName.includes('financial services')) {
      fundTypeCategories['Sectoral Fund'].push(fund);
    } else if (fundName.includes('2025') || fundName.includes('2026') || fundName.includes('2027') || fundName.includes('2032') || fundName.includes('maturity')) {
      fundTypeCategories['Target Maturity Fund'].push(fund);
    } else if (fundName.includes('elss') || fundName.includes('tax saver')) {
      fundTypeCategories['ELSS Fund'].push(fund);
    } else if (fundName.includes('liquid') || fundName.includes('overnight')) {
      fundTypeCategories['Liquid Fund'].push(fund);
    } else if (fundName.includes('international') || fundName.includes('global') || fundName.includes('emerging')) {
      fundTypeCategories['International Fund'].push(fund);
    } else if (fundName.includes('infrastructure')) {
      fundTypeCategories['Infrastructure Fund'].push(fund);
    }
  });

  // Update fund categories in database
  console.log('\nUpdating fund categories:');
  for (const [newCategory, funds] of Object.entries(fundTypeCategories)) {
    if (funds.length > 0) {
      console.log(`${newCategory}: ${funds.length} funds identified`);
      
      // Update category for these funds
      const fundIds = funds.map(f => f.id);
      if (fundIds.length > 0) {
        await executeRawQuery(`
          UPDATE funds 
          SET category = $1 
          WHERE id = ANY($2)
        `, [newCategory, fundIds]);
        
        console.log(`  ✓ Updated ${fundIds.length} funds to ${newCategory} category`);
      }
    }
  }

  // Show updated category distribution
  const updatedCategoriesResult = await executeRawQuery(`
    SELECT category, COUNT(*) as fund_count
    FROM funds 
    GROUP BY category
    ORDER BY fund_count DESC
  `);

  console.log('\nUpdated fund category distribution:');
  updatedCategoriesResult.rows.forEach(row => {
    console.log(`  ${row.category}: ${row.fund_count} funds`);
  });

  console.log('\nRecommendation: Import historical NAV data for these new categories to enable quartile analysis');
}

// Execute the fix
fixQuartileSystem().then(() => {
  console.log('Quartile system fix completed');
  process.exit(0);
}).catch((error) => {
  console.error('Script failed:', error);
  process.exit(1);
});