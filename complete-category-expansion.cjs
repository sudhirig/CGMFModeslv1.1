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

async function completeCategoryExpansion() {
  try {
    console.log('=== Completing Fund Category Expansion ===');

    // First, let's finish the current quartile rankings without the overflow
    await completeCurrentQuartiles();

    // Then expand the category structure
    await expandAllCategories();

    console.log('\n=== Category Expansion Complete ===');

  } catch (error) {
    console.error('Error in category expansion:', error.message);
  }
}

async function completeCurrentQuartiles() {
  console.log('Completing quartile rankings for current authentic data...');

  // Get latest calculation date
  const latestDateResult = await executeRawQuery(`
    SELECT MAX(calculation_date) as latest_date 
    FROM fund_performance_metrics
  `);
  
  const calculationDate = latestDateResult.rows[0].latest_date;

  // Process Debt category (was skipped due to overflow)
  const debtFundsResult = await executeRawQuery(`
    SELECT DISTINCT ON (f.id) 
      f.id, f.fund_name, pm.composite_score
    FROM funds f
    JOIN fund_performance_metrics pm ON f.id = pm.fund_id
    WHERE f.category = 'Debt' AND pm.calculation_date = $1
    ORDER BY f.id, pm.composite_score DESC
  `, [calculationDate]);

  if (debtFundsResult.rows.length > 0) {
    console.log(`\n=== Debt Category Quartile Rankings ===`);
    
    const debtFunds = debtFundsResult.rows.sort((a, b) => 
      parseFloat(b.composite_score) - parseFloat(a.composite_score)
    );

    console.log(`Processing ${debtFunds.length} debt funds...`);

    for (let i = 0; i < debtFunds.length; i++) {
      const fund = debtFunds[i];
      const rank = i + 1;
      const totalFunds = debtFunds.length;
      
      // Simple quartile assignment for small sample
      let quartile, quartileLabel;
      if (rank === 1) {
        quartile = 1;
        quartileLabel = 'BUY';
      } else if (rank <= Math.ceil(totalFunds * 0.5)) {
        quartile = 2;
        quartileLabel = 'HOLD';
      } else if (rank <= Math.ceil(totalFunds * 0.75)) {
        quartile = 3;
        quartileLabel = 'REVIEW';
      } else {
        quartile = 4;
        quartileLabel = 'SELL';
      }

      await executeRawQuery(`
        INSERT INTO quartile_rankings (
          fund_id, category, calculation_date, quartile, quartile_label,
          rank, total_funds, percentile, composite_score
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (fund_id, category, calculation_date) DO NOTHING
      `, [
        fund.id, 
        'Debt', 
        calculationDate, 
        quartile, 
        quartileLabel,
        rank, 
        totalFunds, 
        parseFloat((rank / totalFunds * 100).toFixed(1)),
        parseFloat(parseFloat(fund.composite_score).toFixed(2))
      ]);

      console.log(`${quartileLabel}: ${fund.fund_name} (${fund.composite_score}% return, Rank: ${rank})`);
    }
  }

  // Show current quartile summary
  const quartileStatsResult = await executeRawQuery(`
    SELECT category, quartile_label, COUNT(*) as count
    FROM quartile_rankings
    WHERE calculation_date = $1
    GROUP BY category, quartile_label
    ORDER BY category, 
      CASE quartile_label 
        WHEN 'BUY' THEN 1 
        WHEN 'HOLD' THEN 2 
        WHEN 'REVIEW' THEN 3 
        WHEN 'SELL' THEN 4 
      END
  `, [calculationDate]);

  console.log('\nCurrent quartile distribution:');
  let currentCategory = '';
  quartileStatsResult.rows.forEach(row => {
    if (row.category !== currentCategory) {
      console.log(`\n${row.category}:`);
      currentCategory = row.category;
    }
    console.log(`  ${row.quartile_label}: ${row.count} funds`);
  });
}

async function expandAllCategories() {
  console.log('\n=== Expanding to Include All Fund Types ===');

  // Analyze the "Other" category funds by name patterns
  const otherFundsResult = await executeRawQuery(`
    SELECT id, fund_name, category, subcategory
    FROM funds 
    WHERE category = 'Other'
    ORDER BY fund_name
  `);

  console.log(`Found ${otherFundsResult.rows.length} funds in "Other" category to categorize`);

  const categoryMappings = [];

  // Categorize funds based on name patterns
  otherFundsResult.rows.forEach(fund => {
    const name = fund.fund_name.toLowerCase();
    let newCategory = 'Other'; // default

    if (name.includes('gold etf') || name.includes('gold exchange traded')) {
      newCategory = 'Gold ETF';
    } else if (name.includes('silver etf') || name.includes('silver exchange traded')) {
      newCategory = 'Silver ETF';
    } else if (name.includes('index fund') || name.includes('nifty') || name.includes('sensex') || name.includes('bse') || name.includes('crisil ibx')) {
      newCategory = 'Index Fund';
    } else if (name.includes('fof') || name.includes('fund of funds') || name.includes('allocator')) {
      newCategory = 'Fund of Funds';
    } else if (name.includes('elss') || name.includes('tax saver') || name.includes('equity linked saving')) {
      newCategory = 'ELSS Fund';
    } else if (name.includes('liquid') || name.includes('overnight') || name.includes('money market')) {
      newCategory = 'Liquid Fund';
    } else if (name.includes('international') || name.includes('global') || name.includes('emerging markets') || name.includes('overseas')) {
      newCategory = 'International Fund';
    } else if (name.includes('2025') || name.includes('2026') || name.includes('2027') || name.includes('2028') || name.includes('2029') || name.includes('2030') || name.includes('2032')) {
      newCategory = 'Target Maturity Fund';
    } else if (name.includes('infrastructure') || name.includes('banking') || name.includes('pharma') || name.includes('technology') || name.includes('financial services') || name.includes('healthcare') || name.includes('energy')) {
      newCategory = 'Sectoral Fund';
    } else if (name.includes('ultra short') || name.includes('short term') || name.includes('medium term') || name.includes('long term') || name.includes('duration')) {
      newCategory = 'Duration Fund';
    } else if (name.includes('arbitrage') || name.includes('hedge')) {
      newCategory = 'Arbitrage Fund';
    }

    if (newCategory !== 'Other') {
      categoryMappings.push({ id: fund.id, name: fund.fund_name, newCategory });
    }
  });

  // Group by new categories
  const categoryGroups = {};
  categoryMappings.forEach(mapping => {
    if (!categoryGroups[mapping.newCategory]) {
      categoryGroups[mapping.newCategory] = [];
    }
    categoryGroups[mapping.newCategory].push(mapping);
  });

  // Update database categories
  console.log('\nUpdating fund categories:');
  for (const [newCategory, mappings] of Object.entries(categoryGroups)) {
    if (mappings.length > 0) {
      const fundIds = mappings.map(m => m.id);
      
      await executeRawQuery(`
        UPDATE funds 
        SET category = $1 
        WHERE id = ANY($2)
      `, [newCategory, fundIds]);
      
      console.log(`✓ ${newCategory}: ${mappings.length} funds updated`);
    }
  }

  // Show final category distribution
  const finalCategoriesResult = await executeRawQuery(`
    SELECT category, COUNT(*) as fund_count
    FROM funds 
    GROUP BY category
    ORDER BY fund_count DESC
  `);

  console.log('\nFinal fund category distribution:');
  finalCategoriesResult.rows.forEach(row => {
    console.log(`  ${row.category}: ${row.fund_count} funds`);
  });

  console.log('\nNext steps for complete quartile system:');
  console.log('1. Import historical NAV data for new categories');
  console.log('2. Calculate performance metrics for each category');
  console.log('3. Generate category-specific quartile rankings');
  console.log('4. Enable portfolio recommendations across all fund types');
}

// Execute the expansion
completeCategoryExpansion().then(() => {
  console.log('Category expansion completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script failed:', error);
  process.exit(1);
});