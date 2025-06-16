/**
 * Expand Fund Category Analysis
 * Updates the quartile system to analyze all major fund categories
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function expandFundCategories() {
  try {
    console.log('=== Expanding Fund Category Analysis ===');
    
    // Get current category distribution
    const categoryStats = await pool.query(`
      SELECT 
        f.category,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN nav_count.nav_records >= 252 THEN 1 END) as eligible_funds,
        AVG(nav_count.nav_records) as avg_nav_records
      FROM funds f
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as nav_records
        FROM nav_data 
        WHERE created_at > '2025-05-30 06:45:00'
        GROUP BY fund_id
      ) nav_count ON f.id = nav_count.fund_id
      GROUP BY f.category
      ORDER BY COUNT(*) DESC
    `);
    
    console.log('\nCurrent Category Distribution:');
    console.log('Category'.padEnd(25) + 'Total'.padEnd(10) + 'Eligible'.padEnd(10) + 'Avg NAV');
    console.log('-'.repeat(55));
    
    for (const row of categoryStats.rows) {
      const category = row.category || 'Unknown';
      const total = parseInt(row.total_funds);
      const eligible = parseInt(row.eligible_funds) || 0;
      const avgNav = parseFloat(row.avg_nav_records) || 0;
      
      console.log(
        category.padEnd(25) + 
        total.toString().padEnd(10) + 
        eligible.toString().padEnd(10) + 
        Math.round(avgNav)
      );
    }
    
    // Identify categories with sufficient funds for quartile analysis
    const eligibleCategories = categoryStats.rows.filter(row => 
      parseInt(row.eligible_funds) >= 4 // Minimum 4 funds for quartiles
    );
    
    console.log(`\n✓ Found ${eligibleCategories.length} categories eligible for quartile analysis`);
    
    // Update the quartile system configuration
    const expandedCategories = eligibleCategories.map(row => row.category);
    
    console.log('\nExpanded Categories for Quartile Analysis:');
    expandedCategories.forEach((cat, index) => {
      console.log(`${index + 1}. ${cat}`);
    });
    
    // Test scoring for each expanded category
    console.log('\n=== Testing Category-Specific Scoring ===');
    
    for (const category of expandedCategories) {
      const categoryFunds = await pool.query(`
        SELECT f.id, f.fund_name
        FROM funds f
        JOIN (
          SELECT fund_id, COUNT(*) as nav_count
          FROM nav_data 
          WHERE created_at > '2025-05-30 06:45:00'
          GROUP BY fund_id
          HAVING COUNT(*) >= 252
        ) nav_count ON f.id = nav_count.fund_id
        WHERE f.category = $1
        ORDER BY nav_count.nav_count DESC
        LIMIT 5
      `, [category]);
      
      if (categoryFunds.rows.length > 0) {
        console.log(`\n${category}: ${categoryFunds.rows.length} eligible funds`);
        categoryFunds.rows.forEach(fund => {
          console.log(`  - ${fund.fund_name}`);
        });
      }
    }
    
    // Create category-specific performance benchmarks
    console.log('\n=== Creating Category Benchmarks ===');
    
    const benchmarkResults = await pool.query(`
      INSERT INTO fund_category_benchmarks (category, min_funds_required, created_at)
      SELECT DISTINCT category, 4, NOW()
      FROM funds f
      WHERE category IN (${expandedCategories.map((_, i) => `$${i + 1}`).join(',')})
      ON CONFLICT (category) DO UPDATE SET
        min_funds_required = EXCLUDED.min_funds_required,
        updated_at = NOW()
      RETURNING category
    `, expandedCategories);
    
    console.log(`✓ Created benchmarks for ${benchmarkResults.rows.length} categories`);
    
    // Update automated scheduler for expanded categories
    console.log('\n=== Updating Automation Configuration ===');
    
    const automationUpdate = await pool.query(`
      UPDATE automation_config 
      SET 
        enabled_categories = $1::text[],
        updated_at = NOW()
      WHERE config_name = 'quartile_analysis'
      RETURNING id
    `, [expandedCategories]);
    
    if (automationUpdate.rows.length === 0) {
      await pool.query(`
        INSERT INTO automation_config (config_name, enabled_categories, created_at)
        VALUES ('quartile_analysis', $1::text[], NOW())
      `, [expandedCategories]);
    }
    
    console.log('✓ Updated automation configuration');
    
    // Summary
    console.log('\n=== Category Expansion Summary ===');
    console.log(`Original categories: 3 (Equity, Debt, Hybrid)`);
    console.log(`Expanded categories: ${expandedCategories.length}`);
    console.log(`New categories added: ${expandedCategories.length - 3}`);
    console.log(`Total eligible funds: ${eligibleCategories.reduce((sum, cat) => sum + parseInt(cat.eligible_funds), 0)}`);
    
    console.log('\n✓ Fund category expansion completed successfully!');
    
    return {
      success: true,
      expandedCategories,
      totalEligibleFunds: eligibleCategories.reduce((sum, cat) => sum + parseInt(cat.eligible_funds), 0)
    };
    
  } catch (error) {
    console.error('Error expanding fund categories:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

// Add the missing table if it doesn't exist
async function createSupportingTables() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS fund_category_benchmarks (
        id SERIAL PRIMARY KEY,
        category VARCHAR(100) UNIQUE NOT NULL,
        min_funds_required INTEGER DEFAULT 4,
        benchmark_index VARCHAR(100),
        risk_free_rate DECIMAL(5,2) DEFAULT 6.5,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    
    await pool.query(`
      CREATE TABLE IF NOT EXISTS automation_config (
        id SERIAL PRIMARY KEY,
        config_name VARCHAR(100) UNIQUE NOT NULL,
        enabled_categories TEXT[],
        config_data JSONB,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    
    console.log('✓ Supporting tables created');
  } catch (error) {
    console.error('Error creating supporting tables:', error);
  }
}

if (require.main === module) {
  createSupportingTables()
    .then(() => expandFundCategories())
    .then(result => {
      console.log('\nExpansion completed:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('Expansion failed:', error);
      process.exit(1);
    });
}

module.exports = { expandFundCategories };