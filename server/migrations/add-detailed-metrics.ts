import { pool } from '../db';

/**
 * Migration to add detailed fund metrics to the fund_scores table
 * This enhances transparency by storing both raw metric values and derived scores
 */
async function runMigration() {
  console.log('Starting migration: Adding detailed metrics to fund_scores table');
  
  try {
    // Begin transaction
    await pool.query('BEGIN');
    
    // Add raw return fields
    await pool.query(`
      ALTER TABLE fund_scores
      ADD COLUMN IF NOT EXISTS return_1m DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS return_3m DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS return_6m DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS return_1y DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS return_3y DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS return_5y DECIMAL(6,2)
    `);
    console.log('Added raw return fields');
    
    // Add risk metric fields
    await pool.query(`
      ALTER TABLE fund_scores
      ADD COLUMN IF NOT EXISTS volatility_1y DECIMAL(6,4),
      ADD COLUMN IF NOT EXISTS volatility_3y DECIMAL(6,4),
      ADD COLUMN IF NOT EXISTS sharpe_ratio_1y DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS sharpe_ratio_3y DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS sortino_ratio_1y DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS sortino_ratio_3y DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS max_drawdown DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS up_capture_ratio DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS down_capture_ratio DECIMAL(5,2)
    `);
    console.log('Added risk metric fields');
    
    // Add quality metric fields
    await pool.query(`
      ALTER TABLE fund_scores
      ADD COLUMN IF NOT EXISTS consistency_score DECIMAL(4,2),
      ADD COLUMN IF NOT EXISTS category_median_expense_ratio DECIMAL(4,2),
      ADD COLUMN IF NOT EXISTS category_std_dev_expense_ratio DECIMAL(4,2),
      ADD COLUMN IF NOT EXISTS expense_ratio_rank DECIMAL(5,2),
      ADD COLUMN IF NOT EXISTS fund_aum DECIMAL(12,2),
      ADD COLUMN IF NOT EXISTS category_median_aum DECIMAL(12,2),
      ADD COLUMN IF NOT EXISTS fund_size_factor DECIMAL(4,2)
    `);
    console.log('Added quality metric fields');
    
    // Add context fields
    await pool.query(`
      ALTER TABLE fund_scores
      ADD COLUMN IF NOT EXISTS risk_free_rate DECIMAL(4,2),
      ADD COLUMN IF NOT EXISTS category_benchmark_return_1y DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS category_benchmark_return_3y DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS median_returns_1y DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS median_returns_3y DECIMAL(6,2),
      ADD COLUMN IF NOT EXISTS above_median_months_count INTEGER,
      ADD COLUMN IF NOT EXISTS total_months_evaluated INTEGER
    `);
    console.log('Added context fields');
    
    // Commit transaction
    await pool.query('COMMIT');
    console.log('Migration completed successfully');
    
  } catch (error) {
    await pool.query('ROLLBACK');
    console.error('Migration failed:', error);
    throw error;
  }
}

// Export the migration function
export default runMigration;