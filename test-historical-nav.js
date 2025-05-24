import { exec } from 'child_process';
import pg from 'pg';
const { Pool } = pg;

// Create a database connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Function to check if NAV values vary across dates
async function checkNavVariation() {
  try {
    console.log('Checking NAV data variation across dates...');
    
    // Query to check if we have NAV data with different values
    const result = await pool.query(`
      SELECT fund_id, COUNT(*) as record_count, 
             COUNT(DISTINCT nav_value) as distinct_values,
             MIN(nav_value) as min_value,
             MAX(nav_value) as max_value
      FROM nav_data
      GROUP BY fund_id
      ORDER BY distinct_values DESC
      LIMIT 10
    `);
    
    console.log('NAV data variation results:');
    console.table(result.rows);
    
    // Query to get some recent NAV values for a fund with variation
    if (result.rows.length > 0) {
      const fundId = result.rows[0].fund_id;
      
      console.log(`\nRecent NAV values for fund ID ${fundId}:`);
      const navValues = await pool.query(`
        SELECT nav_date, nav_value
        FROM nav_data
        WHERE fund_id = $1
        ORDER BY nav_date DESC
        LIMIT 10
      `, [fundId]);
      
      console.table(navValues.rows);
    }
    
    // Close the pool
    pool.end();
  } catch (error) {
    console.error('Error checking NAV variation:', error);
    pool.end();
  }
}

// Run the function
checkNavVariation();