// Script to verify the quality of NAV data, ensuring proper variation across dates
import pg from 'pg';

const { Pool } = pg;

async function verifyNavDataQuality() {
  try {
    console.log('Starting NAV data quality verification...');
    
    // Connect to database
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    
    // Check import progress
    const countResult = await pool.query('SELECT COUNT(*) as total_records FROM nav_data');
    const totalRecords = parseInt(countResult.rows[0].total_records);
    console.log(`Total NAV records imported so far: ${totalRecords}`);
    
    // Check for funds with multiple NAV entries
    const multipleEntriesResult = await pool.query(`
      SELECT fund_id, COUNT(*) as record_count
      FROM nav_data
      GROUP BY fund_id
      HAVING COUNT(*) > 1
      ORDER BY COUNT(*) DESC
      LIMIT 10
    `);
    
    console.log(`\nFunds with multiple NAV entries: ${multipleEntriesResult.rowCount}`);
    if (multipleEntriesResult.rowCount > 0) {
      console.table(multipleEntriesResult.rows);
      
      // For the first fund with multiple entries, check for variation
      const fundId = multipleEntriesResult.rows[0].fund_id;
      const variationResult = await pool.query(`
        SELECT fund_id, 
               COUNT(*) as record_count, 
               COUNT(DISTINCT nav_value) as distinct_values,
               MIN(nav_value) as min_value,
               MAX(nav_value) as max_value,
               (MAX(nav_value) - MIN(nav_value)) / MIN(nav_value) * 100 as percent_variation
        FROM nav_data
        WHERE fund_id = $1
        GROUP BY fund_id
      `, [fundId]);
      
      console.log(`\nNAV value variation for fund ID ${fundId}:`);
      console.table(variationResult.rows);
      
      // Get the actual fund details
      const fundResult = await pool.query(`
        SELECT id, fund_name, amc_name, category
        FROM funds
        WHERE id = $1
      `, [fundId]);
      
      console.log(`\nFund details for ID ${fundId}:`);
      console.table(fundResult.rows);
      
      // Get all NAV records for this fund to see the actual values
      const navResult = await pool.query(`
        SELECT nav_date, nav_value
        FROM nav_data
        WHERE fund_id = $1
        ORDER BY nav_date DESC
      `, [fundId]);
      
      console.log(`\nAll NAV records for fund ID ${fundId}:`);
      console.table(navResult.rows);
    } else {
      console.log('No funds with multiple NAV entries found yet. The import is still in progress.');
      console.log('Check back later when more data has been imported.');
    }
    
    // Check for funds with varying NAV values
    const varyingValuesResult = await pool.query(`
      SELECT fund_id, 
             COUNT(*) as record_count, 
             COUNT(DISTINCT nav_value) as distinct_values,
             MIN(nav_value) as min_value,
             MAX(nav_value) as max_value,
             (MAX(nav_value) - MIN(nav_value)) / MIN(nav_value) * 100 as percent_variation
      FROM nav_data
      GROUP BY fund_id
      HAVING COUNT(DISTINCT nav_value) > 1
      ORDER BY COUNT(DISTINCT nav_value) DESC
      LIMIT 5
    `);
    
    console.log(`\nFunds with varying NAV values: ${varyingValuesResult.rowCount}`);
    if (varyingValuesResult.rowCount > 0) {
      console.table(varyingValuesResult.rows);
    } else {
      console.log('No funds with varying NAV values found yet. The import is still in progress.');
    }
    
    // Close the pool
    await pool.end();
    
    console.log('\nNAV data quality verification complete.');
    console.log('Summary:');
    console.log(`- Total NAV records: ${totalRecords}`);
    console.log(`- Funds with multiple entries: ${multipleEntriesResult.rowCount}`);
    console.log(`- Funds with varying values: ${varyingValuesResult.rowCount}`);
    
    if (multipleEntriesResult.rowCount === 0) {
      console.log('\nRecommendation: Run this script again later when more data has been imported.');
    }
  } catch (error) {
    console.error('Error during NAV data quality verification:', error);
  }
}

// Run the verification
verifyNavDataQuality();