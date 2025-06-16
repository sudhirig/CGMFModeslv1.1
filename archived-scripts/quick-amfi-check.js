/**
 * Quick AMFI Coverage Check
 */

import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function quickAMFICheck() {
  try {
    // Get AMFI scheme codes
    const response = await fetch('https://www.amfiindia.com/spages/NAVAll.txt');
    const text = await response.text();
    
    const amfiCodes = [];
    const lines = text.split('\n');
    
    for (const line of lines) {
      if (line.trim() && !line.startsWith('Scheme Code') && line.includes(';')) {
        const parts = line.split(';');
        if (parts[0]?.trim() && !isNaN(parts[0].trim())) {
          amfiCodes.push(parts[0].trim());
        }
      }
    }
    
    console.log(`AMFI Authoritative List: ${amfiCodes.length} funds`);
    
    // Check coverage in database
    const dbResult = await pool.query(`
      SELECT 
        COUNT(DISTINCT f.id) as total_funds_in_db,
        COUNT(DISTINCT CASE WHEN f.scheme_code = ANY($1) THEN f.id END) as amfi_funds_found,
        COUNT(DISTINCT CASE WHEN f.scheme_code = ANY($1) AND nd.fund_id IS NOT NULL THEN f.id END) as amfi_funds_with_nav,
        COUNT(DISTINCT CASE WHEN f.scheme_code = ANY($1) AND nd.nav_count >= 252 THEN f.id END) as amfi_analysis_ready
      FROM funds f
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as nav_count
        FROM nav_data 
        GROUP BY fund_id
      ) nd ON f.id = nd.fund_id
    `, [amfiCodes]);
    
    const stats = dbResult.rows[0];
    
    console.log('\nYour AMFI Coverage:');
    console.log(`Total in Database: ${parseInt(stats.total_funds_in_db).toLocaleString()}`);
    console.log(`AMFI Funds Found: ${parseInt(stats.amfi_funds_found).toLocaleString()}`);
    console.log(`With NAV Data: ${parseInt(stats.amfi_funds_with_nav).toLocaleString()}`);
    console.log(`Analysis Ready: ${parseInt(stats.amfi_analysis_ready).toLocaleString()}`);
    
    const coverage = (parseInt(stats.amfi_funds_found) / amfiCodes.length * 100).toFixed(1);
    const navCoverage = (parseInt(stats.amfi_funds_with_nav) / amfiCodes.length * 100).toFixed(1);
    
    console.log(`\nCoverage: ${coverage}% of AMFI funds`);
    console.log(`NAV Coverage: ${navCoverage}% with historical data`);
    console.log(`Missing: ${amfiCodes.length - parseInt(stats.amfi_funds_found)} AMFI funds`);
    
  } catch (error) {
    console.error('Check failed:', error.message);
  } finally {
    await pool.end();
  }
}

quickAMFICheck();