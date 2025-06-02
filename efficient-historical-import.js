/**
 * Efficient historical NAV data import for recently added funds
 * Processes funds in smaller batches to ensure successful completion
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function efficientHistoricalImport() {
  try {
    console.log('Starting efficient historical NAV data import...');
    
    // Get first 50 funds that need historical data
    const fundsNeedingData = await pool.query(`
      SELECT 
        f.id,
        f.scheme_code,
        f.fund_name,
        f.amc_name
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE nd.fund_id IS NULL
      AND f.scheme_code IS NOT NULL
      ORDER BY f.id
      LIMIT 50
    `);
    
    console.log(`Processing ${fundsNeedingData.rows.length} funds for historical data import`);
    
    let successCount = 0;
    let failedCount = 0;
    
    for (const fund of fundsNeedingData.rows) {
      try {
        console.log(`\nImporting data for: ${fund.fund_name}`);
        console.log(`Scheme Code: ${fund.scheme_code}`);
        
        const imported = await importRecentHistoricalData(fund);
        
        if (imported > 0) {
          successCount++;
          console.log(`✓ Imported ${imported} records for fund ${fund.id}`);
        } else {
          failedCount++;
          console.log(`⚠ No recent data available for fund ${fund.id}`);
        }
        
        // Rate limiting delay
        await new Promise(resolve => setTimeout(resolve, 1000));
        
      } catch (error) {
        failedCount++;
        console.error(`✗ Error importing fund ${fund.id}: ${error.message}`);
      }
    }
    
    console.log(`\n=== Import Summary ===`);
    console.log(`Successfully imported: ${successCount} funds`);
    console.log(`Failed imports: ${failedCount} funds`);
    console.log(`Remaining funds needing data: ${2453 - fundsNeedingData.rows.length}`);
    
  } catch (error) {
    console.error('Error in efficient historical import:', error);
  } finally {
    await pool.end();
  }
}

async function importRecentHistoricalData(fund) {
  try {
    // Get last 6 months of data
    const url = `https://api.mfapi.in/mf/${fund.scheme_code}`;
    console.log(`Fetching from: ${url}`);
    
    const response = await axios.get(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Fund-Analysis-Platform/1.0'
      }
    });
    
    if (!response.data || !response.data.data) {
      throw new Error('Invalid API response structure');
    }
    
    const navData = response.data.data;
    console.log(`API returned ${navData.length} total records`);
    
    if (navData.length === 0) {
      return 0;
    }
    
    // Process recent data (last 6 months)
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(sixMonthsAgo.getMonth() - 6);
    
    const recentData = navData.filter(record => {
      if (!record.date || !record.nav) return false;
      
      // Parse date (format: DD-MM-YYYY)
      const dateParts = record.date.split('-');
      if (dateParts.length !== 3) return false;
      
      const recordDate = new Date(
        parseInt(dateParts[2]), // year
        parseInt(dateParts[1]) - 1, // month (0-indexed)
        parseInt(dateParts[0]) // day
      );
      
      return recordDate >= sixMonthsAgo && !isNaN(parseFloat(record.nav));
    });
    
    console.log(`Filtered to ${recentData.length} recent records`);
    
    if (recentData.length === 0) {
      return 0;
    }
    
    // Insert records in batches
    let insertedCount = 0;
    const batchSize = 50;
    
    for (let i = 0; i < recentData.length; i += batchSize) {
      const batch = recentData.slice(i, i + batchSize);
      
      for (const record of batch) {
        try {
          // Convert date format from DD-MM-YYYY to YYYY-MM-DD
          const dateParts = record.date.split('-');
          const formattedDate = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
          
          await pool.query(`
            INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (fund_id, nav_date) DO NOTHING
          `, [
            fund.id,
            formattedDate,
            parseFloat(record.nav),
            new Date()
          ]);
          
          insertedCount++;
        } catch (insertError) {
          // Skip invalid records
          continue;
        }
      }
    }
    
    return insertedCount;
    
  } catch (error) {
    if (error.code === 'ENOTFOUND' || error.response?.status === 404) {
      throw new Error(`Fund ${fund.scheme_code} not found in MFAPI`);
    }
    throw error;
  }
}

efficientHistoricalImport();