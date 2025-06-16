/**
 * Import historical NAV data for recently added funds
 * Process the 2,453 funds that need historical data collection
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function importHistoricalNavData() {
  try {
    console.log('Starting historical NAV data import for recently added funds...');
    
    // Get funds that need historical NAV data
    const fundsNeedingData = await pool.query(`
      SELECT 
        f.id,
        f.scheme_code,
        f.fund_name,
        f.amc_name,
        f.created_at
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE nd.fund_id IS NULL
      AND f.scheme_code IS NOT NULL
      ORDER BY f.created_at DESC
      LIMIT 100
    `);
    
    console.log(`Found ${fundsNeedingData.rows.length} funds needing historical NAV data`);
    
    let importedCount = 0;
    let failedCount = 0;
    
    for (const fund of fundsNeedingData.rows) {
      try {
        console.log(`Importing NAV data for ${fund.fund_name} (${fund.scheme_code})`);
        
        // Import data for the last 12 months
        const imported = await importNavForFund(fund);
        
        if (imported > 0) {
          importedCount++;
          console.log(`✓ Imported ${imported} NAV records for fund ${fund.id}`);
        } else {
          failedCount++;
          console.log(`⚠ No data found for fund ${fund.id}`);
        }
        
        // Delay to respect API limits
        await new Promise(resolve => setTimeout(resolve, 500));
        
      } catch (error) {
        failedCount++;
        console.error(`✗ Failed to import data for fund ${fund.id}: ${error.message}`);
      }
    }
    
    console.log(`\nHistorical import complete!`);
    console.log(`Successfully imported data for: ${importedCount} funds`);
    console.log(`Failed imports: ${failedCount} funds`);
    
  } catch (error) {
    console.error('Error in historical NAV import:', error);
  } finally {
    await pool.end();
  }
}

async function importNavForFund(fund) {
  let totalImported = 0;
  const endDate = new Date();
  const startDate = new Date();
  startDate.setFullYear(endDate.getFullYear() - 1); // 1 year back
  
  // Generate monthly chunks for import
  const monthsToImport = [];
  const current = new Date(startDate);
  
  while (current <= endDate) {
    monthsToImport.push({
      year: current.getFullYear(),
      month: current.getMonth() + 1
    });
    current.setMonth(current.getMonth() + 1);
  }
  
  for (const period of monthsToImport) {
    try {
      const navData = await fetchNavDataFromMFAPI(fund.scheme_code, period.year, period.month);
      
      if (navData && navData.length > 0) {
        const inserted = await insertNavRecords(fund.id, navData);
        totalImported += inserted;
      }
      
      // Small delay between API calls
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`Error importing ${period.year}-${period.month} for fund ${fund.id}:`, error.message);
    }
  }
  
  return totalImported;
}

async function fetchNavDataFromMFAPI(schemeCode, year, month) {
  try {
    // Using MFAPI.in for historical NAV data
    const url = `https://api.mfapi.in/mf/${schemeCode}`;
    const response = await axios.get(url, {
      timeout: 10000,
      headers: {
        'User-Agent': 'Fund-Analysis-Platform/1.0'
      }
    });
    
    if (response.data && response.data.data) {
      // Filter data for the specific month/year
      const monthStr = month.toString().padStart(2, '0');
      const datePattern = `${year}-${monthStr}`;
      
      const filteredData = response.data.data.filter(record => 
        record.date && record.date.startsWith(datePattern)
      );
      
      return filteredData.map(record => ({
        date: record.date,
        nav: parseFloat(record.nav)
      })).filter(record => !isNaN(record.nav));
    }
    
    return [];
  } catch (error) {
    if (error.code === 'ENOTFOUND' || error.code === 'ECONNREFUSED') {
      throw new Error('Unable to connect to MFAPI service');
    }
    throw error;
  }
}

async function insertNavRecords(fundId, navData) {
  if (!navData || navData.length === 0) return 0;
  
  try {
    let insertedCount = 0;
    
    for (const record of navData) {
      try {
        await pool.query(`
          INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (fund_id, nav_date) DO NOTHING
        `, [
          fundId,
          record.date,
          record.nav,
          new Date()
        ]);
        insertedCount++;
      } catch (insertError) {
        // Skip duplicates or invalid records
        continue;
      }
    }
    
    return insertedCount;
  } catch (error) {
    console.error(`Error inserting NAV records for fund ${fundId}:`, error.message);
    return 0;
  }
}

importHistoricalNavData();