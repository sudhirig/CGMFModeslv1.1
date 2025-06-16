/**
 * Comprehensive Historical NAV Data Import
 * Imports authentic historical data for funds with insufficient records
 * Prioritizes older funds and those with high potential for analysis
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function startComprehensiveHistoricalImport() {
  console.log('🚀 Starting comprehensive historical NAV data import...');
  
  try {
    // Get funds that need historical data (less than 100 records)
    const fundsQuery = `
      SELECT 
        f.id,
        f.scheme_code,
        f.fund_name,
        f.inception_date,
        f.category,
        COALESCE(nav_count.record_count, 0) as current_records,
        CASE 
          WHEN f.inception_date IS NOT NULL 
          THEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, f.inception_date))
          ELSE NULL 
        END as fund_age_years
      FROM funds f
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as record_count 
        FROM nav_data 
        GROUP BY fund_id
      ) nav_count ON f.id = nav_count.fund_id
      WHERE COALESCE(nav_count.record_count, 0) < 100
        AND f.scheme_code IS NOT NULL
        AND f.status = 'ACTIVE'
      ORDER BY 
        CASE WHEN f.inception_date IS NOT NULL THEN f.inception_date ELSE '2025-01-01' END ASC,
        f.category ASC
      LIMIT 50
    `;
    
    const fundsResult = await pool.query(fundsQuery);
    const fundsToProcess = fundsResult.rows;
    
    console.log(`Found ${fundsToProcess.length} funds requiring historical data import`);
    
    let totalImported = 0;
    let successCount = 0;
    
    for (let i = 0; i < fundsToProcess.length; i++) {
      const fund = fundsToProcess[i];
      console.log(`\n[${i + 1}/${fundsToProcess.length}] Processing: ${fund.fund_name}`);
      console.log(`  Scheme Code: ${fund.scheme_code}, Current Records: ${fund.current_records}`);
      
      try {
        const importedCount = await importHistoricalDataForFund(fund);
        totalImported += importedCount;
        
        if (importedCount > 0) {
          successCount++;
          console.log(`  ✅ Imported ${importedCount} historical records`);
        } else {
          console.log(`  ⚠️ No additional data found`);
        }
        
        // Small delay to be respectful to API
        await new Promise(resolve => setTimeout(resolve, 500));
        
      } catch (error) {
        console.error(`  ❌ Error processing fund ${fund.scheme_code}:`, error.message);
      }
    }
    
    console.log(`\n📊 Import Summary:`);
    console.log(`  Funds processed: ${fundsToProcess.length}`);
    console.log(`  Successful imports: ${successCount}`);
    console.log(`  Total NAV records imported: ${totalImported}`);
    
  } catch (error) {
    console.error('Error in comprehensive historical import:', error);
  } finally {
    await pool.end();
  }
}

async function importHistoricalDataForFund(fund) {
  const schemeCode = fund.scheme_code;
  let totalImported = 0;
  
  // Generate date ranges for the last 5 years to get substantial historical data
  const dateRanges = generateDateRanges(60); // 5 years of monthly data
  
  for (const dateRange of dateRanges) {
    try {
      const historicalData = await fetchHistoricalNavFromMFAPI(schemeCode, dateRange.year, dateRange.month);
      
      if (historicalData && historicalData.length > 0) {
        const importedCount = await bulkInsertNavData(fund.id, historicalData);
        totalImported += importedCount;
      }
      
      // Small delay between requests
      await new Promise(resolve => setTimeout(resolve, 200));
      
    } catch (error) {
      console.log(`    Warning: Could not fetch data for ${dateRange.year}-${dateRange.month}: ${error.message}`);
    }
  }
  
  return totalImported;
}

function generateDateRanges(monthsBack = 60) {
  const ranges = [];
  const now = new Date();
  
  for (let i = 0; i < monthsBack; i++) {
    const date = new Date(now.getFullYear(), now.getMonth() - i, 1);
    ranges.push({
      year: date.getFullYear(),
      month: date.getMonth() + 1
    });
  }
  
  return ranges;
}

async function fetchHistoricalNavFromMFAPI(schemeCode, year, month) {
  const paddedMonth = month.toString().padStart(2, '0');
  const url = `https://api.mfapi.in/mf/${schemeCode}/${year}-${paddedMonth}-01`;
  
  try {
    const response = await axios.get(url, {
      timeout: 10000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MutualFundAnalyzer/1.0)'
      }
    });
    
    if (response.data && response.data.data && Array.isArray(response.data.data)) {
      return response.data.data.map(entry => ({
        nav_date: entry.date,
        nav_value: parseFloat(entry.nav)
      })).filter(entry => !isNaN(entry.nav_value) && entry.nav_value > 0);
    }
    
    return [];
  } catch (error) {
    if (error.response && error.response.status === 404) {
      return []; // No data available for this period
    }
    throw error;
  }
}

async function bulkInsertNavData(fundId, navDataArray) {
  if (!navDataArray || navDataArray.length === 0) {
    return 0;
  }
  
  try {
    const values = navDataArray.map(nav => 
      `(${fundId}, '${nav.nav_date}', ${nav.nav_value})`
    ).join(',');
    
    const insertQuery = `
      INSERT INTO nav_data (fund_id, nav_date, nav_value)
      VALUES ${values}
      ON CONFLICT (fund_id, nav_date) DO NOTHING
    `;
    
    const result = await pool.query(insertQuery);
    return result.rowCount || 0;
  } catch (error) {
    console.error('Error in bulk insert:', error.message);
    return 0;
  }
}

// Start the import process
startComprehensiveHistoricalImport()
  .then(() => {
    console.log('\n✅ Comprehensive historical import completed');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Import failed:', error);
    process.exit(1);
  });

export {
  startComprehensiveHistoricalImport,
  importHistoricalDataForFund
};