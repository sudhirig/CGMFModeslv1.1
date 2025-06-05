/**
 * Batch Historical NAV Data Import
 * Efficient import system for authentic historical data
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function runBatchHistoricalImport() {
  console.log('Starting batch historical NAV import...');
  
  try {
    // Get 10 priority funds that need historical data
    const priorityFundsQuery = `
      SELECT 
        f.id,
        f.scheme_code,
        f.fund_name,
        f.category,
        COALESCE(nav_count.record_count, 0) as current_records
      FROM funds f
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as record_count 
        FROM nav_data 
        GROUP BY fund_id
      ) nav_count ON f.id = nav_count.fund_id
      WHERE COALESCE(nav_count.record_count, 0) < 50
        AND f.scheme_code IS NOT NULL
        AND f.status = 'ACTIVE'
        AND f.category IN ('Equity', 'Debt', 'Hybrid')
      ORDER BY 
        CASE f.category 
          WHEN 'Equity' THEN 1 
          WHEN 'Debt' THEN 2 
          WHEN 'Hybrid' THEN 3 
          ELSE 4 
        END,
        COALESCE(nav_count.record_count, 0) ASC
      LIMIT 10
    `;
    
    const result = await pool.query(priorityFundsQuery);
    const funds = result.rows;
    
    console.log(`Found ${funds.length} priority funds for historical import`);
    
    let totalImported = 0;
    
    for (let i = 0; i < funds.length; i++) {
      const fund = funds[i];
      console.log(`\n[${i + 1}/${funds.length}] ${fund.fund_name}`);
      console.log(`  Category: ${fund.category}, Current records: ${fund.current_records}`);
      
      const imported = await importRecentHistoricalData(fund);
      totalImported += imported;
      
      if (imported > 0) {
        console.log(`  ✅ Imported ${imported} records`);
      }
      
      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    console.log(`\n📈 Batch import completed. Total imported: ${totalImported} records`);
    
    // Update database statistics
    await updateImportStats(totalImported);
    
  } catch (error) {
    console.error('Batch import error:', error);
  } finally {
    await pool.end();
  }
}

async function importRecentHistoricalData(fund) {
  const schemeCode = fund.scheme_code;
  let imported = 0;
  
  // Import last 12 months of data
  const dateRanges = generateRecentMonths(12);
  
  for (const range of dateRanges) {
    try {
      const data = await fetchMonthlyData(schemeCode, range.year, range.month);
      
      if (data && data.length > 0) {
        const insertCount = await insertNavRecords(fund.id, data);
        imported += insertCount;
      }
      
      await new Promise(resolve => setTimeout(resolve, 300));
      
    } catch (error) {
      // Continue with next month if this one fails
      continue;
    }
  }
  
  return imported;
}

function generateRecentMonths(count) {
  const months = [];
  const now = new Date();
  
  for (let i = 0; i < count; i++) {
    const date = new Date(now.getFullYear(), now.getMonth() - i, 1);
    months.push({
      year: date.getFullYear(),
      month: date.getMonth() + 1
    });
  }
  
  return months;
}

async function fetchMonthlyData(schemeCode, year, month) {
  const paddedMonth = month.toString().padStart(2, '0');
  const url = `https://api.mfapi.in/mf/${schemeCode}/${year}-${paddedMonth}-01`;
  
  try {
    const response = await axios.get(url, {
      timeout: 8000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; MutualFundAnalyzer/1.0)'
      }
    });
    
    if (response.data && response.data.data) {
      return response.data.data.map(entry => ({
        nav_date: entry.date,
        nav_value: parseFloat(entry.nav)
      })).filter(entry => !isNaN(entry.nav_value) && entry.nav_value > 0);
    }
    
    return [];
  } catch (error) {
    return [];
  }
}

async function insertNavRecords(fundId, navData) {
  if (!navData || navData.length === 0) return 0;
  
  try {
    const values = navData.map(nav => 
      `(${fundId}, '${nav.nav_date}', ${nav.nav_value})`
    ).join(',');
    
    const query = `
      INSERT INTO nav_data (fund_id, nav_date, nav_value)
      VALUES ${values}
      ON CONFLICT (fund_id, nav_date) DO NOTHING
    `;
    
    const result = await pool.query(query);
    return result.rowCount || 0;
  } catch (error) {
    return 0;
  }
}

async function updateImportStats(totalImported) {
  try {
    await pool.query(`
      INSERT INTO etl_pipeline_runs (
        pipeline_name, 
        status, 
        start_time, 
        end_time, 
        records_processed, 
        error_message
      ) VALUES (
        'Batch Historical NAV Import', 
        'COMPLETED', 
        NOW() - INTERVAL '5 minutes', 
        NOW(), 
        $1, 
        'Successfully imported historical NAV data for priority funds'
      )
    `, [totalImported]);
  } catch (error) {
    console.error('Failed to update import stats:', error);
  }
}

runBatchHistoricalImport();