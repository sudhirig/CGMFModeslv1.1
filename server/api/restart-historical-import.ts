import express from 'express';
import { DateTime } from 'luxon';
import { db, pool, executeRawQuery } from '../db';
import { generateHistoricalDates, fetchHistoricalNavData } from '../amfi-scraper';
import axios from 'axios';
import * as cheerio from 'cheerio';

const router = express.Router();

/**
 * This endpoint restarts the historical NAV import process with improved reliability
 * It will first mark the current running import as failed, then start a new import
 * with better batching and error handling to fetch real historical data from AMFI
 */
router.post('/start', async (req, res) => {
  try {
    // First, update any stuck import processes to mark them as failed
    await executeRawQuery(`
      UPDATE etl_pipeline_runs
      SET status = 'FAILED',
          end_time = NOW(),
          error_message = 'Process was stuck and restarted by user'
      WHERE status = 'RUNNING'
      AND pipeline_name LIKE '%Historical%'
      AND start_time < NOW() - INTERVAL '30 minutes'
    `);

    // Get a list of all fund IDs with basic NAV data but no history
    const fundsQuery = `
      SELECT f.id, f.scheme_code, f.fund_name
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id
      GROUP BY f.id, f.scheme_code, f.fund_name
      HAVING COUNT(n.nav_date) <= 1
      ORDER BY f.id
      LIMIT 1000
    `;
    
    const fundsResult = await executeRawQuery(fundsQuery);
    const fundsToProcess = fundsResult.rows;
    
    if (fundsToProcess.length === 0) {
      return res.status(200).json({
        success: false,
        message: 'No funds found that need historical NAV data'
      });
    }
    
    // Insert a new ETL run record
    const etlRunResult = await executeRawQuery(`
      INSERT INTO etl_pipeline_runs 
        (pipeline_name, status, start_time, records_processed, error_message) 
      VALUES 
        ('Historical NAV Import Restart', 'RUNNING', NOW(), 0, 'Import started with real AMFI data')
      RETURNING id
    `);
    
    const etlRunId = etlRunResult.rows[0].id;
    
    // Generate historical dates - last 36 months
    const historicalDates = generateHistoricalDates(36);
    
    // Start the import process in the background
    const importPromise = processHistoricalImport(fundsToProcess, historicalDates, etlRunId);
    
    // Return immediately
    res.status(200).json({
      success: true,
      message: `Started historical NAV import for ${fundsToProcess.length} funds using real AMFI data`,
      etlRunId: etlRunId,
      fundsToProcess: fundsToProcess.length,
      monthsOfHistory: 36
    });
    
    // Continue in the background
    importPromise.catch(error => {
      console.error('Error in background historical import:', error);
    });
  } catch (error: any) {
    console.error('Error restarting historical import:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to restart historical import: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

/**
 * Process the historical import in batches, fetching real data from AMFI
 */
async function processHistoricalImport(
  funds: any[], 
  dates: { year: number, month: number }[], 
  etlRunId: number
): Promise<void> {
  let processedCount = 0;
  const batchSize = 5; // Process 5 funds at a time to avoid overwhelming the AMFI API
  
  for (let i = 0; i < funds.length; i += batchSize) {
    const batch = funds.slice(i, i + batchSize);
    
    try {
      // Process this batch
      await Promise.all(batch.map(fund => 
        processHistoricalFund(fund, dates)
      ));
      
      processedCount += batch.length;
      
      // Update the ETL run record
      await executeRawQuery(`
        UPDATE etl_pipeline_runs
        SET records_processed = $1,
            error_message = 'Processing fund batch ' + $2 + ' of ' + $3 + ' with real AMFI data'
        WHERE id = $4
      `, [processedCount, Math.floor(i / batchSize) + 1, Math.ceil(funds.length / batchSize), etlRunId]);
      
      console.log(`Processed historical NAV for ${processedCount} of ${funds.length} funds using real AMFI data`);
    } catch (error) {
      console.error(`Error processing batch starting at index ${i}:`, error);
    }
    
    // Add a delay to prevent overwhelming the AMFI API
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  // Mark the ETL run as completed
  await executeRawQuery(`
    UPDATE etl_pipeline_runs
    SET status = 'COMPLETED',
        end_time = NOW(),
        error_message = 'Successfully imported real historical NAV data for ' + $1 + ' funds'
    WHERE id = $2
  `, [processedCount, etlRunId]);
}

/**
 * Process historical NAV data for a single fund by fetching from AMFI
 */
async function processHistoricalFund(fund: any, dates: { year: number, month: number }[]): Promise<void> {
  try {
    // Sort dates from newest to oldest (AMFI API works better this way)
    const sortedDates = [...dates].sort((a, b) => {
      if (a.year !== b.year) return b.year - a.year; // Descending by year
      return b.month - a.month; // Descending by month
    });
    
    // Process in smaller batches to avoid timeout issues
    const batchSize = 3; // Process 3 months at a time
    
    for (let i = 0; i < sortedDates.length; i += batchSize) {
      const dateBatch = sortedDates.slice(i, i + batchSize);
      const navEntries = [];
      
      for (const { year, month } of dateBatch) {
        try {
          // Fetch real NAV data from AMFI for this month/year
          const navData = await fetchHistoricalNavDataForFund(fund.scheme_code, year, month);
          
          if (navData && navData.length > 0) {
            // Add all the NAV entries for this month
            for (const entry of navData) {
              navEntries.push({
                fund_id: fund.id,
                nav_date: entry.date,
                nav_value: entry.nav
              });
            }
            
            console.log(`Fetched ${navData.length} NAV entries for fund ${fund.scheme_code} (${year}-${month})`);
          } else {
            console.log(`No NAV data available for fund ${fund.scheme_code} (${year}-${month})`);
          }
        } catch (error) {
          console.error(`Error fetching NAV data for fund ${fund.scheme_code} (${year}-${month}):`, error);
        }
        
        // Small delay between month requests
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      
      // Batch insert the NAV entries if we have any
      if (navEntries.length > 0) {
        try {
          const placeholders = navEntries.map((_, idx) => 
            `($${idx*3 + 1}, $${idx*3 + 2}, $${idx*3 + 3})`
          ).join(', ');
          
          const values = navEntries.flatMap(entry => 
            [entry.fund_id, entry.nav_date, entry.nav_value]
          );
          
          // Use ON CONFLICT to avoid duplicate entries
          await executeRawQuery(`
            INSERT INTO nav_data (fund_id, nav_date, nav_value)
            VALUES ${placeholders}
            ON CONFLICT (fund_id, nav_date) DO UPDATE
            SET nav_value = EXCLUDED.nav_value
          `, values);
          
          console.log(`Inserted ${navEntries.length} historical NAV entries for fund ${fund.id}`);
        } catch (error) {
          console.error(`Error inserting NAV data for fund ${fund.id}:`, error);
        }
      }
      
      // Delay between batches to avoid overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  } catch (error) {
    console.error(`Error processing historical data for fund ${fund.id}:`, error);
  }
}

/**
 * Fetch historical NAV data for a specific fund from AMFI for a given month/year
 */
async function fetchHistoricalNavDataForFund(schemeCode: string, year: number, month: number): Promise<{ date: string, nav: string }[]> {
  try {
    // Calculate the from and to dates for the specified month
    const fromDate = new Date(year, month - 1, 1);
    const toDate = new Date(year, month, 0); // Last day of the month
    
    // Format dates as required by AMFI API (dd-MMM-yyyy)
    const formatDate = (date: Date) => {
      const day = date.getDate().toString().padStart(2, '0');
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const monthStr = monthNames[date.getMonth()];
      const yearStr = date.getFullYear();
      return `${day}-${monthStr}-${yearStr}`;
    };
    
    const fromDateStr = formatDate(fromDate);
    const toDateStr = formatDate(toDate);
    
    // AMFI NAV history URL (note: actual URL may vary, this is based on common patterns)
    const url = 'https://www.amfiindia.com/spages/NAVHistoryReport.aspx';
    
    // Fetch the historical NAV data
    const response = await axios.post(url, new URLSearchParams({
      'SchemeCode': schemeCode,
      'FromDate': fromDateStr,
      'ToDate': toDateStr
    }), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      },
      timeout: 15000 // 15 second timeout
    });
    
    // Parse the HTML response to extract NAV data
    const $ = cheerio.load(response.data);
    const navEntries: { date: string, nav: string }[] = [];
    
    // Find the table containing NAV history
    $('table.table-bordered tr').each((index, element) => {
      // Skip header row
      if (index === 0) return;
      
      const columns = $(element).find('td');
      if (columns.length >= 2) {
        const dateText = $(columns[0]).text().trim();
        const navText = $(columns[1]).text().trim();
        
        if (dateText && navText && !isNaN(parseFloat(navText))) {
          // Convert date from dd-MMM-yyyy to yyyy-MM-dd format for database
          const dateParts = dateText.split('-');
          if (dateParts.length === 3) {
            const day = dateParts[0];
            const monthAbbr = dateParts[1];
            const year = dateParts[2];
            
            const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            const monthIndex = monthNames.findIndex(m => m === monthAbbr);
            
            if (monthIndex !== -1) {
              const monthNum = (monthIndex + 1).toString().padStart(2, '0');
              const formattedDate = `${year}-${monthNum}-${day}`;
              
              navEntries.push({
                date: formattedDate,
                nav: navText
              });
            }
          }
        }
      }
    });
    
    return navEntries;
  } catch (error) {
    console.error(`Error fetching historical NAV for scheme ${schemeCode} (${year}-${month}):`, error);
    return [];
  }
}

/**
 * Check the status of the historical import process
 */
router.get('/status', async (req, res) => {
  try {
    // Get the latest ETL run status
    const etlRunResult = await executeRawQuery(`
      SELECT * FROM etl_pipeline_runs
      WHERE pipeline_name = 'Historical NAV Import Restart'
      ORDER BY start_time DESC
      LIMIT 1
    `);
    
    const etlRun = etlRunResult.rows[0];
    
    // Get statistics on historical NAV data
    const navStatsResult = await executeRawQuery(`
      SELECT 
        COUNT(DISTINCT fund_id) as funds_with_history,
        COUNT(*) as total_nav_records,
        MIN(nav_date) as earliest_date,
        MAX(nav_date) as latest_date
      FROM nav_data
    `);
    
    const navStats = navStatsResult.rows[0];
    
    // Get a sample of funds with the most historical data
    const topFundsResult = await executeRawQuery(`
      SELECT 
        f.id, 
        f.fund_name, 
        COUNT(n.nav_date) as nav_count,
        MIN(n.nav_date) as earliest_date,
        MAX(n.nav_date) as latest_date
      FROM 
        funds f
      JOIN 
        nav_data n ON f.id = n.fund_id
      GROUP BY 
        f.id, f.fund_name
      ORDER BY 
        COUNT(n.nav_date) DESC
      LIMIT 10
    `);
    
    res.status(200).json({
      success: true,
      etlRun,
      navStats,
      topFundsWithHistory: topFundsResult.rows
    });
  } catch (error: any) {
    console.error('Error getting historical import status:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to get historical import status: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

export default router;