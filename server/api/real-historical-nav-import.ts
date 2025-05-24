import express from 'express';
import { executeRawQuery } from '../db';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { storage } from '../storage';

const router = express.Router();

/**
 * This endpoint initiates a genuine historical NAV data import process
 * that fetches ONLY real data from AMFI, with no synthetic data generation
 */
router.post('/start', async (req, res) => {
  try {
    // First, cancel any running imports
    await executeRawQuery(`
      UPDATE etl_pipeline_runs
      SET status = 'FAILED',
          end_time = NOW(),
          error_message = 'Process canceled to start authentic data import'
      WHERE status = 'RUNNING'
      AND pipeline_name LIKE '%historical%'
    `);

    // Create a new ETL run for this process
    const etlRun = await storage.createETLRun({
      pipelineName: 'authentic_historical_import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Starting authentic historical NAV data import process'
    });

    // Get funds that need historical data (have only one NAV entry)
    // Process all funds in the database, prioritizing Q1 and Q2 funds first
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.scheme_code, f.fund_name, fs.quartile
      FROM funds f
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = (
        SELECT MAX(score_date) FROM fund_scores WHERE fund_id = f.id
      )
      JOIN nav_data n ON f.id = n.fund_id
      GROUP BY f.id, f.scheme_code, f.fund_name, fs.quartile
      HAVING COUNT(n.nav_date) <= 1
      ORDER BY 
        CASE 
          WHEN fs.quartile = 1 THEN 1
          WHEN fs.quartile = 2 THEN 2
          WHEN fs.quartile = 3 THEN 3
          WHEN fs.quartile = 4 THEN 4
          ELSE 5
        END,
        f.id
    `);
    
    const fundsToProcess = fundsResult.rows;
    
    if (fundsToProcess.length === 0) {
      await storage.updateETLRun(etlRun.id, {
        status: 'COMPLETED',
        endTime: new Date(),
        errorMessage: 'No funds found that need historical NAV data'
      });
      
      return res.status(200).json({
        success: true,
        message: 'No funds found that need historical NAV data'
      });
    }
    
    // Start the import process in the background
    processAuthenticHistoricalImport(fundsToProcess, etlRun.id);
    
    res.status(200).json({
      success: true,
      message: `Started authentic historical NAV import for ${fundsToProcess.length} funds`,
      etlRunId: etlRun.id
    });
  } catch (error: any) {
    console.error('Error starting authentic historical import:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to start authentic historical import: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

/**
 * Process the authentic historical import in batches
 */
async function processAuthenticHistoricalImport(funds: any[], etlRunId: number): Promise<void> {
  let processedCount = 0;
  const batchSize = 5; // Process 5 funds at a time to avoid overwhelming the AMFI API
  
  for (let i = 0; i < funds.length; i += batchSize) {
    const batch = funds.slice(i, i + batchSize);
    
    try {
      // Process this batch
      await Promise.all(batch.map(fund => 
        processAuthenticHistoricalFund(fund)
      ));
      
      processedCount += batch.length;
      
      // Update the ETL run record
      await storage.updateETLRun(etlRunId, {
        recordsProcessed: processedCount,
        errorMessage: `Imported batch ${Math.floor(i / batchSize) + 1}, progress: ${processedCount}/${funds.length}`
      });
      
      console.log(`Imported batch ${Math.floor(i / batchSize) + 1}, progress: ${processedCount}/${funds.length}`);
    } catch (error) {
      console.error(`Error processing batch starting at index ${i}:`, error);
    }
    
    // Add a delay to prevent overwhelming the AMFI API
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  // Mark the ETL run as completed
  await storage.updateETLRun(etlRunId, {
    status: 'COMPLETED',
    endTime: new Date(),
    errorMessage: `Successfully imported authentic historical NAV data for ${processedCount} funds`
  });
}

/**
 * Process authentic historical NAV data for a single fund
 */
async function processAuthenticHistoricalFund(fund: any): Promise<void> {
  try {
    // We'll process 36 months of data (3 years)
    const months = 36;
    
    // Generate dates for the last X months
    const dates = generateMonthsBack(months);
    
    // Process in smaller batches
    const batchSize = 3; // 3 months at a time
    
    for (let i = 0; i < dates.length; i += batchSize) {
      const dateBatch = dates.slice(i, i + batchSize);
      const navEntries = [];
      
      for (const { year, month } of dateBatch) {
        try {
          // Fetch real NAV data from AMFI for this month/year
          const navData = await fetchAuthenticHistoricalNav(fund.scheme_code, year, month);
          
          if (navData && navData.length > 0) {
            // Add all the NAV entries for this month
            for (const entry of navData) {
              navEntries.push({
                fundId: fund.id,
                navDate: entry.date,
                navValue: entry.nav // Keep as string to match the expected type
              });
            }
            
            console.log(`Fetched ${navData.length} authentic NAV entries for fund ${fund.scheme_code} (${year}-${month})`);
          } else {
            console.log(`No authentic NAV data available for fund ${fund.scheme_code} (${year}-${month})`);
          }
        } catch (error) {
          console.error(`Error fetching authentic NAV data for fund ${fund.scheme_code} (${year}-${month}):`, error);
        }
        
        // Small delay between requests
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      
      // Batch insert the NAV entries
      if (navEntries.length > 0) {
        try {
          // Convert entries to match the expected schema type
          const formattedEntries = navEntries.map(entry => ({
            fundId: entry.fundId,
            navDate: entry.navDate,
            navValue: entry.navValue
          }));
          
          // Use the storage interface to insert NAV data
          await storage.bulkInsertNavData(formattedEntries);
          console.log(`Inserted ${navEntries.length} authentic historical NAV entries for fund ${fund.id}`);
        } catch (error) {
          console.error(`Error inserting authentic NAV data for fund ${fund.id}:`, error);
        }
      }
      
      // Delay between batches
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  } catch (error) {
    console.error(`Error processing authentic historical data for fund ${fund.id}:`, error);
  }
}

/**
 * Generate array of months going back from current date
 */
function generateMonthsBack(monthsBack: number): { year: number, month: number }[] {
  const dates = [];
  const today = new Date();
  let currentMonth = today.getMonth(); // 0-11
  let currentYear = today.getFullYear();
  
  for (let i = 0; i < monthsBack; i++) {
    dates.push({ year: currentYear, month: currentMonth + 1 }); // Convert to 1-12 format
    
    // Go back one month
    currentMonth--;
    if (currentMonth < 0) {
      currentMonth = 11; // December
      currentYear--;
    }
  }
  
  return dates;
}

/**
 * Fetch authentic historical NAV data from AMFI for a specific fund and month
 */
async function fetchAuthenticHistoricalNav(schemeCode: string, year: number, month: number): Promise<{ date: string, nav: string }[]> {
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
    
    // AMFI NAV history URL 
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
              const formattedDate = `${year}-${monthNum}-${day.padStart(2, '0')}`;
              
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
    console.error(`Error fetching authentic historical NAV for scheme ${schemeCode} (${year}-${month}):`, error);
    return [];
  }
}

/**
 * Check the status of the authentic historical import process
 */
router.get('/status', async (req, res) => {
  try {
    // Get the latest ETL run status
    const etlRuns = await storage.getETLRuns('authentic_historical_import', 1);
    const etlRun = etlRuns.length > 0 ? etlRuns[0] : null;
    
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
    console.error('Error getting authentic historical import status:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to get authentic historical import status: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

export default router;