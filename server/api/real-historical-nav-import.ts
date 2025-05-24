import express from 'express';
import { executeRawQuery } from '../db';
import axios from 'axios';
import * as cheerio from 'cheerio';
import { storage } from '../storage';

const router = express.Router();

/**
 * This endpoint restarts any stuck historical NAV import
 * It will cancel the current stuck import and start a fresh one
 */
router.post('/restart', async (req, res) => {
  try {
    // First, cancel any running imports
    await executeRawQuery(`
      UPDATE etl_pipeline_runs
      SET status = 'FAILED',
          end_time = NOW(),
          error_message = 'Process canceled due to being stuck - restarted with improved implementation'
      WHERE status = 'RUNNING'
      AND pipeline_name = 'authentic_historical_import'
    `);
    
    // Create a new ETL run for this process
    const etlRun = await storage.createETLRun({
      pipelineName: 'authentic_historical_import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Restarting authentic historical NAV data import with improved reliability'
    });
    
    // Get funds that need historical data - same as original query but with limit to focus on high priority funds
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.scheme_code, f.fund_name, fs.quartile
      FROM funds f
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = (
        SELECT MAX(score_date) FROM fund_scores WHERE fund_id = f.id
      )
      JOIN nav_data n ON f.id = n.fund_id
      GROUP BY f.id, f.scheme_code, f.fund_name, fs.quartile
      HAVING COUNT(n.nav_date) <= 2
      ORDER BY 
        CASE 
          WHEN fs.quartile = 1 THEN 1
          WHEN fs.quartile = 2 THEN 2
          WHEN fs.quartile = 3 THEN 3
          WHEN fs.quartile = 4 THEN 4
          ELSE 5
        END,
        f.id
      LIMIT 500  -- Process top priority funds first
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
      message: `Restarted authentic historical NAV import for ${fundsToProcess.length} funds with improved implementation`,
      etlRunId: etlRun.id
    });
  } catch (error: any) {
    console.error('Error restarting authentic historical import:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to restart authentic historical import: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

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
  const batchSize = 2; // Process just 2 funds at a time to avoid overwhelming the AMFI API
  
  await storage.updateETLRun(etlRunId, {
    recordsProcessed: processedCount,
    errorMessage: `Starting improved historical NAV import for ${funds.length} funds...`
  });
  
  // Process each fund individually to prevent Promise.all from getting stuck
  for (let i = 0; i < funds.length; i += batchSize) {
    const batch = funds.slice(i, i + batchSize);
    
    try {
      // Process each fund in the batch sequentially
      for (const fund of batch) {
        try {
          await storage.updateETLRun(etlRunId, {
            errorMessage: `Processing fund ${processedCount + 1}/${funds.length}: ${fund.fund_name} (ID: ${fund.id})`
          });
          
          // Process this fund with timeout protection
          await Promise.race([
            processAuthenticHistoricalFund(fund),
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error(`Timeout processing fund ${fund.id}`)), 60000)
            )
          ]);
          
          // Only increment count if processing completes successfully
          processedCount++;
          
          // Update the ETL run record after each fund
          await storage.updateETLRun(etlRunId, {
            recordsProcessed: processedCount,
            errorMessage: `Imported fund ${processedCount}/${funds.length}: ${fund.fund_name}`
          });
          
          console.log(`Imported fund ${processedCount}/${funds.length}: ${fund.fund_name} (ID: ${fund.id})`);
        } catch (fundError) {
          console.error(`Error processing fund ${fund.id} (${fund.fund_name}):`, fundError);
          await storage.updateETLRun(etlRunId, {
            errorMessage: `Error with fund ${fund.id}, continuing to next fund... (${processedCount}/${funds.length})`
          });
          // We don't increment processedCount for failed funds
        }
        
        // Add a delay between individual funds
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    } catch (batchError) {
      console.error(`Error processing batch starting at index ${i}:`, batchError);
    }
    
    // Add a delay between batches
    await new Promise(resolve => setTimeout(resolve, 3000));
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
    // We'll process 24 months of data (2 years) to speed up the import and focus on more recent data
    const months = 24;
    
    // Generate dates for the last X months
    const dates = generateMonthsBack(months);
    
    // Process in smaller batches with better error handling
    const batchSize = 2; // 2 months at a time to be more resilient
    let totalNavEntriesInserted = 0;
    
    for (let i = 0; i < dates.length; i += batchSize) {
      const dateBatch = dates.slice(i, i + batchSize);
      const allNavEntries = [];
      
      for (const { year, month } of dateBatch) {
        try {
          console.log(`Processing fund ${fund.id} (${fund.fund_name}): ${year}-${month}`);
          
          // Fetch real NAV data from AMFI for this month/year with timeout protection
          let navData;
          try {
            navData = await Promise.race([
              fetchAuthenticHistoricalNav(fund.scheme_code, year, month),
              new Promise<never>((_, reject) => 
                setTimeout(() => reject(new Error(`Timeout fetching NAV for ${year}-${month}`)), 20000)
              )
            ]);
          } catch (timeoutError) {
            console.error(`Timeout fetching NAV data for scheme ${fund.scheme_code} (${year}-${month})`);
            continue; // Skip this month and move to the next
          }
          
          if (navData && navData.length > 0) {
            console.log(`Found ${navData.length} NAV entries for ${fund.fund_name} (${year}-${month})`);
            
            // Add all the NAV entries for this month
            for (const entry of navData) {
              allNavEntries.push({
                fundId: fund.id,
                navDate: entry.date,
                navValue: parseFloat(entry.nav)
              });
            }
          } else {
            console.log(`No authentic NAV data available for fund ${fund.scheme_code} (${year}-${month})`);
          }
        } catch (error) {
          console.error(`Error fetching authentic NAV data for fund ${fund.scheme_code} (${year}-${month}):`, error);
        }
        
        // Small delay between months to avoid overwhelming the AMFI API
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      // Process this batch of NAV entries if we found any
      if (allNavEntries.length > 0) {
        try {
          // Batch insert using direct SQL for better performance with conflict handling
          const placeholders = [];
          const values = [];
          let paramIndex = 1;
          
          for (const entry of allNavEntries) {
            placeholders.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2})`);
            values.push(entry.fundId, entry.navDate, entry.navValue);
            paramIndex += 3;
          }
          
          // Use an UPSERT operation for better reliability
          const query = `
            INSERT INTO nav_data (fund_id, nav_date, nav_value)
            VALUES ${placeholders.join(', ')}
            ON CONFLICT (fund_id, nav_date) 
            DO UPDATE SET nav_value = EXCLUDED.nav_value
          `;
          
          await executeRawQuery(query, values);
          totalNavEntriesInserted += allNavEntries.length;
          
          console.log(`Successfully inserted/updated ${allNavEntries.length} NAV entries for fund ${fund.id} (${fund.fund_name})`);
        } catch (insertError) {
          console.error(`Error inserting NAV data batch for fund ${fund.id}:`, insertError);
          
          // If batch insert fails, try individual inserts as fallback
          try {
            console.log(`Attempting individual inserts for ${allNavEntries.length} entries...`);
            for (const entry of allNavEntries) {
              try {
                await executeRawQuery(`
                  INSERT INTO nav_data (fund_id, nav_date, nav_value)
                  VALUES ($1, $2, $3)
                  ON CONFLICT (fund_id, nav_date) DO UPDATE SET nav_value = EXCLUDED.nav_value
                `, [entry.fundId, entry.navDate, entry.navValue]);
                
                totalNavEntriesInserted++;
              } catch (singleInsertError) {
                console.error(`Error inserting individual NAV entry:`, singleInsertError);
              }
            }
          } catch (fallbackError) {
            console.error(`Fallback insert strategy also failed:`, fallbackError);
          }
        }
      }
      
      // Add a delay between batches to prevent overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    console.log(`Completed processing for fund ${fund.id} (${fund.fund_name}). Total NAV entries inserted: ${totalNavEntriesInserted}`);
  } catch (error) {
    console.error(`Error processing authentic historical data for fund ${fund.id}:`, error);
    throw error; // Re-throw to let the caller handle it
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
    
    // We'll try multiple AMFI endpoints - they sometimes change their API structure
    const urls = [
      'https://www.amfiindia.com/spages/NAVHistoryReport.aspx',
      'https://www.amfiindia.com/nav-history-download'
    ];
    
    const navEntries: { date: string, nav: string }[] = [];
    let success = false;
    
    // Implement retry mechanism for greater reliability
    for (const url of urls) {
      if (success) break; // If we already got data from one endpoint, don't try others
      
      let retryCount = 0;
      const maxRetries = 3;
      
      while (retryCount < maxRetries && !success) {
        try {
          console.log(`Attempting to fetch historical NAV for scheme ${schemeCode} (${year}-${month}) from ${url}, attempt ${retryCount + 1}`);
          
          // Different request format based on endpoint
          let response;
          if (url.includes('NAVHistoryReport')) {
            // First endpoint method
            response = await axios.post(url, new URLSearchParams({
              'SchemeCode': schemeCode,
              'FromDate': fromDateStr,
              'ToDate': toDateStr
            }), {
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9'
              },
              timeout: 20000 // 20 second timeout (shorter to fail faster)
            });
          } else {
            // Second endpoint method
            response = await axios.get(`${url}?schemecode=${schemeCode}&fromdate=${fromDateStr}&todate=${toDateStr}`, {
              headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9'
              },
              timeout: 20000
            });
          }
          
          // If we got data, try to parse it
          if (response && response.data) {
            console.log(`Got response for scheme ${schemeCode} (${year}-${month}), parsing...`);
            
            // Parse the HTML response to extract NAV data
            const $ = cheerio.load(response.data);
            
            // Try multiple table selectors as AMFI sometimes changes their HTML structure
            const tableSelectors = [
              'table.table-bordered',
              'table.table',
              'table#tblHistoricNAV',
              'table'
            ];
            
            let foundData = false;
            
            // Try each selector until we find data
            for (const selector of tableSelectors) {
              if (foundData) break;
              
              // Check if we found a table with this selector
              const table = $(selector);
              if (table.length === 0) continue;
              
              // Now try to parse the table rows
              table.find('tr').each((index, element) => {
                // Skip header row
                if (index === 0) return;
                
                const columns = $(element).find('td');
                // Check if we have at least date and NAV columns
                if (columns.length >= 2) {
                  const dateText = $(columns[0]).text().trim();
                  const navText = $(columns[1]).text().trim();
                  
                  if (dateText && navText && !isNaN(parseFloat(navText))) {
                    foundData = true;
                    
                    // Convert date from dd-MMM-yyyy to yyyy-MM-dd format for database
                    try {
                      const dateParts = dateText.split('-');
                      if (dateParts.length === 3) {
                        const day = dateParts[0];
                        const monthAbbr = dateParts[1];
                        const year = dateParts[2];
                        
                        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                        const monthIndex = monthNames.findIndex(m => m.toLowerCase() === monthAbbr.toLowerCase());
                        
                        if (monthIndex !== -1) {
                          const monthNum = (monthIndex + 1).toString().padStart(2, '0');
                          const paddedDay = day.padStart(2, '0');
                          const formattedDate = `${year}-${monthNum}-${paddedDay}`;
                          
                          navEntries.push({
                            date: formattedDate,
                            nav: navText
                          });
                        }
                      }
                    } catch (dateParseError) {
                      console.error(`Error parsing date ${dateText}:`, dateParseError);
                    }
                  }
                }
              });
              
              if (foundData) {
                success = true;
                console.log(`Found ${navEntries.length} NAV entries for scheme ${schemeCode} (${year}-${month})`);
                break; // Break out of the tableSelectors loop
              }
            }
            
            if (success) break; // Break out of the retry loop
          }
          
          retryCount++;
          
          // Wait before retrying (exponential backoff)
          if (!success && retryCount < maxRetries) {
            await new Promise(resolve => setTimeout(resolve, 2000 * Math.pow(2, retryCount)));
          }
        } catch (error) {
          console.error(`AMFI fetch attempt ${retryCount + 1} failed for scheme ${schemeCode} (${year}-${month}):`, error);
          retryCount++;
          
          // Wait before retrying
          if (retryCount < maxRetries) {
            await new Promise(resolve => setTimeout(resolve, 2000 * Math.pow(2, retryCount)));
          }
        }
      }
    }
    
    // If we couldn't get any data after all attempts, return empty array
    if (navEntries.length === 0) {
      console.error(`Could not fetch any real NAV data for scheme ${schemeCode} (${year}-${month}) after all attempts`);
      return [];
    }
    
    return navEntries;
  } catch (error) {
    console.error(`Error fetching historical NAV for scheme ${schemeCode} (${year}-${month}):`, error);
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