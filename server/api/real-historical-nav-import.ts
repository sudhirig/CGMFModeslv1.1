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
 * Process the authentic historical import in batches with improved performance
 */
async function processAuthenticHistoricalImport(funds: any[], etlRunId: number): Promise<void> {
  let processedCount = 0;
  let successCount = 0;
  let errorCount = 0;
  let navEntriesImported = 0;
  
  // Increased batch size for better performance
  const batchSize = 5; // Process 5 funds at a time - better throughput while still avoiding overwhelming the API
  
  // Detailed statistics for logging
  const stats = {
    startTime: new Date(),
    fundsByCategory: {} as Record<string, number>,
    fundsByQuartile: {} as Record<string, number>,
    totalNavEntries: 0,
    oldestDate: null as string | null,
    newestDate: null as string | null,
    errors: [] as string[]
  };
  
  await storage.updateETLRun(etlRunId, {
    recordsProcessed: processedCount,
    errorMessage: `Starting improved historical NAV import for ${funds.length} funds with enhanced performance...`
  });
  
  console.log(`=== Starting historical NAV import for ${funds.length} funds ===`);
  console.log(`Batch size: ${batchSize} funds per batch`);
  console.log(`Time started: ${stats.startTime.toISOString()}`);
  
  // Process in parallel batches for better performance
  for (let i = 0; i < funds.length; i += batchSize) {
    const batch = funds.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(funds.length / batchSize);
    
    console.log(`\n--- Processing batch ${batchNumber}/${totalBatches} (${batch.length} funds) ---`);
    
    try {
      // Update ETL status at the start of each batch
      await storage.updateETLRun(etlRunId, {
        recordsProcessed: processedCount,
        errorMessage: `Processing batch ${batchNumber}/${totalBatches}: ${processedCount} funds complete, ${successCount} successful, ${errorCount} errors`
      });
      
      // Process each fund in the batch with improved parallel processing
      const batchPromises = batch.map(fund => {
        return (async () => {
          const fundStart = Date.now();
          
          try {
            // Track fund categories and quartiles for reporting
            if (fund.category) {
              stats.fundsByCategory[fund.category] = (stats.fundsByCategory[fund.category] || 0) + 1;
            }
            
            if (fund.quartile) {
              stats.fundsByQuartile[fund.quartile] = (stats.fundsByQuartile[fund.quartile] || 0) + 1;
            }
            
            console.log(`Processing fund: ${fund.fund_name} (ID: ${fund.id}, Scheme: ${fund.scheme_code})`);
            
            // Process with timeout protection - increased to 120 seconds for funds with more data
            const result = await Promise.race([
              processAuthenticHistoricalFund(fund),
              new Promise<{success: boolean, message: string, navCount: number}>((_, reject) => 
                setTimeout(() => reject(new Error(`Timeout processing fund ${fund.id} after 120 seconds`)), 120000)
              )
            ]);
            
            // Track successful processing
            if (result.success) {
              successCount++;
              navEntriesImported += result.navCount;
              stats.totalNavEntries += result.navCount;
              
              // Track date ranges for reporting
              if (result.oldestDate) {
                if (!stats.oldestDate || result.oldestDate < stats.oldestDate) {
                  stats.oldestDate = result.oldestDate;
                }
              }
              
              if (result.newestDate) {
                if (!stats.newestDate || result.newestDate > stats.newestDate) {
                  stats.newestDate = result.newestDate;
                }
              }
              
              const processingTime = ((Date.now() - fundStart) / 1000).toFixed(1);
              console.log(`✓ Success: ${fund.fund_name} - Imported ${result.navCount} NAV entries in ${processingTime}s`);
            } else {
              errorCount++;
              stats.errors.push(`${fund.id} (${fund.fund_name}): ${result.message}`);
              console.log(`✗ Failed: ${fund.fund_name} - ${result.message}`);
            }
          } catch (fundError: any) {
            errorCount++;
            const errorMessage = fundError.message || 'Unknown error';
            stats.errors.push(`${fund.id} (${fund.fund_name}): ${errorMessage}`);
            console.error(`✗ Error processing fund ${fund.id} (${fund.fund_name}):`, fundError);
          }
          
          // Return success status for counting
          return { fundId: fund.id, fundName: fund.fund_name };
        })();
      });
      
      // Wait for all funds in this batch to complete
      await Promise.all(batchPromises);
      
      // Update processed count after batch completes
      processedCount += batch.length;
      
      // Update ETL run with detailed progress
      await storage.updateETLRun(etlRunId, {
        recordsProcessed: processedCount,
        errorMessage: `Batch ${batchNumber}/${totalBatches} complete: ${processedCount}/${funds.length} funds processed, ${navEntriesImported} NAV entries imported`
      });
      
      // Log detailed batch summary
      const elapsedMinutes = ((Date.now() - stats.startTime.getTime()) / 60000).toFixed(1);
      console.log(`\n--- Batch ${batchNumber}/${totalBatches} complete ---`);
      console.log(`Progress: ${processedCount}/${funds.length} funds (${Math.round(processedCount/funds.length*100)}%)`);
      console.log(`Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)`);
      console.log(`NAV entries: ${navEntriesImported} total`);
      console.log(`Time elapsed: ${elapsedMinutes} minutes`);
      
      // Calculate and log estimated completion time
      if (batchNumber > 1) {
        const avgTimePerBatch = (Date.now() - stats.startTime.getTime()) / batchNumber;
        const remainingBatches = totalBatches - batchNumber;
        const estimatedRemainingMs = avgTimePerBatch * remainingBatches;
        const estimatedCompletion = new Date(Date.now() + estimatedRemainingMs);
        console.log(`Estimated completion: ${estimatedCompletion.toLocaleTimeString()} (${Math.round(estimatedRemainingMs/60000)} minutes remaining)`);
      }
    } catch (batchError: any) {
      console.error(`Error processing batch ${batchNumber}:`, batchError);
      stats.errors.push(`Batch ${batchNumber}: ${batchError.message || 'Unknown batch error'}`);
      
      // Update ETL run with error information
      await storage.updateETLRun(etlRunId, {
        errorMessage: `Error in batch ${batchNumber}: ${batchError.message || 'Unknown error'}, continuing to next batch...`
      });
    }
    
    // Add a shorter delay between batches - just enough to prevent overwhelming the API
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  // Generate detailed completion report
  const totalTime = ((Date.now() - stats.startTime.getTime()) / 60000).toFixed(1);
  const completionReport = `
  === Historical NAV Import Complete ===
  Total funds processed: ${processedCount}/${funds.length}
  Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)
  Total NAV entries imported: ${navEntriesImported}
  Date range: ${stats.oldestDate || 'N/A'} to ${stats.newestDate || 'N/A'}
  Time taken: ${totalTime} minutes
  Errors: ${stats.errors.length}
  `;
  
  console.log(completionReport);
  
  // Mark the ETL run as completed with detailed statistics
  await storage.updateETLRun(etlRunId, {
    status: 'COMPLETED',
    endTime: new Date(),
    recordsProcessed: processedCount,
    errorMessage: `Import complete: ${successCount} funds successful, ${navEntriesImported} NAV entries imported, ${stats.errors.length} errors`
  });
}

/**
 * Process authentic historical NAV data for a single fund
 * Returns detailed statistics about the import process
 */
async function processAuthenticHistoricalFund(fund: any): Promise<{
  success: boolean;
  message: string;
  navCount: number;
  oldestDate?: string;
  newestDate?: string;
}> {
  try {
    // We'll process 24 months of data (2 years) to speed up the import and focus on more recent data
    const months = 24;
    
    // Generate dates for the last X months
    const dates = generateMonthsBack(months);
    
    // Process in smaller batches with better error handling
    const batchSize = 3; // 3 months at a time for better throughput
    let totalNavEntriesInserted = 0;
    let oldestDate: string | undefined;
    let newestDate: string | undefined;
    
    // Track which months were successfully processed
    const processedMonths: string[] = [];
    const failedMonths: string[] = [];
    
    for (let i = 0; i < dates.length; i += batchSize) {
      const dateBatch = dates.slice(i, i + batchSize);
      const allNavEntries: { fundId: number; navDate: string; navValue: number }[] = [];
      
      for (const { year, month } of dateBatch) {
        const monthLabel = `${year}-${month.toString().padStart(2, '0')}`;
        
        try {
          // Fetch real NAV data from AMFI for this month/year with timeout protection
          let navData;
          try {
            navData = await Promise.race([
              fetchAuthenticHistoricalNav(fund.scheme_code, year, month),
              new Promise<never>((_, reject) => 
                setTimeout(() => reject(new Error(`Timeout fetching NAV for ${monthLabel}`)), 25000)
              )
            ]);
          } catch (timeoutError) {
            console.error(`Timeout fetching NAV data for scheme ${fund.scheme_code} (${monthLabel})`);
            failedMonths.push(monthLabel);
            continue; // Skip this month and move to the next
          }
          
          if (navData && navData.length > 0) {
            processedMonths.push(monthLabel);
            
            // Add all the NAV entries for this month
            for (const entry of navData) {
              // Track the date range for reporting
              if (!oldestDate || entry.date < oldestDate) {
                oldestDate = entry.date;
              }
              
              if (!newestDate || entry.date > newestDate) {
                newestDate = entry.date;
              }
              
              allNavEntries.push({
                fundId: fund.id,
                navDate: entry.date,
                navValue: parseFloat(entry.nav)
              });
            }
          } else {
            // No data found for this month
            failedMonths.push(monthLabel);
          }
        } catch (error) {
          console.error(`Error fetching authentic NAV data for fund ${fund.scheme_code} (${monthLabel}):`, error);
          failedMonths.push(monthLabel);
        }
        
        // Small delay between months to avoid overwhelming the AMFI API
        await new Promise(resolve => setTimeout(resolve, 500));
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
        } catch (insertError) {
          console.error(`Error inserting NAV data batch for fund ${fund.id}:`, insertError);
          
          // If batch insert fails, try individual inserts as fallback
          try {
            console.log(`Attempting individual inserts as fallback...`);
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
      
      // Short delay between batches to prevent overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Generate a detailed status message
    let statusMessage = '';
    if (totalNavEntriesInserted > 0) {
      statusMessage = `Successfully imported ${totalNavEntriesInserted} NAV entries (${processedMonths.length}/${processedMonths.length + failedMonths.length} months)`;
      if (failedMonths.length > 0) {
        statusMessage += ` - Failed months: ${failedMonths.slice(0, 3).join(', ')}${failedMonths.length > 3 ? '...' : ''}`;
      }
    } else if (processedMonths.length > 0 && totalNavEntriesInserted === 0) {
      statusMessage = `Processed ${processedMonths.length} months but no NAV entries were found`;
    } else {
      statusMessage = `No authentic NAV data available for this fund after trying ${dates.length} months`;
    }
    
    // Return detailed statistics about the processing
    return {
      success: totalNavEntriesInserted > 0,
      message: statusMessage,
      navCount: totalNavEntriesInserted,
      oldestDate,
      newestDate
    };
  } catch (error: any) {
    console.error(`Error processing authentic historical data for fund ${fund.id}:`, error);
    return {
      success: false,
      message: `Error: ${error.message || 'Unknown error'}`,
      navCount: 0
    };
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