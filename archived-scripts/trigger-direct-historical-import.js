/**
 * Direct script to trigger historical NAV import with direct database access
 * This bypasses the API layer to directly import historical NAV data
 */
import { db, pool } from './server/db.js';
import axios from 'axios';
import * as cheerio from 'cheerio';

// Configuration
const BATCH_SIZE = 10; // Process 10 funds at a time (increased from 5)
const MONTHS_TO_IMPORT = 12; // Focus on 12 months for initial import
const REQUEST_TIMEOUT = 30000; // 30 second timeout for API requests
const DELAY_BETWEEN_REQUESTS = 1000; // 1 second delay between requests

async function startDirectHistoricalImport() {
  console.log("=== Starting Direct Historical NAV Import ===");
  
  try {
    // First, update the existing ETL run to show we're taking over
    await pool.query(`
      UPDATE etl_pipeline_runs
      SET status = 'RUNNING',
          error_message = 'Direct historical NAV import in progress with enhanced implementation',
          records_processed = 0
      WHERE id = 457
    `);
    
    // Find funds that need historical data, prioritizing Q1 and Q2 funds
    const fundsResult = await pool.query(`
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
      LIMIT 100  -- Process 100 high priority funds first
    `);
    
    const fundsToProcess = fundsResult.rows;
    console.log(`Found ${fundsToProcess.length} funds that need historical NAV data`);
    
    // Process funds in batches
    let processedCount = 0;
    let successCount = 0;
    let errorCount = 0;
    let totalNavEntriesImported = 0;
    
    const startTime = new Date();
    
    for (let i = 0; i < fundsToProcess.length; i += BATCH_SIZE) {
      const batch = fundsToProcess.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(fundsToProcess.length / BATCH_SIZE);
      
      console.log(`\n--- Processing batch ${batchNumber}/${totalBatches} (${batch.length} funds) ---`);
      
      // Update ETL run status
      await pool.query(`
        UPDATE etl_pipeline_runs
        SET error_message = $1,
            records_processed = $2
        WHERE id = 457
      `, [
        `Processing batch ${batchNumber}/${totalBatches}: ${processedCount} funds complete, ${successCount} successful, ${errorCount} errors`,
        processedCount
      ]);
      
      // Process each fund in the batch in parallel
      const batchResults = await Promise.all(batch.map(fund => processFundHistoricalData(fund)));
      
      // Update statistics
      for (const result of batchResults) {
        processedCount++;
        
        if (result.success) {
          successCount++;
          totalNavEntriesImported += result.navCount;
        } else {
          errorCount++;
        }
      }
      
      // Log batch summary
      const elapsedMinutes = ((Date.now() - startTime.getTime()) / 60000).toFixed(1);
      console.log(`\n--- Batch ${batchNumber}/${totalBatches} complete ---`);
      console.log(`Progress: ${processedCount}/${fundsToProcess.length} funds (${Math.round(processedCount/fundsToProcess.length*100)}%)`);
      console.log(`Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)`);
      console.log(`NAV entries: ${totalNavEntriesImported} total`);
      console.log(`Time elapsed: ${elapsedMinutes} minutes`);
    }
    
    // Complete the ETL run
    const totalTime = ((Date.now() - startTime.getTime()) / 60000).toFixed(1);
    await pool.query(`
      UPDATE etl_pipeline_runs
      SET status = 'COMPLETED',
          end_time = NOW(),
          records_processed = $1,
          error_message = $2
      WHERE id = 457
    `, [
      processedCount,
      `Import complete: ${successCount}/${processedCount} funds successful, ${totalNavEntriesImported} NAV entries imported in ${totalTime} minutes`
    ]);
    
    console.log(`\n=== Historical NAV Import Complete ===`);
    console.log(`Total funds processed: ${processedCount}/${fundsToProcess.length}`);
    console.log(`Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)`);
    console.log(`Total NAV entries imported: ${totalNavEntriesImported}`);
    console.log(`Time taken: ${totalTime} minutes`);
    
  } catch (error) {
    console.error("Error in direct historical import:", error);
    
    // Update ETL run with error
    await pool.query(`
      UPDATE etl_pipeline_runs
      SET status = 'FAILED',
          end_time = NOW(),
          error_message = $1
      WHERE id = 457
    `, [`Error in direct historical import: ${error.message || 'Unknown error'}`]);
  }
}

async function processFundHistoricalData(fund) {
  const fundStart = Date.now();
  console.log(`Processing fund: ${fund.fund_name} (ID: ${fund.id}, Scheme: ${fund.scheme_code})`);
  
  try {
    // Generate dates for the last X months
    const dates = generateMonthsBack(MONTHS_TO_IMPORT);
    
    // Track statistics
    let navCount = 0;
    const processedMonths = [];
    const failedMonths = [];
    
    // Process each month
    for (const { year, month } of dates) {
      const monthLabel = `${year}-${month.toString().padStart(2, '0')}`;
      
      try {
        // Fetch historical NAV data for this month with timeout protection
        const navData = await Promise.race([
          fetchHistoricalNav(fund.scheme_code, year, month),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error(`Timeout fetching NAV for ${monthLabel}`)), REQUEST_TIMEOUT)
          )
        ]);
        
        if (navData && navData.length > 0) {
          processedMonths.push(monthLabel);
          
          // Prepare the NAV entries for this month
          const navEntries = navData.map(entry => ({
            fundId: fund.id,
            navDate: entry.date,
            navValue: parseFloat(entry.nav)
          })).filter(entry => !isNaN(entry.navValue) && entry.navValue > 0);
          
          // Insert the NAV entries into the database using UPSERT to avoid duplicates
          if (navEntries.length > 0) {
            // Use a single transaction for the batch insert
            const client = await pool.connect();
            try {
              await client.query('BEGIN');
              
              for (const entry of navEntries) {
                await client.query(`
                  INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
                  VALUES ($1, $2, $3, NOW())
                  ON CONFLICT (fund_id, nav_date) DO UPDATE
                  SET nav_value = EXCLUDED.nav_value
                `, [entry.fundId, entry.navDate, entry.navValue]);
                
                navCount++;
              }
              
              await client.query('COMMIT');
            } catch (txError) {
              await client.query('ROLLBACK');
              throw txError;
            } finally {
              client.release();
            }
          }
        } else {
          failedMonths.push(`${monthLabel} (no data)`);
        }
      } catch (monthError) {
        failedMonths.push(`${monthLabel} (${monthError.message || 'error'})`);
        console.error(`Error processing ${monthLabel} for fund ${fund.id}:`, monthError.message);
      }
      
      // Add a small delay between requests to avoid overwhelming the AMFI server
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_REQUESTS));
    }
    
    const processingTime = ((Date.now() - fundStart) / 1000).toFixed(1);
    
    if (navCount > 0) {
      console.log(`✓ Success: ${fund.fund_name} - Imported ${navCount} NAV entries in ${processingTime}s`);
      return { success: true, navCount, fundId: fund.id, fundName: fund.fund_name };
    } else {
      console.log(`✗ No data: ${fund.fund_name} - No NAV entries found in ${processingTime}s`);
      return { success: false, navCount: 0, fundId: fund.id, fundName: fund.fund_name, message: 'No NAV data found' };
    }
  } catch (fundError) {
    const errorMessage = fundError.message || 'Unknown error';
    console.error(`✗ Error processing fund ${fund.id} (${fund.fund_name}):`, errorMessage);
    return { success: false, navCount: 0, fundId: fund.id, fundName: fund.fund_name, message: errorMessage };
  }
}

// Helper function to generate array of months going back
function generateMonthsBack(monthsBack) {
  const result = [];
  const now = new Date();
  let currentMonth = now.getMonth() + 1; // JavaScript months are 0-based
  let currentYear = now.getFullYear();
  
  for (let i = 0; i < monthsBack; i++) {
    result.push({ year: currentYear, month: currentMonth });
    
    currentMonth--;
    if (currentMonth === 0) {
      currentMonth = 12;
      currentYear--;
    }
  }
  
  return result;
}

// Function to fetch historical NAV data from AMFI
async function fetchHistoricalNav(schemeCode, year, month) {
  // Format the date range for the AMFI historical NAV API
  const formatDate = (date) => {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const year = date.getFullYear();
    return `${day}-${month}-${year}`;
  };
  
  // Calculate the first and last day of the month
  const fromDate = new Date(year, month - 1, 1);
  let toDate;
  if (month === 12) {
    toDate = new Date(year + 1, 0, 0);
  } else {
    toDate = new Date(year, month, 0);
  }
  
  const fromDateStr = formatDate(fromDate);
  const toDateStr = formatDate(toDate);
  
  try {
    // Make the request to AMFI with retries
    let retries = 0;
    const MAX_RETRIES = 3;
    
    while (retries < MAX_RETRIES) {
      try {
        const response = await axios.get('https://www.amfiindia.com/spages/NAVHistoryReport.aspx', {
          params: {
            sch: schemeCode,
            from_date: fromDateStr,
            to_date: toDateStr
          },
          timeout: REQUEST_TIMEOUT - 5000 // Slightly shorter than our overall timeout
        });
        
        // Parse the HTML response to extract NAV data
        const $ = cheerio.load(response.data);
        const navData = [];
        
        // Find the table with NAV data - it's typically the first or second table
        $('table').each((i, table) => {
          // Check if this looks like the right table by examining headers
          const headers = $(table).find('tr:first-child th, tr:first-child td').map((_, el) => $(el).text().trim()).get();
          
          if (headers.some(header => header.includes('Date') || header.includes('NAV'))) {
            // This is likely the NAV data table
            $(table).find('tr:not(:first-child)').each((_, row) => {
              const cells = $(row).find('td').map((_, cell) => $(cell).text().trim()).get();
              
              if (cells.length >= 2) {
                const navDate = cells[0];
                const navValue = cells[1];
                
                // Skip entries with invalid data
                if (navDate && navValue && !isNaN(parseFloat(navValue)) && parseFloat(navValue) > 0) {
                  // Convert date from DD-MM-YYYY to YYYY-MM-DD for database
                  const [day, month, year] = navDate.split('-');
                  const formattedDate = `${year}-${month}-${day}`;
                  
                  navData.push({
                    date: formattedDate,
                    nav: navValue
                  });
                }
              }
            });
          }
        });
        
        return navData;
      } catch (error) {
        retries++;
        
        if (retries >= MAX_RETRIES) {
          throw error;
        }
        
        // Exponential backoff before retry
        const backoffMs = Math.pow(2, retries) * 1000;
        await new Promise(resolve => setTimeout(resolve, backoffMs));
      }
    }
  } catch (error) {
    console.error(`Error fetching historical NAV for scheme ${schemeCode} (${year}-${month}):`, error.message);
    throw error;
  }
}

// Start the import process
startDirectHistoricalImport().catch(error => {
  console.error("Unhandled error in direct historical import:", error);
});