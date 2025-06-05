/**
 * Faster Historical NAV Import Script
 * This script uses more aggressive parallel processing to speed up the import
 */
import pg from 'pg';
import axios from 'axios';
import * as cheerio from 'cheerio';

const { Pool } = pg;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Configuration for faster processing
const BATCH_SIZE = 5; // Process 5 funds at a time
const MONTHS_TO_IMPORT = 3; // Focus on most recent 3 months for initial import
const CONCURRENT_REQUESTS = 3; // Make 3 concurrent requests per fund
const TOTAL_FUNDS = 20; // Process 20 high-priority funds

async function startFasterHistoricalImport() {
  console.log("=== Starting Faster Historical NAV Import ===");
  
  try {
    // Create a new ETL run for this process
    const etlRunResult = await pool.query(`
      INSERT INTO etl_pipeline_runs 
      (pipeline_name, status, start_time, records_processed, error_message, created_at)
      VALUES ('faster_historical_import', 'RUNNING', NOW(), 0, 'Starting faster historical NAV import', NOW())
      RETURNING id
    `);
    
    const etlRunId = etlRunResult.rows[0].id;
    console.log(`Created ETL Run ID: ${etlRunId}`);
    
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
      LIMIT ${TOTAL_FUNDS}
    `);
    
    const fundsToProcess = fundsResult.rows;
    console.log(`Found ${fundsToProcess.length} funds for faster historical NAV import`);
    
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
        WHERE id = $3
      `, [
        `Processing batch ${batchNumber}/${totalBatches}: ${processedCount} funds complete, ${successCount} successful, ${errorCount} errors`,
        processedCount,
        etlRunId
      ]);
      
      // Process all funds in batch concurrently
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
      WHERE id = $3
    `, [
      processedCount,
      `Import complete: ${successCount}/${processedCount} funds successful, ${totalNavEntriesImported} NAV entries imported in ${totalTime} minutes`,
      etlRunId
    ]);
    
    console.log(`\n=== Faster Historical NAV Import Complete ===`);
    console.log(`Total funds processed: ${processedCount}/${fundsToProcess.length}`);
    console.log(`Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)`);
    console.log(`Total NAV entries imported: ${totalNavEntriesImported}`);
    console.log(`Time taken: ${totalTime} minutes`);
    
  } catch (error) {
    console.error("Error in faster historical import:", error);
  } finally {
    // Always end by closing the pool
    await pool.end();
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
    
    // Process months in parallel with concurrency limit
    const monthChunks = [];
    for (let i = 0; i < dates.length; i += CONCURRENT_REQUESTS) {
      monthChunks.push(dates.slice(i, i + CONCURRENT_REQUESTS));
    }
    
    for (const monthChunk of monthChunks) {
      // Process each month in the chunk concurrently
      const monthResults = await Promise.all(monthChunk.map(async ({ year, month }) => {
        try {
          // Fetch historical NAV data for this month
          const navData = await fetchHistoricalNav(fund.scheme_code, year, month);
          
          if (navData && navData.length > 0) {
            return {
              success: true,
              monthLabel: `${year}-${month.toString().padStart(2, '0')}`,
              navEntries: navData.map(entry => ({
                fundId: fund.id,
                navDate: entry.date,
                navValue: parseFloat(entry.nav)
              })).filter(entry => !isNaN(entry.navValue) && entry.navValue > 0)
            };
          } else {
            return { success: false, monthLabel: `${year}-${month.toString().padStart(2, '0')}` };
          }
        } catch (error) {
          return { success: false, monthLabel: `${year}-${month.toString().padStart(2, '0')}`, error };
        }
      }));
      
      // Insert all NAV entries from successful months
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        
        for (const monthResult of monthResults) {
          if (monthResult.success && monthResult.navEntries && monthResult.navEntries.length > 0) {
            for (const entry of monthResult.navEntries) {
              await client.query(`
                INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (fund_id, nav_date) DO UPDATE
                SET nav_value = EXCLUDED.nav_value
              `, [entry.fundId, entry.navDate, entry.navValue]);
              
              navCount++;
            }
          }
        }
        
        await client.query('COMMIT');
      } catch (txError) {
        await client.query('ROLLBACK');
        throw txError;
      } finally {
        client.release();
      }
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
    const MAX_RETRIES = 2;
    
    while (retries < MAX_RETRIES) {
      try {
        const response = await axios.get('https://www.amfiindia.com/spages/NAVHistoryReport.aspx', {
          params: {
            sch: schemeCode,
            from_date: fromDateStr,
            to_date: toDateStr
          },
          timeout: 20000 // 20 second timeout for faster processing
        });
        
        // Parse the HTML response to extract NAV data
        const $ = cheerio.load(response.data);
        const navData = [];
        
        // Find the table with NAV data
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
        
        // Shorter backoff for faster processing
        const backoffMs = 1000 * retries;
        await new Promise(resolve => setTimeout(resolve, backoffMs));
      }
    }
  } catch (error) {
    console.error(`Error fetching historical NAV for scheme ${schemeCode} (${year}-${month}):`, error.message);
    throw error;
  }
}

// Start the import process
startFasterHistoricalImport().catch(error => {
  console.error("Unhandled error in faster historical import:", error);
});