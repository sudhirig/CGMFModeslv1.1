/**
 * Direct script to trigger historical NAV import with improved implementation
 * This bypasses any issues with the background processing
 */
const { pool } = require('./server/db');

async function startHistoricalNavImport() {
  console.log("=== Starting Enhanced Historical NAV Import ===");
  try {
    // First, cancel any running imports
    await pool.query(`
      UPDATE etl_pipeline_runs
      SET status = 'FAILED',
          end_time = NOW(),
          error_message = 'Process canceled to start a new enhanced import'
      WHERE status = 'RUNNING'
      AND pipeline_name = 'authentic_historical_import'
    `);
    
    console.log("✓ Canceled any existing historical NAV import processes");
    
    // Create a new ETL run record
    const etlResult = await pool.query(`
      INSERT INTO etl_pipeline_runs 
      (pipeline_name, status, start_time, records_processed, error_message)
      VALUES 
      ('authentic_historical_import', 'RUNNING', NOW(), 0, 'Starting enhanced historical NAV import process')
      RETURNING id
    `);
    
    const etlRunId = etlResult.rows[0].id;
    console.log(`✓ Created new ETL run with ID: ${etlRunId}`);
    
    // Get funds that need historical data - focus on high priority funds
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
      LIMIT 100
    `);
    
    const funds = fundsResult.rows;
    console.log(`✓ Found ${funds.length} funds that need historical NAV data`);
    
    if (funds.length === 0) {
      await pool.query(`
        UPDATE etl_pipeline_runs
        SET status = 'COMPLETED',
            end_time = NOW(),
            error_message = 'No funds found that need historical NAV data'
        WHERE id = $1
      `, [etlRunId]);
      console.log("✓ No funds found that need historical NAV data");
      return;
    }
    
    // Process funds in batches
    const batchSize = 5;
    let processedCount = 0;
    let successCount = 0;
    let errorCount = 0;
    let navEntriesImported = 0;
    
    console.log(`=== Starting historical NAV import for ${funds.length} funds ===`);
    console.log(`Batch size: ${batchSize} funds per batch`);
    
    for (let i = 0; i < funds.length; i += batchSize) {
      const batch = funds.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      const totalBatches = Math.ceil(funds.length / batchSize);
      
      console.log(`\n--- Processing batch ${batchNumber}/${totalBatches} (${batch.length} funds) ---`);
      
      try {
        // Update ETL status at the start of each batch
        await pool.query(`
          UPDATE etl_pipeline_runs
          SET records_processed = $1,
              error_message = $2
          WHERE id = $3
        `, [
          processedCount,
          `Processing batch ${batchNumber}/${totalBatches}: ${processedCount} funds complete, ${successCount} successful, ${errorCount} errors`,
          etlRunId
        ]);
        
        // Process each fund in the batch
        for (const fund of batch) {
          try {
            console.log(`Processing fund: ${fund.fund_name} (ID: ${fund.id}, Scheme: ${fund.scheme_code})`);
            
            // Process this fund
            const result = await processFund(fund);
            
            if (result.success) {
              successCount++;
              navEntriesImported += result.navCount;
              console.log(`✓ Success: ${fund.fund_name} - Imported ${result.navCount} NAV entries`);
            } else {
              errorCount++;
              console.log(`✗ Failed: ${fund.fund_name} - ${result.message}`);
            }
          } catch (fundError) {
            errorCount++;
            console.error(`✗ Error processing fund ${fund.id} (${fund.fund_name}):`, fundError);
          }
        }
        
        // Update processed count after batch completes
        processedCount += batch.length;
        
        // Update ETL run with detailed progress
        await pool.query(`
          UPDATE etl_pipeline_runs
          SET records_processed = $1,
              error_message = $2
          WHERE id = $3
        `, [
          processedCount,
          `Batch ${batchNumber}/${totalBatches} complete: ${processedCount}/${funds.length} funds processed, ${navEntriesImported} NAV entries imported`,
          etlRunId
        ]);
        
        console.log(`\n--- Batch ${batchNumber}/${totalBatches} complete ---`);
        console.log(`Progress: ${processedCount}/${funds.length} funds (${Math.round(processedCount/funds.length*100)}%)`);
        console.log(`Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)`);
        console.log(`NAV entries: ${navEntriesImported} total`);
      } catch (batchError) {
        console.error(`Error processing batch ${batchNumber}:`, batchError);
      }
      
      // Add a short delay between batches
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    // Mark the ETL run as completed
    await pool.query(`
      UPDATE etl_pipeline_runs
      SET status = 'COMPLETED',
          end_time = NOW(),
          records_processed = $1,
          error_message = $2
      WHERE id = $3
    `, [
      processedCount,
      `Import complete: ${successCount} funds successful, ${navEntriesImported} NAV entries imported, ${errorCount} errors`,
      etlRunId
    ]);
    
    console.log(`\n=== Historical NAV Import Complete ===`);
    console.log(`Total funds processed: ${processedCount}/${funds.length}`);
    console.log(`Success rate: ${successCount}/${processedCount} (${Math.round(successCount/processedCount*100)}%)`);
    console.log(`Total NAV entries imported: ${navEntriesImported}`);
  } catch (error) {
    console.error("Error running historical NAV import:", error);
  } finally {
    console.log("=== Historical NAV Import Process Ended ===");
  }
}

// Function to process a single fund
async function processFund(fund) {
  try {
    // We'll process 24 months of data (2 years) to get good historical data
    const months = 24;
    
    // Generate dates for the last X months
    const dates = generateMonthsBack(months);
    
    // Process in smaller batches with better error handling
    const batchSize = 3; // 3 months at a time for better throughput
    let totalNavEntriesInserted = 0;
    let oldestDate = null;
    let newestDate = null;
    
    // Track which months were successfully processed
    const processedMonths = [];
    const failedMonths = [];
    
    for (let i = 0; i < dates.length; i += batchSize) {
      const dateBatch = dates.slice(i, i + batchSize);
      const allNavEntries = [];
      
      for (const { year, month } of dateBatch) {
        const monthLabel = `${year}-${month.toString().padStart(2, '0')}`;
        
        try {
          // Fetch real NAV data from AMFI for this month/year
          const navData = await fetchHistoricalNav(fund.scheme_code, year, month);
          
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
          console.error(`Error fetching NAV data for fund ${fund.scheme_code} (${monthLabel}):`, error);
          failedMonths.push(monthLabel);
        }
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
          
          await pool.query(query, values);
          totalNavEntriesInserted += allNavEntries.length;
        } catch (insertError) {
          console.error(`Error inserting NAV data batch for fund ${fund.id}:`, insertError);
          
          // If batch insert fails, try individual inserts as fallback
          try {
            for (const entry of allNavEntries) {
              try {
                await pool.query(`
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
    }
    
    // Generate a detailed status message
    let statusMessage = '';
    if (totalNavEntriesInserted > 0) {
      statusMessage = `Successfully imported ${totalNavEntriesInserted} NAV entries (${processedMonths.length}/${processedMonths.length + failedMonths.length} months)`;
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
  } catch (error) {
    console.error(`Error processing historical data for fund ${fund.id}:`, error);
    return {
      success: false,
      message: `Error: ${error.message || 'Unknown error'}`,
      navCount: 0
    };
  }
}

// Generate array of months going back from current date
function generateMonthsBack(monthsBack) {
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

// Fetch historical NAV data from AMFI
async function fetchHistoricalNav(schemeCode, year, month) {
  const axios = require('axios');
  const cheerio = require('cheerio');
  
  try {
    // Calculate the from and to dates for the specified month
    const fromDate = new Date(year, month - 1, 1);
    const toDate = new Date(year, month, 0); // Last day of the month
    
    // Format dates as required by AMFI API (dd-MMM-yyyy)
    const formatDate = (date) => {
      const day = date.getDate().toString().padStart(2, '0');
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const monthStr = monthNames[date.getMonth()];
      const yearStr = date.getFullYear();
      return `${day}-${monthStr}-${yearStr}`;
    };
    
    const fromDateStr = formatDate(fromDate);
    const toDateStr = formatDate(toDate);
    
    // Implement retry mechanism with exponential backoff
    const maxRetries = 3;
    let retryCount = 0;
    let navEntries = [];
    
    while (retryCount < maxRetries && navEntries.length === 0) {
      try {
        // We'll try multiple AMFI endpoints
        const url = 'https://www.amfiindia.com/spages/NAVHistoryReport.aspx';
        
        // Make the POST request to get historical NAV data
        const response = await axios.post(url, 
          `SchemeCode=${schemeCode}&FromDate=${fromDateStr}&ToDate=${toDateStr}`,
          {
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            },
            timeout: 25000 // 25 second timeout
          }
        );
        
        if (response.status === 200) {
          const $ = cheerio.load(response.data);
          
          // Extract NAV data from the HTML table
          const navData = [];
          
          // AMFI website can return data in different table formats, try both common formats
          const tableSelector = '.mfnavcontent table, #gvNAVReport';
          
          $(tableSelector).find('tr').each((index, row) => {
            // Skip header row
            if (index === 0) return;
            
            const columns = $(row).find('td');
            if (columns.length >= 2) {
              const dateText = $(columns[0]).text().trim();
              const navText = $(columns[1]).text().trim();
              
              if (dateText && navText && navText !== 'N.A.' && !isNaN(parseFloat(navText))) {
                // Try to parse the date - can be in different formats
                let navDate;
                
                // Try dd-MM-yyyy format
                const dateParts = dateText.split('-');
                if (dateParts.length === 3) {
                  // Could be dd-MM-yyyy or dd-MMM-yyyy
                  const day = parseInt(dateParts[0]);
                  const monthPart = dateParts[1];
                  const year = parseInt(dateParts[2]);
                  
                  if (!isNaN(day) && !isNaN(year)) {
                    if (!isNaN(parseInt(monthPart))) {
                      // It's dd-MM-yyyy
                      const month = parseInt(monthPart) - 1;
                      navDate = new Date(year, month, day);
                    } else {
                      // It's dd-MMM-yyyy
                      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                      const month = monthNames.findIndex(m => m.toLowerCase() === monthPart.toLowerCase());
                      if (month !== -1) {
                        navDate = new Date(year, month, day);
                      }
                    }
                  }
                }
                
                if (navDate) {
                  const formattedDate = `${navDate.getFullYear()}-${(navDate.getMonth() + 1).toString().padStart(2, '0')}-${navDate.getDate().toString().padStart(2, '0')}`;
                  
                  navData.push({
                    date: formattedDate,
                    nav: navText
                  });
                }
              }
            }
          });
          
          if (navData.length > 0) {
            navEntries = navData;
            break;
          }
        }
      } catch (error) {
        console.error(`Attempt ${retryCount + 1} failed for scheme ${schemeCode} (${year}-${month}):`, error.message);
      }
      
      // Increment retry count and wait before retrying
      retryCount++;
      
      // Wait before retrying with exponential backoff
      if (retryCount < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, 2000 * Math.pow(2, retryCount)));
      }
    }
    
    return navEntries;
  } catch (error) {
    console.error(`Error fetching historical NAV for scheme ${schemeCode} (${year}-${month}):`, error);
    return [];
  }
}

// Run the script
startHistoricalNavImport().catch(console.error);