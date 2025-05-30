import express from 'express';
import axios from 'axios';
import { storage } from '../storage';
import { executeRawQuery } from '../db';

const router = express.Router();

// Configuration
const BATCH_SIZE = 100; // Increased batch size
const REQUEST_DELAY = 500; // Reduced to 0.5 seconds between requests
const MAX_RETRIES = 2; // Reduced retries for faster processing
const TIMEOUT = 15000; // Reduced timeout to 15 seconds
const PARALLEL_REQUESTS = 3; // Process multiple funds simultaneously

interface MFAPIScheme {
  schemeCode: number;
  schemeName: string;
}

interface MFAPIHistoricalData {
  meta: {
    scheme_name: string;
    scheme_code: string;
    scheme_category: string;
    fund_house: string;
  };
  data: Array<{
    date: string; // DD-MM-YYYY format
    nav: string;
  }>;
}

/**
 * Start the historical NAV import from MFAPI.in
 */
router.post('/start', async (req, res) => {
  try {
    // Create ETL run to track progress
    const etlRun = await storage.createETLRun({
      pipelineName: 'mfapi_historical_import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Starting MFAPI.in historical NAV import process'
    });

    // Start the import process in background
    processHistoricalImport(etlRun.id);

    res.json({
      success: true,
      message: 'MFAPI.in historical NAV import started',
      etlRunId: etlRun.id
    });

  } catch (error: any) {
    console.error('Error starting MFAPI historical import:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to start import: ' + error.message
    });
  }
});

/**
 * Get the status of the historical import
 */
router.get('/status', async (req, res) => {
  try {
    const latestRun = await executeRawQuery(`
      SELECT * FROM etl_pipeline_runs 
      WHERE pipeline_name = 'mfapi_historical_import'
      ORDER BY id DESC LIMIT 1
    `);

    if (latestRun.rows.length === 0) {
      return res.json({
        success: true,
        status: 'No import runs found'
      });
    }

    res.json({
      success: true,
      etlRun: latestRun.rows[0]
    });

  } catch (error: any) {
    console.error('Error getting import status:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get status: ' + error.message
    });
  }
});

/**
 * Process the historical import in the background
 */
async function processHistoricalImport(etlRunId: number) {
  const startTime = new Date();
  let processedCount = 0;
  let successCount = 0;
  let errorCount = 0;
  let totalNavRecords = 0;

  try {
    console.log('=== Starting MFAPI.in Historical NAV Import ===');

    // Step 1: Get funds that need historical data, prioritizing by category
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.scheme_code, f.fund_name, f.category, COUNT(n.nav_date) as nav_count
      FROM funds f
      LEFT JOIN nav_data n ON f.id = n.fund_id
      GROUP BY f.id, f.scheme_code, f.fund_name, f.category
      HAVING COUNT(n.nav_date) <= 2
      ORDER BY 
        CASE f.category 
          WHEN 'Equity' THEN 1 
          WHEN 'Hybrid' THEN 2 
          WHEN 'Debt' THEN 3 
          ELSE 4 
        END,
        f.id
      LIMIT 3000  -- Increased to 3000 funds
    `);

    const fundsToProcess = fundsResult.rows;
    console.log(`Found ${fundsToProcess.length} funds that need historical NAV data`);

    await storage.updateETLRun(etlRunId, {
      errorMessage: `Processing ${fundsToProcess.length} funds for historical NAV data import`
    });

    // Step 2: Process funds in batches
    for (let i = 0; i < fundsToProcess.length; i += BATCH_SIZE) {
      const batch = fundsToProcess.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(fundsToProcess.length / BATCH_SIZE);

      console.log(`\n--- Processing batch ${batchNumber}/${totalBatches} (${batch.length} funds) ---`);

      // Update ETL status
      await storage.updateETLRun(etlRunId, {
        recordsProcessed: processedCount,
        errorMessage: `Processing batch ${batchNumber}/${totalBatches}: ${processedCount} funds complete, ${successCount} successful, ${errorCount} errors`
      });

      // Process funds in parallel within each batch
      const fundPromises = [];
      for (let j = 0; j < batch.length; j += PARALLEL_REQUESTS) {
        const parallelBatch = batch.slice(j, j + PARALLEL_REQUESTS);
        
        const batchPromises = parallelBatch.map(async (fund, index) => {
          try {
            // Stagger requests slightly to avoid overwhelming the API
            await new Promise(resolve => setTimeout(resolve, index * 200));
            
            const result = await processSingleFund(fund);
            processedCount++;

            if (result.success) {
              successCount++;
              totalNavRecords += result.navCount;
              console.log(`✓ ${fund.fund_name}: ${result.navCount} NAV records imported`);
            } else {
              errorCount++;
              console.log(`✗ ${fund.fund_name}: ${result.message}`);
            }

            return result;
          } catch (fundError: any) {
            errorCount++;
            processedCount++;
            console.error(`✗ Error processing fund ${fund.id}:`, fundError.message);
            return { success: false, navCount: 0, message: fundError.message };
          }
        });

        fundPromises.push(...batchPromises);

        // Wait for this parallel batch to complete before starting the next
        await Promise.allSettled(batchPromises);
        
        // Brief delay between parallel batches
        await new Promise(resolve => setTimeout(resolve, REQUEST_DELAY));
      }

      // Log batch progress
      const elapsedMinutes = ((Date.now() - startTime.getTime()) / 60000).toFixed(1);
      console.log(`Batch ${batchNumber} complete: ${successCount}/${processedCount} successful, ${totalNavRecords} NAV records imported, ${elapsedMinutes}min elapsed`);
    }

    // Complete the ETL run
    const totalTime = ((Date.now() - startTime.getTime()) / 60000).toFixed(1);
    await storage.updateETLRun(etlRunId, {
      status: 'COMPLETED',
      endTime: new Date(),
      recordsProcessed: processedCount,
      errorMessage: `Import complete: ${successCount}/${processedCount} funds successful, ${totalNavRecords} NAV records imported in ${totalTime} minutes`
    });

    console.log(`\n=== MFAPI.in Import Complete ===`);
    console.log(`Processed: ${processedCount} funds`);
    console.log(`Successful: ${successCount} funds`);
    console.log(`NAV Records: ${totalNavRecords} imported`);
    console.log(`Time: ${totalTime} minutes`);

  } catch (error: any) {
    console.error('Error in historical import process:', error);
    await storage.updateETLRun(etlRunId, {
      status: 'FAILED',
      endTime: new Date(),
      errorMessage: `Import failed: ${error.message}`
    });
  }
}

/**
 * Process a single fund to import its historical NAV data
 */
async function processSingleFund(fund: any): Promise<{ success: boolean; navCount: number; message: string }> {
  try {
    // Step 1: Try to find this fund in MFAPI.in by scheme code
    let mfapiData: MFAPIHistoricalData | null = null;

    if (fund.scheme_code) {
      try {
        mfapiData = await fetchMFAPIData(fund.scheme_code);
      } catch (error) {
        // If scheme code doesn't work, try name search
        console.log(`Scheme code ${fund.scheme_code} not found, trying name search...`);
      }
    }

    // Step 2: If scheme code failed, try name-based search
    if (!mfapiData) {
      const searchResults = await searchMFAPIByName(fund.fund_name);
      if (searchResults.length > 0) {
        // Try the first matching result
        mfapiData = await fetchMFAPIData(searchResults[0].schemeCode);
      }
    }

    if (!mfapiData || !mfapiData.data || mfapiData.data.length === 0) {
      return {
        success: false,
        navCount: 0,
        message: 'No historical data found in MFAPI.in'
      };
    }

    // Step 3: Import the NAV data
    const navCount = await importNavData(fund.id, mfapiData.data);

    return {
      success: true,
      navCount,
      message: `Imported ${navCount} NAV records`
    };

  } catch (error: any) {
    return {
      success: false,
      navCount: 0,
      message: error.message || 'Unknown error'
    };
  }
}

/**
 * Fetch historical data from MFAPI.in for a specific scheme code
 */
async function fetchMFAPIData(schemeCode: string | number): Promise<MFAPIHistoricalData> {
  let retries = 0;
  while (retries < MAX_RETRIES) {
    try {
      const response = await axios.get(`https://api.mfapi.in/mf/${schemeCode}`, {
        timeout: TIMEOUT
      });
      return response.data;
    } catch (error: any) {
      retries++;
      if (retries >= MAX_RETRIES) {
        throw new Error(`Failed to fetch data after ${MAX_RETRIES} retries: ${error.message}`);
      }
      // Exponential backoff
      await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, retries)));
    }
  }
  throw new Error('Max retries exceeded');
}

/**
 * Search for funds by name in MFAPI.in
 */
async function searchMFAPIByName(fundName: string): Promise<MFAPIScheme[]> {
  try {
    // Extract key words from fund name for better matching
    const searchTerm = fundName.split(' ').slice(0, 2).join(' '); // Use first 2 words
    const response = await axios.get(`https://api.mfapi.in/mf/search?q=${encodeURIComponent(searchTerm)}`, {
      timeout: TIMEOUT
    });
    return response.data || [];
  } catch (error: any) {
    console.error(`Error searching for fund "${fundName}":`, error.message);
    return [];
  }
}

/**
 * Import NAV data into our database using bulk insert for better performance
 */
async function importNavData(fundId: number, navData: Array<{ date: string; nav: string }>): Promise<number> {
  if (!navData || navData.length === 0) return 0;

  const validEntries = [];
  
  for (const entry of navData) {
    try {
      // Convert date from DD-MM-YYYY to YYYY-MM-DD
      const [day, month, year] = entry.date.split('-');
      const navDate = `${year}-${month}-${day}`;
      const navValue = parseFloat(entry.nav);

      if (isNaN(navValue) || navValue <= 0) {
        continue; // Skip invalid NAV values
      }

      validEntries.push({ fundId, navDate, navValue });
    } catch (error: any) {
      console.error(`Error parsing NAV data for fund ${fundId} on ${entry.date}:`, error.message);
    }
  }

  if (validEntries.length === 0) return 0;

  try {
    // Bulk insert with conflict resolution
    const values = validEntries.map((entry, index) => 
      `($${index * 3 + 1}, $${index * 3 + 2}, $${index * 3 + 3}, NOW())`
    ).join(', ');
    
    const params = validEntries.flatMap(entry => [entry.fundId, entry.navDate, entry.navValue]);
    
    await executeRawQuery(`
      INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
      VALUES ${values}
      ON CONFLICT (fund_id, nav_date) DO UPDATE
      SET nav_value = EXCLUDED.nav_value
    `, params);

    return validEntries.length;
  } catch (error: any) {
    console.error(`Error bulk importing NAV data for fund ${fundId}:`, error.message);
    // Fallback to individual inserts if bulk fails
    return await importNavDataFallback(fundId, validEntries);
  }
}

/**
 * Fallback method for individual NAV data inserts
 */
async function importNavDataFallback(fundId: number, validEntries: Array<{ fundId: number; navDate: string; navValue: number }>): Promise<number> {
  let importCount = 0;
  
  for (const entry of validEntries) {
    try {
      await executeRawQuery(`
        INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (fund_id, nav_date) DO UPDATE
        SET nav_value = EXCLUDED.nav_value
      `, [entry.fundId, entry.navDate, entry.navValue]);
      
      importCount++;
    } catch (error: any) {
      console.error(`Error importing single NAV record for fund ${fundId}:`, error.message);
    }
  }
  
  return importCount;
}

export default router;