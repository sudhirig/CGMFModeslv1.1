import express from 'express';
import axios from 'axios';
import { storage } from '../storage';
import { executeRawQuery } from '../db';

const router = express.Router();

// AMFI URL for latest NAV data
const AMFI_NAV_ALL_URL = 'https://www.amfiindia.com/spages/NAVAll.txt';

/**
 * Start daily NAV update with real AMFI data
 */
/**
 * Cancel any running daily NAV updates and start a fresh one
 */
router.post('/restart', async (req, res) => {
  try {
    // Check if any daily import is running
    const runningImports = await executeRawQuery(`
      SELECT id FROM etl_pipeline_runs 
      WHERE pipeline_name = 'real_daily_nav_update'
      AND status = 'RUNNING'
    `);
    
    // Mark any running imports as cancelled
    if (runningImports.rows.length > 0) {
      const importId = runningImports.rows[0].id;
      await storage.updateETLRun(importId, {
        status: 'CANCELLED',
        endTime: new Date(),
        errorMessage: 'Cancelled to start a fresh import with improved implementation'
      });
      console.log(`Cancelled daily NAV update with ID ${importId}`);
    }
    
    // Create a new ETL run
    const etlRun = await storage.createETLRun({
      pipelineName: 'real_daily_nav_update',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Starting fresh daily NAV update with real AMFI data (improved implementation)'
    });
    
    // Start the import process in the background
    processRealDailyUpdate(etlRun.id);
    
    res.status(200).json({
      success: true,
      message: 'Started fresh daily NAV update with improved implementation',
      etlRunId: etlRun.id
    });
  } catch (error: any) {
    console.error('Error restarting daily NAV update:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to restart daily NAV update: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

router.post('/start', async (req, res) => {
  try {
    // First, check if a daily import is already running
    const runningImports = await executeRawQuery(`
      SELECT id FROM etl_pipeline_runs 
      WHERE pipeline_name = 'real_daily_nav_update'
      AND status = 'RUNNING'
    `);
    
    if (runningImports.rows.length > 0) {
      return res.status(200).json({
        success: false,
        message: 'A daily NAV update is already in progress',
        runId: runningImports.rows[0].id
      });
    }
    
    // Create a new ETL run
    const etlRun = await storage.createETLRun({
      pipelineName: 'real_daily_nav_update',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Starting daily NAV update with real AMFI data'
    });
    
    // Start the import process in the background
    processRealDailyUpdate(etlRun.id);
    
    res.status(200).json({
      success: true,
      message: 'Started daily NAV update with real AMFI data',
      etlRunId: etlRun.id
    });
  } catch (error: any) {
    console.error('Error starting daily NAV update:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to start daily NAV update: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

/**
 * Process the daily NAV update in the background
 */
async function processRealDailyUpdate(etlRunId: number): Promise<void> {
  try {
    console.log('Starting real daily NAV update from AMFI...');
    
    // Update ETL record to show we're fetching data
    await storage.updateETLRun(etlRunId, {
      recordsProcessed: 0,
      errorMessage: 'Fetching latest NAV data from AMFI...'
    });
    
    // Fetch the latest NAV data from AMFI with increased timeout and retries
    let response;
    let retryCount = 0;
    const maxRetries = 3;
    
    while (retryCount < maxRetries) {
      try {
        response = await axios.get(AMFI_NAV_ALL_URL, {
          timeout: 60000, // 60 second timeout
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
          }
        });
        
        // If we got data, break out of retry loop
        if (response && response.data) {
          break;
        }
      } catch (error) {
        console.error(`AMFI fetch attempt ${retryCount + 1} failed:`, error);
        retryCount++;
        
        // Update ETL status with retry information
        await storage.updateETLRun(etlRunId, {
          errorMessage: `Retry ${retryCount}/${maxRetries}: Fetching latest NAV data from AMFI...`
        });
        
        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 5000 * Math.pow(2, retryCount)));
      }
    }
    
    // Check if we have data after all retries
    if (!response || !response.data) {
      await storage.updateETLRun(etlRunId, {
        status: 'FAILED',
        endTime: new Date(),
        errorMessage: 'No data received from AMFI after multiple attempts'
      });
      return;
    }
    
    // Parse the NAV data
    const navText = response.data;
    const navEntries: any[] = [];
    
    // Split by lines
    const lines = navText.split('\n');
    
    let currentAMC = '';
    let currentSchemeType = '';
    let processedCount = 0;
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      if (line.length === 0) continue;
      
      // Check if line contains AMC name
      if (line.includes('Mutual Fund') && !line.includes(';')) {
        currentAMC = line;
        continue;
      }
      
      // Check if line contains scheme type
      if (line.includes('Schemes') && !line.includes(';')) {
        currentSchemeType = line;
        continue;
      }
      
      // Skip header lines
      if (line.startsWith('Scheme Code') || line.includes('ISIN Div Payout') || line.includes('ISIN Growth')) {
        continue;
      }
      
      // Parse fund data line (semicolon separated)
      if (line.includes(';')) {
        const parts = line.split(';');
        
        if (parts.length >= 5) {
          const schemeCode = parts[0].trim();
          const navValueStr = parts[4].trim().replace(/,/g, '');
          
          // Only process if we have valid NAV data
          if (schemeCode && navValueStr && !isNaN(parseFloat(navValueStr))) {
            // Store the scheme code and NAV value for batch processing later
            // We'll look up all fund IDs in a single query rather than one query per fund
            navEntries.push({
              schemeCode,
              navValue: parseFloat(navValueStr),
              navDate: new Date().toISOString().split('T')[0] // Today's date
            });
            
            processedCount++;
            
            // Update progress every 1000 funds for better performance
            if (processedCount % 1000 === 0) {
              await storage.updateETLRun(etlRunId, {
                recordsProcessed: processedCount,
                errorMessage: `Processed ${processedCount} NAV entries so far`
              });
              console.log(`Processed ${processedCount} NAV entries so far`);
            }
          }
        }
      }
    }
    
    // We need to convert scheme codes to fund IDs
    console.log(`Found ${navEntries.length} valid NAV entries from AMFI data, looking up fund IDs...`);
    
    // Update ETL run status
    await storage.updateETLRun(etlRunId, {
      recordsProcessed: navEntries.length,
      errorMessage: `Looking up fund IDs for ${navEntries.length} entries...`
    });
    
    // Get all scheme codes
    const schemeCodes = navEntries.map(entry => entry.schemeCode);
    
    // Create a lookup map of scheme code to fund ID with a single query
    console.log(`Looking up fund IDs for ${schemeCodes.length} unique scheme codes...`);
    
    // Split into chunks of 1000 to avoid query size limits
    const schemeCodeChunks = [];
    const chunkSize = 1000;
    for (let i = 0; i < schemeCodes.length; i += chunkSize) {
      schemeCodeChunks.push(schemeCodes.slice(i, i + chunkSize));
    }
    
    const schemeCodeToFundId = new Map();
    let fundIdLookupCount = 0;
    
    for (const schemeCodeChunk of schemeCodeChunks) {
      try {
        // Build a query to get all fund IDs in one go
        const placeholders = schemeCodeChunk.map((_, index) => `$${index + 1}`).join(',');
        const query = `SELECT id, scheme_code FROM funds WHERE scheme_code IN (${placeholders})`;
        
        const fundResult = await executeRawQuery(query, schemeCodeChunk);
        
        // Map scheme codes to fund IDs
        for (const row of fundResult.rows) {
          schemeCodeToFundId.set(row.scheme_code, row.id);
          fundIdLookupCount++;
        }
        
        // Update progress
        await storage.updateETLRun(etlRunId, {
          errorMessage: `Looked up ${fundIdLookupCount} fund IDs so far...`
        });
      } catch (error) {
        console.error('Error looking up fund IDs:', error);
      }
    }
    
    console.log(`Successfully looked up ${fundIdLookupCount} fund IDs`);
    
    // Create the final NAV entries with fund IDs
    const navDataToInsert = [];
    for (const entry of navEntries) {
      const fundId = schemeCodeToFundId.get(entry.schemeCode);
      if (fundId) {
        navDataToInsert.push({
          fundId,
          navDate: entry.navDate,
          navValue: entry.navValue
        });
      }
    }
    
    console.log(`Prepared ${navDataToInsert.length} NAV entries for insertion`);
    
    // Batch insert NAV entries with larger batch size for better performance
    console.log(`Inserting ${navDataToInsert.length} NAV entries to database...`);
    const batchSize = 500; // Increased batch size
    let insertedCount = 0;
    
    // Update ETL run with insertion status
    await storage.updateETLRun(etlRunId, {
      errorMessage: `Starting to insert ${navDataToInsert.length} NAV entries...`
    });
    
    for (let i = 0; i < navDataToInsert.length; i += batchSize) {
      const batch = navDataToInsert.slice(i, i + batchSize);
      
      try {
        // Use UPSERT instead of INSERT to handle existing records
        // We'll create a custom query to update if the record exists, or insert if it doesn't
        const placeholders = [];
        const values = [];
        let valueIndex = 1;
        
        for (const entry of batch) {
          placeholders.push(`($${valueIndex}, $${valueIndex + 1}, $${valueIndex + 2})`);
          values.push(entry.fundId, entry.navDate, entry.navValue);
          valueIndex += 3;
        }
        
        // Build and execute the UPSERT query
        const query = `
          INSERT INTO nav_data (fund_id, nav_date, nav_value)
          VALUES ${placeholders.join(', ')}
          ON CONFLICT (fund_id, nav_date) 
          DO UPDATE SET nav_value = EXCLUDED.nav_value
        `;
        
        await executeRawQuery(query, values);
        
        insertedCount += batch.length;
        
        // Update progress
        await storage.updateETLRun(etlRunId, {
          recordsProcessed: insertedCount,
          errorMessage: `Processed ${insertedCount} of ${navDataToInsert.length} NAV entries (using upsert)`
        });
        console.log(`Processed batch ${Math.floor(i / batchSize) + 1}, progress: ${insertedCount}/${navDataToInsert.length}`);
      } catch (error) {
        console.error(`Error processing NAV batch ${Math.floor(i / batchSize) + 1}:`, error);
      }
    }
    
    // Mark the ETL run as completed
    await storage.updateETLRun(etlRunId, {
      status: 'COMPLETED',
      endTime: new Date(),
      recordsProcessed: insertedCount,
      errorMessage: `Successfully imported ${insertedCount} NAV entries from AMFI`
    });
    
    console.log(`Daily NAV update completed successfully. Imported ${insertedCount} NAV entries.`);
  } catch (error: any) {
    console.error('Error processing daily NAV update:', error);
    
    // Mark the ETL run as failed
    await storage.updateETLRun(etlRunId, {
      status: 'FAILED',
      endTime: new Date(),
      errorMessage: `Failed to process daily NAV update: ${error.message || 'Unknown error'}`
    });
  }
}

/**
 * Get the status of the daily NAV update
 */
router.get('/status', async (req, res) => {
  try {
    // Get the latest ETL run
    const etlRuns = await storage.getETLRuns('real_daily_nav_update', 1);
    const etlRun = etlRuns.length > 0 ? etlRuns[0] : null;
    
    res.status(200).json({
      success: true,
      etlRun,
      isRunning: etlRun ? etlRun.status === 'RUNNING' : false
    });
  } catch (error: any) {
    console.error('Error getting daily NAV update status:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to get daily NAV update status: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

export default router;