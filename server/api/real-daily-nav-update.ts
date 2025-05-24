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
    
    // Fetch the latest NAV data from AMFI
    const response = await axios.get(AMFI_NAV_ALL_URL, {
      timeout: 30000 // 30 second timeout
    });
    
    if (!response.data) {
      await storage.updateETLRun(etlRunId, {
        status: 'FAILED',
        endTime: new Date(),
        errorMessage: 'No data received from AMFI'
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
      
      // Parse fund data line (semicolon separated)
      if (line.includes(';')) {
        const parts = line.split(';');
        
        if (parts.length >= 5) {
          const schemeCode = parts[0].trim();
          
          // Look up the fund by scheme code
          try {
            const fundResult = await executeRawQuery(
              `SELECT id FROM funds WHERE scheme_code = $1`,
              [schemeCode]
            );
            
            if (fundResult.rows && fundResult.rows.length > 0) {
              const fundId = fundResult.rows[0].id;
              
              // Parse NAV value
              let navValueStr = parts[4].trim().replace(/,/g, '');
              if (navValueStr && !isNaN(parseFloat(navValueStr))) {
                // Get current date as ISO string (YYYY-MM-DD)
                const today = new Date().toISOString().split('T')[0];
                
                // Add NAV entry
                navEntries.push({
                  fundId,
                  navDate: today,
                  navValue: navValueStr
                });
                
                processedCount++;
                
                // Update progress every 100 funds
                if (processedCount % 100 === 0) {
                  await storage.updateETLRun(etlRunId, {
                    recordsProcessed: processedCount,
                    errorMessage: `Processed ${processedCount} NAV entries so far`
                  });
                  console.log(`Processed ${processedCount} NAV entries so far`);
                }
              }
            }
          } catch (error) {
            console.error(`Error processing fund with scheme code ${schemeCode}:`, error);
          }
        }
      }
    }
    
    // Batch insert NAV entries (100 at a time)
    console.log(`Inserting ${navEntries.length} NAV entries to database...`);
    const batchSize = 100;
    let insertedCount = 0;
    
    for (let i = 0; i < navEntries.length; i += batchSize) {
      const batch = navEntries.slice(i, i + batchSize);
      
      try {
        await storage.bulkInsertNavData(batch);
        insertedCount += batch.length;
        
        // Update progress
        await storage.updateETLRun(etlRunId, {
          recordsProcessed: insertedCount,
          errorMessage: `Inserted ${insertedCount} of ${navEntries.length} NAV entries`
        });
        console.log(`Inserted batch ${Math.floor(i / batchSize) + 1}, progress: ${insertedCount}/${navEntries.length}`);
      } catch (error) {
        console.error(`Error inserting NAV batch ${Math.floor(i / batchSize) + 1}:`, error);
      }
      
      // Small delay to avoid database overload
      await new Promise(resolve => setTimeout(resolve, 500));
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