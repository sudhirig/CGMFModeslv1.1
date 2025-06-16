/**
 * Script to restart the historical NAV import process with improved implementation
 */
const { pool } = require('./server/db');
const { storage } = require('./server/storage');

async function restartHistoricalNavImport() {
  console.log("=== Restarting Historical NAV Import Process ===");
  
  try {
    // First, cancel any running imports
    await pool.query(`
      UPDATE etl_pipeline_runs
      SET status = 'FAILED',
          end_time = NOW(),
          error_message = 'Process canceled due to being stuck - restarted with improved implementation'
      WHERE status = 'RUNNING'
      AND pipeline_name = 'authentic_historical_import'
    `);
    
    console.log("✓ Canceled any stuck historical NAV import processes");
    
    // Create a new ETL run for this process
    const etlRun = await storage.createETLRun({
      pipelineName: 'authentic_historical_import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Restarting authentic historical NAV data import with improved batch processing and reliability'
    });
    
    console.log(`✓ Created new ETL run with ID: ${etlRun.id}`);
    
    // Get funds that need historical data - prioritize high-value funds
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
      LIMIT 500
    `);
    
    const fundsToProcess = fundsResult.rows;
    
    if (fundsToProcess.length === 0) {
      await storage.updateETLRun(etlRun.id, {
        status: 'COMPLETED',
        endTime: new Date(),
        errorMessage: 'No funds found that need historical NAV data'
      });
      
      console.log("✓ No funds found that need historical NAV data");
      return;
    }
    
    console.log(`✓ Found ${fundsToProcess.length} funds that need historical NAV data`);
    
    // Start the import process
    const processModule = require('./server/api/real-historical-nav-import');
    
    if (processModule.processAuthenticHistoricalImport) {
      console.log("✓ Starting import process in the background...");
      processModule.processAuthenticHistoricalImport(fundsToProcess, etlRun.id);
      console.log(`✓ Successfully initiated historical NAV import for ${fundsToProcess.length} funds`);
    } else {
      console.error("❌ Error: processAuthenticHistoricalImport function not found in module");
      await storage.updateETLRun(etlRun.id, {
        status: 'FAILED',
        endTime: new Date(),
        errorMessage: 'Failed to find the import processing function'
      });
    }
  } catch (error) {
    console.error("❌ Error restarting historical NAV import:", error);
  } finally {
    // Close the pool if we're done
    // pool.end();
  }
}

// Run the function
restartHistoricalNavImport().catch(console.error);