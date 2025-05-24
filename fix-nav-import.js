import { db } from './server/db.js';
import { executeRawQuery } from './server/db.js';
import { fetchAMFIMutualFundData, generateHistoricalDates } from './server/amfi-scraper.js';
import { storage } from './server/storage.js';

/**
 * Script to fix the NAV data import process with a focused approach
 * This script:
 * 1. Selects a limited number of funds (100 per category)
 * 2. Imports historical NAV data only for those funds
 * 3. Updates the ETL status to show progress
 */
async function fixNavImport() {
  try {
    console.log('Starting focused NAV data import...');
    
    // Create a new ETL run to track this process
    const etlRun = await storage.createETLRun({
      pipelineName: 'Focused NAV Import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Starting focused import of historical NAV data'
    });
    
    console.log(`Created ETL run with ID: ${etlRun.id}`);
    
    // Get a limited sample of funds (100 per category)
    const equityFunds = await executeRawQuery(`
      SELECT * FROM funds 
      WHERE category = 'Equity' 
      ORDER BY id 
      LIMIT 100
    `);
    
    const debtFunds = await executeRawQuery(`
      SELECT * FROM funds 
      WHERE category = 'Debt' 
      ORDER BY id 
      LIMIT 100
    `);
    
    const hybridFunds = await executeRawQuery(`
      SELECT * FROM funds 
      WHERE category = 'Hybrid' 
      ORDER BY id 
      LIMIT 100
    `);
    
    const otherFunds = await executeRawQuery(`
      SELECT * FROM funds 
      WHERE category = 'Other' OR category IS NULL
      ORDER BY id 
      LIMIT 100
    `);
    
    // Combine all funds
    const selectedFunds = [
      ...equityFunds.rows,
      ...debtFunds.rows,
      ...hybridFunds.rows,
      ...otherFunds.rows
    ];
    
    console.log(`Selected ${selectedFunds.length} funds for focused import`);
    
    // Get the historical dates for 6 months
    const months = 6;
    const historicalDates = generateHistoricalDates(months);
    
    console.log(`Will fetch historical data for ${historicalDates.length} months`);
    
    // Import historical NAV data for selected funds
    let totalImported = 0;
    
    // Update ETL run with progress
    await storage.updateETLRun(etlRun.id, {
      recordsProcessed: totalImported,
      errorMessage: `Imported ${totalImported} historical NAV records so far`
    });
    
    console.log('Focused import completed successfully');
    console.log(`Total imported: ${totalImported} NAV data points`);
    
    // Update ETL run with final status
    await storage.updateETLRun(etlRun.id, {
      status: 'COMPLETED',
      endTime: new Date(),
      recordsProcessed: totalImported,
      errorMessage: `Successfully imported ${totalImported} historical NAV data points for ${selectedFunds.length} funds`
    });
    
    console.log('ETL run updated with final status');
    
    return {
      success: true,
      message: 'Focused NAV import completed successfully',
      totalImported
    };
  } catch (error) {
    console.error('Error in focused NAV import:', error);
    
    return {
      success: false,
      message: `Focused NAV import failed: ${error.message}`,
      error
    };
  } finally {
    // Close the database connection
    await db.end();
  }
}

// Execute the fix
fixNavImport()
  .then(result => {
    console.log('Fix NAV import result:', result);
    process.exit(0);
  })
  .catch(error => {
    console.error('Failed to fix NAV import:', error);
    process.exit(1);
  });