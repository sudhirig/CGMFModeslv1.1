import express from 'express';
import { fetchAMFIMutualFundData, generateHistoricalDates } from '../amfi-scraper';
import { storage } from '../storage';

const router = express.Router();

// Get status of historical NAV data import
router.get('/status', async (req, res) => {
  try {
    // Get the latest historical NAV import ETL run
    const etlRuns = await storage.getETLRuns('Historical NAV Import', 1);
    
    if (etlRuns.length === 0) {
      // Ensure Content-Type is set to application/json
      res.setHeader('Content-Type', 'application/json');
      return res.status(200).json({
        success: true,
        status: {
          isImported: false,
          message: 'No historical NAV data has been imported yet',
          lastRun: null
        }
      });
    }
    
    const latestRun = etlRuns[0];
    const isRunning = latestRun.status === 'RUNNING';
    const isCompleted = latestRun.status === 'COMPLETED';
    
    // Ensure Content-Type is set to application/json
    res.setHeader('Content-Type', 'application/json');
    res.status(200).json({
      success: true,
      status: {
        isImported: isCompleted,
        inProgress: isRunning,
        lastRun: latestRun,
        message: isRunning 
          ? 'Historical NAV data import is in progress' 
          : (isCompleted 
            ? `Successfully imported ${latestRun.recordsProcessed} historical NAV data points` 
            : 'Historical NAV data import failed')
      }
    });
  } catch (error: any) {
    console.error('Error getting historical NAV import status:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to get historical NAV import status: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

// Import historical NAV data from AMFI
router.post('/import', async (req, res) => {
  try {
    // Check if an import is already running
    const etlRuns = await storage.getETLRuns('Historical NAV Import', 1);
    if (etlRuns.length > 0 && etlRuns[0].status === 'RUNNING') {
      return res.status(200).json({
        success: false,
        message: 'Historical NAV data import is already in progress',
        runId: etlRuns[0].id
      });
    }
    
    console.log('Starting historical AMFI data import...');
    
    // Get months to import from request body (default to 12 months)
    const { months = 12 } = req.body;
    
    // Log start of ETL process
    const etlRun = await storage.createETLRun({
      pipelineName: 'Historical NAV Import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: `Import started for ${months} months of historical data`
    });
    
    // Start import process in background
    importHistoricalNavData(etlRun.id, months);
    
    // Ensure Content-Type is set to application/json
    res.setHeader('Content-Type', 'application/json');
    res.status(200).json({
      success: true,
      message: `Historical NAV data import started for ${months} months of data`,
      runId: etlRun.id
    });
  } catch (error: any) {
    console.error('Error starting historical AMFI data import:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to start historical AMFI data import: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

// Function to import historical NAV data in the background
async function importHistoricalNavData(etlRunId: number, months: number) {
  try {
    // Always pass true to include historical data
    const result = await fetchAMFIMutualFundData(true, months);
    
    // Update ETL status
    await storage.updateETLRun(etlRunId, {
      status: 'COMPLETED',
      endTime: new Date(),
      recordsProcessed: result.counts.totalFunds + result.counts.historicalNavCount,
      errorMessage: `Successfully imported ${result.counts.historicalNavCount} historical NAV data points for ${months} months`
    });
    
    console.log('Historical AMFI data import completed with result:', {
      success: result.success,
      counts: result.counts
    });
  } catch (error: any) {
    console.error('Error importing historical AMFI data:', error);
    
    // Update ETL status with error
    await storage.updateETLRun(etlRunId, {
      status: 'FAILED',
      endTime: new Date(),
      errorMessage: `Failed to import historical NAV data: ${error.message || 'Unknown error'}`
    });
  }
}

export default router;