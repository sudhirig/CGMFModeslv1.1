import express from 'express';
import { fetchAMFIMutualFundData } from '../amfi-scraper';
import { storage } from '../storage';

const router = express.Router();

// Import historical NAV data from AMFI
router.post('/', async (req, res) => {
  try {
    console.log('Starting historical AMFI data import...');
    
    // Log start of ETL process
    const etlRun = await storage.createETLRun({
      pipelineName: 'Historical NAV Import',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Import started'
    });
    
    // Always pass true to include historical data
    const result = await fetchAMFIMutualFundData(true);
    
    // Update ETL status
    await storage.updateETLRun(etlRun.id, {
      status: 'COMPLETED',
      endTime: new Date(),
      recordsProcessed: result.counts.totalFunds + result.counts.historicalNavCount,
      errorMessage: `Successfully imported ${result.counts.historicalNavCount} historical NAV data points`
    });
    
    console.log('Historical AMFI data import completed with result:', {
      success: result.success,
      counts: result.counts
    });
    
    res.json({
      success: result.success,
      message: result.message,
      data: result.counts
    });
  } catch (error: any) {
    console.error('Error importing historical AMFI data:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to import historical AMFI data: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

export default router;