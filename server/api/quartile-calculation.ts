import express from 'express';
import { CorrectedScoringEngine } from '../services/corrected-scoring-engine';
import { storage } from '../storage';

const router = express.Router();

/**
 * Trigger quartile calculation for all eligible funds
 */
router.post('/calculate', async (req, res) => {
  try {
    // Create ETL run to track the quartile calculation process
    const etlRun = await storage.createETLRun({
      pipelineName: 'quartile_calculation',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0,
      errorMessage: 'Starting quartile calculation using authentic historical NAV data'
    });

    // Start the calculation process
    processQuartileCalculation(etlRun.id);

    res.json({
      success: true,
      message: 'Quartile calculation started using authentic historical data',
      etlRunId: etlRun.id
    });

  } catch (error: any) {
    console.error('Error starting quartile calculation:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to start quartile calculation: ' + error.message
    });
  }
});

/**
 * Get quartile calculation status
 */
router.get('/status', async (req, res) => {
  try {
    const latestRun = await storage.getETLRuns('quartile_calculation', 1);
    
    if (latestRun.length === 0) {
      return res.json({
        success: true,
        status: 'No quartile calculations found'
      });
    }

    res.json({
      success: true,
      etlRun: latestRun[0]
    });

  } catch (error: any) {
    console.error('Error getting quartile calculation status:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get status: ' + error.message
    });
  }
});

/**
 * Get quartile rankings
 */
router.get('/rankings', async (req, res) => {
  try {
    const { category, quartile } = req.query;
    
    const rankings = await getQuartileRankings(
      category as string, 
      quartile ? parseInt(quartile as string) : undefined
    );
    
    res.json({
      success: true,
      rankings
    });

  } catch (error: any) {
    console.error('Error fetching quartile rankings:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch rankings: ' + error.message
    });
  }
});

/**
 * Get quartile distribution summary
 */
router.get('/distribution', async (req, res) => {
  try {
    const distribution = await getQuartileDistribution();
    
    res.json({
      success: true,
      distribution
    });

  } catch (error: any) {
    console.error('Error fetching quartile distribution:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch distribution: ' + error.message
    });
  }
});

/**
 * Get performance metrics for a specific fund
 */
router.get('/fund/:fundId/metrics', async (req, res) => {
  try {
    const fundId = parseInt(req.params.fundId);
    
    if (isNaN(fundId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid fund ID'
      });
    }
    
    const metrics = await getFundPerformanceMetrics(fundId);
    
    if (!metrics) {
      return res.status(404).json({
        success: false,
        message: 'Performance metrics not found for this fund'
      });
    }
    
    res.json({
      success: true,
      metrics
    });

  } catch (error: any) {
    console.error('Error fetching fund performance metrics:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch metrics: ' + error.message
    });
  }
});

/**
 * Background process for quartile calculation
 */
async function processQuartileCalculation(etlRunId: number) {
  const startTime = new Date();
  
  try {
    console.log('=== Starting Quartile Calculation Process ===');

    await storage.updateETLRun(etlRunId, {
      errorMessage: 'Calculating performance metrics using authentic historical NAV data'
    });

    // Perform the quartile calculation
    const result = await calculateQuartileRankings();

    if (result.success) {
      await storage.updateETLRun(etlRunId, {
        status: 'COMPLETED',
        endTime: new Date(),
        recordsProcessed: result.processedFunds,
        errorMessage: result.message
      });

      console.log('=== Quartile Calculation Complete ===');
      console.log(`Processed: ${result.processedFunds} funds`);
      console.log('Distribution:', result.quartileDistribution);
    } else {
      await storage.updateETLRun(etlRunId, {
        status: 'FAILED',
        endTime: new Date(),
        errorMessage: result.message
      });
      console.error('Quartile calculation failed:', result.message);
    }

  } catch (error: any) {
    console.error('Error in quartile calculation process:', error);
    await storage.updateETLRun(etlRunId, {
      status: 'FAILED',
      endTime: new Date(),
      errorMessage: `Calculation failed: ${error.message}`
    });
  }
}

export default router;