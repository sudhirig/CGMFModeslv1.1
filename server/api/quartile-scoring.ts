import express from 'express';
import { quartileScheduler } from '../services/quartile-scoring-scheduler';
import { storage } from '../storage';
import { pool } from '../db';

const router = express.Router();

// Endpoint to get quartile scoring status
router.get('/status', async (req, res) => {
  try {
    const etlRuns = await storage.getETLRuns('Quartile Scoring', 1);
    
    res.json({
      success: true,
      status: etlRuns.length > 0 ? etlRuns[0] : null
    });
  } catch (error) {
    console.error('Error getting quartile scoring status:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get quartile scoring status'
    });
  }
});

// Endpoint to trigger manual quartile scoring
router.post('/run', async (req, res) => {
  try {
    const category = req.body.category;
    const result = await quartileScheduler.triggerManualRun(category);
    res.json(result);
  } catch (error) {
    console.error('Error triggering quartile scoring:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to trigger quartile scoring'
    });
  }
});

// Endpoint to get quartile distribution stats
router.get('/distribution', async (req, res) => {
  try {
    const category = req.query.category as string | undefined;
    
    console.log("✓ QUARTILE DISTRIBUTION ROUTE - USING STORAGE LAYER");
    
    // Use updated storage layer method for authentic fund_scores_corrected data
    const distributionData = await storage.getQuartileDistribution(category);
    
    console.log("Authentic quartile distribution from storage:", distributionData);
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json({
      ...distributionData,
      lastUpdated: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error getting quartile distribution:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get quartile distribution'
    });
  }
});

// Endpoint to get funds by quartile
router.get('/funds/:quartile', async (req, res) => {
  try {
    const quartile = parseInt(req.params.quartile);
    const category = req.query.category as string | undefined;
    
    if (isNaN(quartile) || quartile < 1 || quartile > 4) {
      return res.status(400).json({
        success: false,
        message: 'Invalid quartile. Must be 1, 2, 3, or 4.'
      });
    }
    
    console.log("✓ QUARTILE FUNDS ROUTE - USING STORAGE LAYER");
    
    // Use updated storage layer method for authentic fund_scores_corrected data
    const fundsData = await storage.getFundsByQuartile(quartile, category);
    
    console.log(`Authentic Q${quartile} funds from storage:`, fundsData.funds?.length || 0, "funds");
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json({
      ...fundsData,
      dataSource: 'fund_scores_corrected',
      lastUpdated: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error getting funds by quartile:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get funds by quartile'
    });
  }
});

// Endpoint to get quartile performance metrics
router.get('/metrics', async (req, res) => {
  try {
    console.log("✓ QUARTILE METRICS ROUTE - USING STORAGE LAYER");
    
    // Use updated storage layer method for authentic fund_scores_corrected data
    const metricsData = await storage.getQuartileMetrics();
    
    console.log("Authentic quartile metrics from storage:", metricsData);
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json({
      ...metricsData,
      dataSource: 'fund_scores_corrected',
      lastUpdated: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error getting quartile metrics:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get quartile metrics'
    });
  }
});

export default router;