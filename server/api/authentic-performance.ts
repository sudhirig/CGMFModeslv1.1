import express from 'express';
import { authenticBatchProcessor } from '../services/authentic-batch-processor';

const router = express.Router();

/**
 * Replace synthetic data with authentic performance calculations
 */
router.post('/replace-synthetic', async (req, res) => {
  try {
    console.log('Starting replacement of synthetic data with authentic calculations...');
    
    const result = await authenticBatchProcessor.replaceSyntheticWithAuthentic();
    
    if (result.success) {
      res.json({
        success: true,
        message: result.message,
        stats: {
          processed: result.processed,
          succeeded: result.succeeded,
          failed: result.failed
        }
      });
    } else {
      res.status(500).json({
        success: false,
        message: 'Failed to replace synthetic data',
        error: result.error
      });
    }
    
  } catch (error: any) {
    console.error('Error in authentic performance replacement:', error);
    res.status(500).json({
      success: false,
      message: 'Internal server error',
      error: error.message
    });
  }
});

/**
 * Get status of authentic performance calculations
 */
router.get('/status', async (req, res) => {
  try {
    const status = await authenticBatchProcessor.getProcessingStatus();
    
    res.json({
      success: true,
      status
    });
    
  } catch (error: any) {
    console.error('Error getting authentic performance status:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get status',
      error: error.message
    });
  }
});

export default router;