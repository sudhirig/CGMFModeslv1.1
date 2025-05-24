import express from 'express';
import { fundDetailsCollector } from '../services/fund-details-collector';

const router = express.Router();

// Import fund details for all funds
router.get('/', async (req, res) => {
  try {
    console.log('Starting fund details import...');
    
    // Check if specific fund IDs were provided
    const fundIds = req.query.fundIds 
      ? (Array.isArray(req.query.fundIds) 
          ? req.query.fundIds.map(id => parseInt(id as string))
          : [parseInt(req.query.fundIds as string)]
        ).filter(id => !isNaN(id))
      : undefined;
    
    // Run the fund details collector
    const result = await fundDetailsCollector.collectFundDetails(fundIds);
    
    console.log('Fund details import completed with result:', result);
    
    res.json({
      success: result.success,
      message: result.message,
      count: result.count
    });
  } catch (error: any) {
    console.error('Error importing fund details:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to import fund details: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

// Schedule regular fund details collection
router.get('/schedule', async (req, res) => {
  try {
    // Get interval in hours, default to weekly (168 hours)
    const intervalHours = req.query.hours ? parseInt(req.query.hours as string) : 168;
    
    // Start the scheduled job
    fundDetailsCollector.startScheduledDetailsFetch(intervalHours);
    
    res.json({
      success: true,
      message: `Scheduled fund details collection every ${intervalHours} hours`,
      nextRun: new Date(Date.now() + intervalHours * 60 * 60 * 1000).toISOString()
    });
  } catch (error: any) {
    console.error('Error scheduling fund details collection:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to schedule fund details collection: ' + (error.message || 'Unknown error')
    });
  }
});

export default router;