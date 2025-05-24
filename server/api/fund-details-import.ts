import express from 'express';
import { fundDetailsCollector } from '../services/fund-details-collector';
import { db } from '../db';
import { funds } from '../../shared/schema';
import { sql } from 'drizzle-orm';
import { isNotNull } from 'drizzle-orm/expressions';
import { and } from 'drizzle-orm/expressions';
import { storage } from '../storage';

const router = express.Router();

// Import fund details for all funds
router.post('/', async (req, res) => {
  try {
    console.log('Starting fund details import...');
    
    // Log ETL run start
    const etlRun = await storage.createETLRun({
      pipelineName: 'Fund Details Collection',
      status: 'running',
      startTime: new Date(),
      recordsProcessed: 0
    });
    
    // Check if specific fund IDs were provided
    const fundIds = req.body.fundIds 
      ? (Array.isArray(req.body.fundIds) 
          ? req.body.fundIds.map(id => parseInt(id as string))
          : [parseInt(req.body.fundIds as string)]
        ).filter(id => !isNaN(id))
      : undefined;
    
    // Run the fund details collector (in the background)
    fundDetailsCollector.collectFundDetails(fundIds)
      .then(async (result) => {
        console.log('Fund details import completed with result:', result);
        
        // Update ETL run with completion status
        await storage.updateETLRun(etlRun.id, {
          status: result.success ? 'completed' : 'failed',
          endTime: new Date(),
          recordsProcessed: result.count || 0,
          errorMessage: result.success ? undefined : result.message
        });
      })
      .catch(async (error) => {
        console.error('Fund details collection failed:', error);
        
        // Update ETL run with error status
        await storage.updateETLRun(etlRun.id, {
          status: 'failed',
          endTime: new Date(),
          errorMessage: error.message || 'Unknown error occurred'
        });
      });
    
    res.json({
      success: true,
      message: 'Fund details collection started',
      etlRunId: etlRun.id
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
router.post('/schedule', async (req, res) => {
  try {
    // Get interval in hours, default to weekly (168 hours)
    const intervalHours = req.body.hours ? parseInt(req.body.hours as string) : 168;
    
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

// Get detailed status of fund details collection
router.get('/status', async (req, res) => {
  try {
    // Get status from the ETL runs database
    const status = await storage.getETLRuns('Fund Details Collection', 1);
    
    // Get total funds count
    const totalFunds = await db.select({ count: sql`count(*)` }).from(funds);
    const totalFundsCount = Number(totalFunds[0].count);
    
    // Get count of funds with enhanced details
    const fundsWithDetails = await db.select({ count: sql`count(*)` })
      .from(funds)
      .where(and(
        isNotNull(funds.inceptionDate),
        isNotNull(funds.expenseRatio)
      ));
    const enhancedFundsCount = Number(fundsWithDetails[0].count);
    
    // Calculate funds pending enhanced details
    const pendingFundsCount = totalFundsCount - enhancedFundsCount;
    
    // Calculate percentage completion
    const percentComplete = totalFundsCount > 0 
      ? Math.round((enhancedFundsCount / totalFundsCount) * 100) 
      : 0;
    
    // Check if collection is in progress
    const isCollectionInProgress = status.length > 0 && 
      status[0].status === 'running';
    
    return res.json({
      success: true,
      status: status.length > 0 ? status[0] : null,
      detailsStats: {
        totalFunds: totalFundsCount,
        enhancedFunds: enhancedFundsCount,
        pendingFunds: pendingFundsCount,
        percentComplete: percentComplete,
        isCollectionInProgress: isCollectionInProgress
      }
    });
  } catch (error: any) {
    console.error('Error getting fund details status:', error);
    return res.status(500).json({
      success: false,
      message: `Failed to get fund details status: ${error.message}`
    });
  }
});

export default router;