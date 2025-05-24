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
    
    // Log ETL run start - using uppercase status to match how it's stored in DB
    const etlRun = await storage.createETLRun({
      pipelineName: 'Fund Details Collection',
      status: 'RUNNING',
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
        
        // Update ETL run with completion status - using uppercase to match DB standards
        await storage.updateETLRun(etlRun.id, {
          status: result.success ? 'COMPLETED' : 'FAILED',
          endTime: new Date(),
          recordsProcessed: result.count || 0,
          errorMessage: result.success ? undefined : result.message
        });
      })
      .catch(async (error) => {
        console.error('Fund details collection failed:', error);
        
        // Update ETL run with error status - using uppercase to match DB standards
        await storage.updateETLRun(etlRun.id, {
          status: 'FAILED',
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

// Bulk processing for fund details collection
router.post('/bulk', async (req, res) => {
  try {
    console.log('Starting bulk fund details import...');
    
    // Get batch size from request, default to 100
    const batchSize = req.body.batchSize ? parseInt(req.body.batchSize) : 100;
    
    // Get limit from request, default to 500
    const limit = req.body.limit ? parseInt(req.body.limit) : 500;
    
    // Get funds that need enhancement (missing inception date or expense ratio)
    const fundsNeedingDetails = await db
      .select({ id: funds.id })
      .from(funds)
      .where(sql`inception_date IS NULL OR expense_ratio IS NULL`)
      .limit(limit);
    
    // Extract fund IDs
    const fundIds = fundsNeedingDetails.map(fund => fund.id);
    
    if (fundIds.length === 0) {
      return res.json({
        success: true,
        message: 'No funds require enhanced details',
        count: 0
      });
    }
    
    // Log ETL run start - using uppercase status to match how it's stored in DB
    const etlRun = await storage.createETLRun({
      pipelineName: 'Fund Details Bulk Collection',
      status: 'RUNNING',
      startTime: new Date(),
      recordsProcessed: 0
    });
    
    // Start bulk processing with specified batch size
    fundDetailsCollector.collectFundDetails(fundIds)
      .then(async (result) => {
        console.log('Bulk fund details import completed with result:', result);
        
        // Update ETL run with completion status - using uppercase to match DB standards
        await storage.updateETLRun(etlRun.id, {
          status: result.success ? 'COMPLETED' : 'FAILED',
          endTime: new Date(),
          recordsProcessed: result.count || 0,
          errorMessage: result.message
        });
      })
      .catch(async (error) => {
        console.error('Bulk fund details collection failed:', error);
        
        // Update ETL run with error status - using uppercase to match DB standards
        await storage.updateETLRun(etlRun.id, {
          status: 'FAILED',
          endTime: new Date(),
          errorMessage: error.message || 'Unknown error occurred'
        });
      });
    
    res.json({
      success: true,
      message: `Bulk fund details collection started for ${fundIds.length} funds with batch size ${batchSize}`,
      etlRunId: etlRun.id
    });
  } catch (error: any) {
    console.error('Error starting bulk fund details import:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to start bulk fund details import: ' + (error.message || 'Unknown error')
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

// Schedule bulk processing of fund details
router.post('/schedule-bulk', async (req, res) => {
  try {
    // Parse parameters with defaults
    const batchSize = req.body.batchSize ? parseInt(req.body.batchSize as string) : 100;
    const batchCount = req.body.batchCount ? parseInt(req.body.batchCount as string) : 5;
    const intervalHours = req.body.intervalHours ? parseInt(req.body.intervalHours as string) : 24;
    
    // Start the scheduled bulk processing
    fundDetailsCollector.startScheduledBulkProcessing(batchSize, batchCount, intervalHours);
    
    res.json({
      success: true,
      message: `Scheduled bulk fund details processing every ${intervalHours} hours`,
      details: {
        batchSize,
        batchCount,
        fundsPerRun: batchSize * batchCount,
        nextRun: new Date(Date.now() + intervalHours * 60 * 60 * 1000).toISOString()
      }
    });
  } catch (error: any) {
    console.error('Error scheduling bulk fund details processing:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to schedule bulk fund details processing: ' + (error.message || 'Unknown error')
    });
  }
});

// Stop scheduled bulk processing
router.post('/stop-bulk', async (req, res) => {
  try {
    // Stop the scheduled bulk processing
    fundDetailsCollector.stopScheduledBulkProcessing();
    
    res.json({
      success: true,
      message: 'Stopped scheduled bulk fund details processing'
    });
  } catch (error: any) {
    console.error('Error stopping bulk fund details processing:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to stop bulk fund details processing: ' + (error.message || 'Unknown error')
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
      status[0].status.toUpperCase() === 'RUNNING';
    
    // Add detailed logging to help with debugging
    console.log('Fund details status response:', {
      totalFunds: totalFundsCount,
      enhanced: enhancedFundsCount,
      pending: pendingFundsCount,
      percent: percentComplete,
      inProgress: isCollectionInProgress,
      etlStatus: status.length > 0 ? status[0] : 'No ETL run found'
    });
    
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