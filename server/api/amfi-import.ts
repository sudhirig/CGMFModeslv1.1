import express from 'express';
import { fetchAMFIMutualFundData } from '../amfi-scraper';
import { executeRawQuery } from '../db';
import { storage } from '../storage';
import { insertEtlPipelineRunSchema } from '../../shared/schema';

const router = express.Router();

// Timers for scheduled imports
let dailyImportTimer: NodeJS.Timeout | null = null;
let weeklyHistoricalImportTimer: NodeJS.Timeout | null = null;

// Millisecond conversion constants
const MINUTE_MS = 60 * 1000;
const HOUR_MS = 60 * MINUTE_MS;
const DAY_MS = 24 * HOUR_MS;

// Helper function to log ETL operation in database
async function logETLOperation(
  pipelineName: string, 
  status: 'RUNNING' | 'COMPLETED' | 'FAILED', 
  recordsProcessed?: number,
  errorMessage?: string
) {
  try {
    const now = new Date();
    
    const etlRun = {
      pipelineName,
      status,
      startTime: now,
      endTime: status !== 'RUNNING' ? now : undefined,
      recordsProcessed,
      errorMessage
    };
    
    await storage.createETLRun(etlRun);
    console.log(`ETL operation logged: ${pipelineName}, status: ${status}`);
  } catch (error) {
    console.error('Failed to log ETL operation:', error);
  }
}

// Run a scheduled import task
async function runScheduledImport(isHistorical: boolean) {
  const pipelineName = isHistorical 
    ? 'scheduled_historical_import' 
    : 'scheduled_daily_update';
  
  try {
    // Log start of ETL process
    await logETLOperation(pipelineName, 'RUNNING');
    
    console.log(`Starting scheduled ${isHistorical ? 'historical' : 'daily'} AMFI data import...`);
    const result = await fetchAMFIMutualFundData(isHistorical);
    
    // Log completion
    await logETLOperation(
      pipelineName, 
      'COMPLETED', 
      result.counts.totalFunds + result.counts.historicalNavCount
    );
    
    console.log(`Scheduled ${isHistorical ? 'historical' : 'daily'} import completed successfully.`);
    return result;
  } catch (error: any) {
    console.error(`Error in scheduled ${isHistorical ? 'historical' : 'daily'} import:`, error);
    
    // Log error
    await logETLOperation(
      pipelineName,
      'FAILED',
      0,
      error.message || 'Unknown error'
    );
    
    throw error;
  }
}

// Schedule an import task
router.get('/schedule-import', async (req, res) => {
  try {
    const type = req.query.type as string || 'daily';
    const interval = req.query.interval as string || 'daily';
    
    // Clear any existing timers first
    if (type === 'daily' && dailyImportTimer) {
      clearInterval(dailyImportTimer);
      dailyImportTimer = null;
    } else if (type === 'historical' && weeklyHistoricalImportTimer) {
      clearInterval(weeklyHistoricalImportTimer);
      weeklyHistoricalImportTimer = null;
    }
    
    let intervalMs = DAY_MS; // Default to daily
    let isHistorical = type === 'historical';
    
    // Set appropriate interval
    if (interval === 'daily') {
      intervalMs = DAY_MS;
    } else if (interval === 'hourly') {
      intervalMs = HOUR_MS;
    } else if (interval === 'weekly') {
      intervalMs = 7 * DAY_MS;
    } else if (interval === 'test') {
      // For testing only - run every 5 minutes
      intervalMs = 5 * MINUTE_MS; 
    }
    
    // Set up the timer
    if (type === 'daily') {
      // Run immediate first import
      await runScheduledImport(false);
      
      // Schedule regular imports
      dailyImportTimer = setInterval(async () => {
        try {
          await runScheduledImport(false);
        } catch (error) {
          console.error('Error in scheduled daily import:', error);
        }
      }, intervalMs);
      
      console.log(`Daily NAV updates scheduled with ${interval} frequency`);
    } else if (type === 'historical') {
      // Run immediate first import
      await runScheduledImport(true);
      
      // Schedule regular imports
      weeklyHistoricalImportTimer = setInterval(async () => {
        try {
          await runScheduledImport(true);
        } catch (error) {
          console.error('Error in scheduled historical import:', error);
        }
      }, intervalMs);
      
      console.log(`Historical data imports scheduled with ${interval} frequency`);
    }
    
    res.json({
      success: true,
      message: `Scheduled ${type} imports with ${interval} frequency`,
      nextRun: new Date(Date.now() + intervalMs).toISOString()
    });
  } catch (error: any) {
    console.error('Error scheduling import:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to schedule import: ' + (error.message || 'Unknown error')
    });
  }
});

// Stop scheduled imports
router.get('/stop-scheduled-import', async (req, res) => {
  try {
    const type = req.query.type as string || 'all';
    
    if (type === 'daily' || type === 'all') {
      if (dailyImportTimer) {
        clearInterval(dailyImportTimer);
        dailyImportTimer = null;
        console.log('Daily import schedule stopped');
      }
    }
    
    if (type === 'historical' || type === 'all') {
      if (weeklyHistoricalImportTimer) {
        clearInterval(weeklyHistoricalImportTimer);
        weeklyHistoricalImportTimer = null;
        console.log('Historical import schedule stopped');
      }
    }
    
    res.json({
      success: true,
      message: `Stopped scheduled ${type} imports`
    });
  } catch (error: any) {
    console.error('Error stopping scheduled import:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to stop scheduled import: ' + (error.message || 'Unknown error')
    });
  }
});

// Check scheduled import status
router.get('/scheduled-status', async (req, res) => {
  try {
    // Get latest ETL runs for each pipeline
    const dailyRuns = await storage.getETLRuns('scheduled_daily_update', 1);
    const historicalRuns = await storage.getETLRuns('scheduled_historical_import', 1);
    
    res.json({
      success: true,
      scheduledImports: {
        daily: {
          active: dailyImportTimer !== null,
          lastRun: dailyRuns.length > 0 ? dailyRuns[0] : null
        },
        historical: {
          active: weeklyHistoricalImportTimer !== null,
          lastRun: historicalRuns.length > 0 ? historicalRuns[0] : null
        }
      }
    });
  } catch (error: any) {
    console.error('Error checking scheduled import status:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to check scheduled import status: ' + (error.message || 'Unknown error')
    });
  }
});

// Import real NAV data from AMFI
router.get('/', async (req, res) => {
  try {
    console.log('Starting real AMFI data import...');
    
    // Check if historical data is requested
    const includeHistorical = req.query.historical === 'true';
    
    console.log(`Import options: includeHistorical=${includeHistorical}`);
    
    // Log start of ETL process
    await logETLOperation(
      includeHistorical ? 'manual_historical_import' : 'manual_daily_update', 
      'RUNNING'
    );
    
    // Fetch AMFI data with or without historical data
    const result = await fetchAMFIMutualFundData(includeHistorical);
    
    // Log completion
    await logETLOperation(
      includeHistorical ? 'manual_historical_import' : 'manual_daily_update',
      'COMPLETED',
      result.counts.totalFunds + result.counts.historicalNavCount
    );
    
    console.log('AMFI data import completed with result:', {
      success: result.success,
      counts: result.counts
    });
    
    res.json({
      success: result.success,
      message: result.message,
      data: result.counts
    });
  } catch (error: any) {
    console.error('Error importing real AMFI data:', error);
    
    // Log error
    await logETLOperation(
      'manual_import',
      'FAILED',
      0,
      error.message || 'Unknown error'
    );
    
    res.status(500).json({
      success: false,
      message: 'Failed to import AMFI data: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

// Check NAV data count
router.get('/status', async (req, res) => {
  try {
    // Get fund count
    const fundResult = await executeRawQuery('SELECT COUNT(*) FROM funds');
    const fundCount = parseInt(fundResult.rows[0].count);
    
    // Get NAV data count
    const navResult = await executeRawQuery('SELECT COUNT(*) FROM nav_data');
    const navCount = parseInt(navResult.rows[0].count);
    
    // Get date range of NAV data
    const dateRangeResult = await executeRawQuery(`
      SELECT 
        MIN(nav_date) as earliest_date,
        MAX(nav_date) as latest_date
      FROM nav_data
    `);
    
    const earliestDate = dateRangeResult.rows[0].earliest_date;
    const latestDate = dateRangeResult.rows[0].latest_date;
    
    // Get the status of ETL runs
    const recentRuns = await storage.getETLRuns(undefined, 5);
    
    res.json({
      success: true,
      fundCount,
      navCount,
      navDataRange: {
        earliestDate,
        latestDate
      },
      recentETLRuns: recentRuns
    });
  } catch (error: any) {
    console.error('Error checking AMFI data status:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to check AMFI data status: ' + (error.message || 'Unknown error')
    });
  }
});

export default router;