import express from 'express';
import { fetchAMFIMutualFundData } from '../amfi-scraper';
import { executeRawQuery } from '../db';

const router = express.Router();

// Import real NAV data from AMFI
router.get('/', async (req, res) => {
  try {
    console.log('Starting real AMFI data import...');
    
    // Check if historical data is requested
    const includeHistorical = req.query.historical === 'true';
    
    console.log(`Import options: includeHistorical=${includeHistorical}`);
    
    // If historical import requested, initialize progress tracking first for UI updates
    if (includeHistorical) {
      updateImportProgress({
        isImporting: true,
        totalMonths: 12,
        completedMonths: 0,
        currentMonth: 'Initializing',
        currentYear: new Date().getFullYear().toString(),
        totalImported: 0,
        errors: []
      });
      
      // Response sent immediately to allow UI to update
      res.json({
        success: true,
        message: "Historical NAV data import started. Check progress status for updates.",
        inProgress: true
      });
      
      // Start the historical data import in the background (non-blocking)
      fetchAMFIMutualFundData(includeHistorical)
        .then(result => {
          console.log('AMFI data import completed with result:', {
            success: result.success,
            counts: result.counts
          });
        })
        .catch(error => {
          console.error('Error in background AMFI data import:', error);
          updateImportProgress({
            isImporting: false,
            errors: [String(error)]
          });
        });
      
      return; // Return early after starting background process
    }
    
    // For regular import (non-historical), proceed as normal
    const result = await fetchAMFIMutualFundData(includeHistorical);
    
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
    res.status(500).json({
      success: false,
      message: 'Failed to import AMFI data: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

// Track import progress globally
let importProgress = {
  isImporting: false,
  totalMonths: 0,
  completedMonths: 0,
  currentMonth: '',
  currentYear: '',
  totalImported: 0,
  lastUpdated: new Date(),
  errors: [] as string[]
};

// Update import progress
export function updateImportProgress(update: Partial<typeof importProgress>) {
  importProgress = {
    ...importProgress,
    ...update,
    lastUpdated: new Date()
  };
}

// Get current import progress
router.get('/progress', (req, res) => {
  res.json(importProgress);
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
    
    res.json({
      success: true,
      fundCount,
      navCount,
      navDataRange: {
        earliestDate,
        latestDate
      }
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