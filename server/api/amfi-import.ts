import express from 'express';
import { fetchAMFIMutualFundData } from '../amfi-scraper';

const router = express.Router();

// Import real NAV data from AMFI
router.get('/', async (req, res) => {
  try {
    console.log('Starting real AMFI data import...');
    const result = await fetchAMFIMutualFundData();
    
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

// Check NAV data count
router.get('/status', async (req, res) => {
  try {
    const { pool } = require('../db');
    
    // Get fund count
    const fundResult = await pool.query('SELECT COUNT(*) FROM funds');
    const fundCount = parseInt(fundResult.rows[0].count);
    
    // Get NAV data count
    const navResult = await pool.query('SELECT COUNT(*) FROM nav_data');
    const navCount = parseInt(navResult.rows[0].count);
    
    // Get date range of NAV data
    const dateRangeResult = await pool.query(`
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