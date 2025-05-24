import express from 'express';
import { executeRawQuery } from '../db';

const router = express.Router();

/**
 * Get the total count of funds in the system
 */
router.get('/', async (_req, res) => {
  try {
    // Query the database directly for the count
    const result = await executeRawQuery('SELECT COUNT(*) as count FROM funds');
    const totalCount = parseInt(result.rows[0].count);
    
    res.json({ 
      success: true,
      count: totalCount 
    });
  } catch (error) {
    console.error('Error fetching fund count:', error);
    res.status(500).json({ 
      success: false,
      message: 'Error fetching fund count' 
    });
  }
});

export default router;