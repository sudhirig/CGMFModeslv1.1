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
    
    let query = `
      SELECT 
        COUNT(*) as total_count,
        SUM(CASE WHEN quartile = 1 THEN 1 ELSE 0 END) as q1_count,
        SUM(CASE WHEN quartile = 2 THEN 1 ELSE 0 END) as q2_count,
        SUM(CASE WHEN quartile = 3 THEN 1 ELSE 0 END) as q3_count,
        SUM(CASE WHEN quartile = 4 THEN 1 ELSE 0 END) as q4_count
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE quartile IS NOT NULL
    `;
    
    const params: any[] = [];
    
    if (category) {
      query += ` AND f.category = $1`;
      params.push(category);
    }
    
    const result = await pool.query(query, params);
    
    // Calculate percentages based on actual counts
    const data = result.rows[0];
    const totalCount = parseInt(data.total_count);
    const q1Count = parseInt(data.q1_count);
    const q2Count = parseInt(data.q2_count);
    const q3Count = parseInt(data.q3_count);
    const q4Count = parseInt(data.q4_count);
    
    // Calculate percentages
    const q1Percent = totalCount > 0 ? Math.round((q1Count / totalCount) * 100) : 0;
    const q2Percent = totalCount > 0 ? Math.round((q2Count / totalCount) * 100) : 0;
    const q3Percent = totalCount > 0 ? Math.round((q3Count / totalCount) * 100) : 0;
    const q4Percent = totalCount > 0 ? Math.round((q4Count / totalCount) * 100) : 0;
    
    res.json({
      totalCount,
      q1Count,
      q2Count,
      q3Count,
      q4Count,
      q1Percent,
      q2Percent,
      q3Percent,
      q4Percent
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
    const limit = parseInt(req.query.limit as string || '20');
    
    if (isNaN(quartile) || quartile < 1 || quartile > 4) {
      return res.status(400).json({
        success: false,
        message: 'Invalid quartile. Must be 1, 2, 3, or 4.'
      });
    }
    
    let query = `
      SELECT 
        f.id,
        f.fund_name as "fundName",
        f.amc_name as "amcName",
        f.category,
        f.subcategory,
        fs.total_score as "totalScore",
        fs.quartile,
        fs.recommendation
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.score_date = (SELECT MAX(score_date) FROM fund_scores)
      AND fs.quartile = $1
    `;
    
    const params: any[] = [quartile];
    
    if (category) {
      query += ` AND f.category = $${params.length + 1}`;
      params.push(category);
    }
    
    query += ` ORDER BY fs.total_score DESC LIMIT $${params.length + 1}`;
    params.push(limit);
    
    const result = await pool.query(query, params);
    
    res.json({
      funds: result.rows
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
    // Get average returns by quartile
    const query = `
      WITH latest_nav AS (
        SELECT 
          fund_id, 
          nav_value,
          nav_date
        FROM nav_data
        WHERE (fund_id, nav_date) IN (
          SELECT fund_id, MAX(nav_date) 
          FROM nav_data 
          GROUP BY fund_id
        )
      ),
      year_ago_nav AS (
        SELECT 
          fund_id, 
          nav_value,
          nav_date
        FROM nav_data
        WHERE nav_date BETWEEN CURRENT_DATE - INTERVAL '370 days' AND CURRENT_DATE - INTERVAL '360 days'
        AND (fund_id, nav_date) IN (
          SELECT fund_id, MAX(nav_date)
          FROM nav_data
          WHERE nav_date BETWEEN CURRENT_DATE - INTERVAL '370 days' AND CURRENT_DATE - INTERVAL '360 days'
          GROUP BY fund_id
        )
      )
      SELECT 
        CASE 
          WHEN fs.quartile = 1 THEN 'Q1'
          WHEN fs.quartile = 2 THEN 'Q2'
          WHEN fs.quartile = 3 THEN 'Q3'
          WHEN fs.quartile = 4 THEN 'Q4'
          ELSE 'Unrated'
        END as name,
        ROUND(AVG((ln.nav_value / yn.nav_value - 1) * 100)::numeric, 2) as "return1Y",
        ROUND(AVG(fs.total_score)::numeric, 2) as "avgScore",
        COUNT(*) as "fundCount"
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      JOIN latest_nav ln ON fs.fund_id = ln.fund_id
      JOIN year_ago_nav yn ON fs.fund_id = yn.fund_id
      WHERE fs.score_date = (SELECT MAX(score_date) FROM fund_scores)
      GROUP BY fs.quartile
      ORDER BY fs.quartile
    `;
    
    const result = await pool.query(query);
    
    res.json({
      returnsData: result.rows
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