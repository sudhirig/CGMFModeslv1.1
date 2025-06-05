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
    
    console.log("✓ DIRECT QUARTILE DISTRIBUTION ROUTE HIT!");
    
    let query = `
      SELECT 
        COUNT(*) as total_count,
        SUM(CASE WHEN fsc.quartile = 1 THEN 1 ELSE 0 END) as q1_count,
        SUM(CASE WHEN fsc.quartile = 2 THEN 1 ELSE 0 END) as q2_count,
        SUM(CASE WHEN fsc.quartile = 3 THEN 1 ELSE 0 END) as q3_count,
        SUM(CASE WHEN fsc.quartile = 4 THEN 1 ELSE 0 END) as q4_count
      FROM fund_scores_corrected fsc
      JOIN funds f ON fsc.fund_id = f.id
      WHERE fsc.score_date = CURRENT_DATE
    `;
    
    const params: any[] = [];
    
    if (category) {
      query += ` AND f.category = $1`;
      params.push(category);
    }
    
    const result = await pool.query(query, params);
    console.log("Authentic quartile distribution result:", result.rows[0]);
    
    // Calculate percentages based on actual counts
    const data = result.rows[0];
    const totalCount = parseInt(data.total_count);
    const q1Count = parseInt(data.q1_count);
    const q2Count = parseInt(data.q2_count);
    const q3Count = parseInt(data.q3_count);
    const q4Count = parseInt(data.q4_count);
    
    console.log("Counts:", { totalCount, q1Count, q2Count, q3Count, q4Count });
    
    // Calculate percentages
    const q1Percent = totalCount > 0 ? Math.round((q1Count / totalCount) * 100) : 0;
    const q2Percent = totalCount > 0 ? Math.round((q2Count / totalCount) * 100) : 0;
    const q3Percent = totalCount > 0 ? Math.round((q3Count / totalCount) * 100) : 0;
    const q4Percent = totalCount > 0 ? Math.round((q4Count / totalCount) * 100) : 0;
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json({
      totalCount,
      q1Count,
      q2Count,
      q3Count,
      q4Count,
      q1Percent,
      q2Percent,
      q3Percent,
      q4Percent,
      dataSource: 'fund_scores_corrected',
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
    const limit = parseInt(req.query.limit as string || '20');
    
    if (isNaN(quartile) || quartile < 1 || quartile > 4) {
      return res.status(400).json({
        success: false,
        message: 'Invalid quartile. Must be 1, 2, 3, or 4.'
      });
    }
    
    console.log("✓ DIRECT QUARTILE FUNDS ROUTE HIT!");
    
    let query = `
      SELECT 
        f.id,
        f.fund_name as "fundName",
        f.amc_name as "amcName",
        f.category,
        f.subcategory,
        fsc.total_score as "totalScore",
        fsc.quartile,
        fsc.recommendation
      FROM fund_scores_corrected fsc
      JOIN funds f ON fsc.fund_id = f.id
      WHERE fsc.quartile = $1 AND fsc.score_date = CURRENT_DATE
    `;
    
    const params: any[] = [quartile];
    
    if (category) {
      query += ` AND f.category = $${params.length + 1}`;
      params.push(category);
    }
    
    query += ` ORDER BY fsc.total_score DESC LIMIT $${params.length + 1}`;
    params.push(limit);
    
    const result = await pool.query(query, params);
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json({
      funds: result.rows,
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
    console.log("✓ DIRECT QUARTILE METRICS ROUTE HIT!");
    
    // Get average returns by quartile using authentic fund_scores_corrected data
    const query = `
      SELECT 
        CASE 
          WHEN fsc.quartile = 1 THEN 'Q1'
          WHEN fsc.quartile = 2 THEN 'Q2'
          WHEN fsc.quartile = 3 THEN 'Q3'
          WHEN fsc.quartile = 4 THEN 'Q4'
          ELSE 'Unrated'
        END as name,
        ROUND(AVG(fsc.return_1y_score * 20)::numeric, 2) as "return1Y",
        ROUND(AVG(fsc.total_score)::numeric, 2) as "avgScore",
        COUNT(*) as "fundCount"
      FROM fund_scores_corrected fsc
      JOIN funds f ON fsc.fund_id = f.id
      WHERE fsc.quartile IS NOT NULL AND fsc.score_date = CURRENT_DATE
      GROUP BY fsc.quartile
      ORDER BY fsc.quartile
    `;
    
    const result = await pool.query(query);
    
    res.setHeader('Cache-Control', 'no-cache');
    res.json({
      returnsData: result.rows,
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