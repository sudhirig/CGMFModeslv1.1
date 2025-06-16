/**
 * Unified Scoring API
 * Consolidates scoring operations with proper error handling and validation
 */

import { Router } from 'express';
import { z } from 'zod';
import { unifiedScoringEngine } from '../services/unified-scoring-engine';

const router = Router();

// Request validation schemas
const scoringRequestSchema = z.object({
  scoreDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  forceRecalculation: z.boolean().optional().default(false)
});

const statsRequestSchema = z.object({
  scoreDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional()
});

/**
 * Calculate unified scores for all funds
 * POST /api/unified-scoring/calculate
 */
router.post('/calculate', async (req, res) => {
  try {
    const { scoreDate, forceRecalculation } = scoringRequestSchema.parse(req.body);
    const targetDate = scoreDate || new Date().toISOString().split('T')[0];
    
    console.log(`Starting unified scoring calculation for ${targetDate}`);
    
    // Check if scores already exist unless forced
    if (!forceRecalculation) {
      const stats = await unifiedScoringEngine.getScoringStats(targetDate);
      if (stats.total_funds > 0) {
        return res.json({
          success: true,
          message: 'Scores already exist for this date',
          scoreDate: targetDate,
          stats
        });
      }
    }
    
    await unifiedScoringEngine.calculateAllScores(targetDate);
    const stats = await unifiedScoringEngine.getScoringStats(targetDate);
    
    res.json({
      success: true,
      message: 'Unified scoring completed successfully',
      scoreDate: targetDate,
      stats
    });
    
  } catch (error) {
    console.error('Unified scoring calculation error:', error);
    
    if (error instanceof z.ZodError) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        details: error.errors
      });
    }
    
    res.status(500).json({
      success: false,
      error: 'Failed to calculate unified scores',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

/**
 * Get scoring statistics
 * GET /api/unified-scoring/stats
 */
router.get('/stats', async (req, res) => {
  try {
    const { scoreDate } = statsRequestSchema.parse(req.query);
    const targetDate = scoreDate || new Date().toISOString().split('T')[0];
    
    const stats = await unifiedScoringEngine.getScoringStats(targetDate);
    
    if (stats.total_funds === 0) {
      return res.status(404).json({
        success: false,
        error: 'No scoring data found for the specified date',
        scoreDate: targetDate
      });
    }
    
    res.json({
      success: true,
      scoreDate: targetDate,
      stats: {
        totalFunds: parseInt(stats.total_funds),
        averageScore: parseFloat(stats.avg_score),
        scoreRange: {
          min: parseFloat(stats.min_score),
          max: parseFloat(stats.max_score)
        },
        quartileDistribution: {
          q1: parseInt(stats.q1_count),
          q2: parseInt(stats.q2_count),
          q3: parseInt(stats.q3_count),
          q4: parseInt(stats.q4_count)
        },
        recommendations: {
          strongBuy: parseInt(stats.strong_buy_count),
          buy: parseInt(stats.buy_count),
          hold: parseInt(stats.hold_count),
          sell: parseInt(stats.sell_count)
        },
        distributionPercentages: {
          positiveRecommendations: ((parseInt(stats.strong_buy_count) + parseInt(stats.buy_count)) / parseInt(stats.total_funds) * 100).toFixed(1)
        }
      }
    });
    
  } catch (error) {
    console.error('Scoring stats error:', error);
    
    if (error instanceof z.ZodError) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        details: error.errors
      });
    }
    
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve scoring statistics',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

/**
 * Health check endpoint
 * GET /api/unified-scoring/health
 */
router.get('/health', async (req, res) => {
  try {
    // Simple database connectivity check
    const stats = await unifiedScoringEngine.getScoringStats();
    
    res.json({
      success: true,
      status: 'healthy',
      timestamp: new Date().toISOString(),
      database: 'connected',
      lastScoringDate: stats.total_funds > 0 ? 'available' : 'no_data'
    });
    
  } catch (error) {
    res.status(503).json({
      success: false,
      status: 'unhealthy',
      timestamp: new Date().toISOString(),
      error: 'Database connectivity issue'
    });
  }
});

export default router;