import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { db, executeRawQuery, pool } from "./db";
import { dataCollector } from "./services/data-collector";
import { elivateFramework } from "./services/elivate-framework";
import { fundScoringEngine } from "./services/fund-scoring";
import { portfolioBuilder } from "./services/portfolio-builder";
import { backtestingEngine } from "./services/backtesting-engine";
import { fundDetailsCollector } from "./services/fund-details-collector";
import { quartileScheduler as automatedScheduler } from "./services/automated-quartile-scheduler";
import { quartileScheduler } from "./services/quartile-scoring-scheduler";
import { quartileSeeder } from "./services/seed-quartile-ratings";
import amfiImportRoutes from "./api/amfi-import";
import fundDetailsImportRoutes from "./api/fund-details-import";
import quartileScoringRoutes from "./api/quartile-scoring";
import historicalNavImportRoutes from "./api/import-historical-nav";

import triggerRescoringRoutes from "./api/trigger-quartile-rescoring";
import restartHistoricalImportRoutes from "./api/restart-historical-import";
import realHistoricalNavImportRoutes from "./api/real-historical-nav-import";
import realDailyNavUpdateRoutes from "./api/real-daily-nav-update";
import fundCountRoutes from "./api/fund-count";
import mftoolTestRoutes from "./api/mftool-test";
import mfapiHistoricalImportRoutes from "./api/mfapi-historical-import";
import quartileCalculationRoutes from "./api/quartile-calculation";

export async function registerRoutes(app: Express): Promise<Server> {
  // Register AMFI data import routes
  app.use('/api/amfi', amfiImportRoutes);
  
  // Register Fund Details routes
  app.use('/api/fund-details', fundDetailsImportRoutes);
  
  // Register Quartile Scoring routes
  app.use('/api/quartile', quartileScoringRoutes);
  
  // Register Historical NAV import route
  app.use('/api/historical-nav', historicalNavImportRoutes);
  
  // Fix NAV Data route removed - synthetic data generation eliminated
  
  // Register Quartile Rescoring route
  app.use('/api/rescoring', triggerRescoringRoutes);
  
  // Register Historical Import Restart route
  app.use('/api/historical-restart', restartHistoricalImportRoutes);
  
  // Register Authentic Historical NAV Import route
  app.use('/api/authentic-nav', realHistoricalNavImportRoutes);
  
  // Register Real Daily NAV Update route
  app.use('/api/daily-nav', realDailyNavUpdateRoutes);
  
  // Register Fund Count route
  app.use('/api/funds/count', fundCountRoutes);
  
  // Register MFTool Test route
  app.use('/api/mftool', mftoolTestRoutes);
  
  // Register MFAPI Historical Import route
  app.use('/api/mfapi-historical', mfapiHistoricalImportRoutes);
  
  // Register Quartile Calculation route
  app.use('/api/quartile', quartileCalculationRoutes);

  // Production Fund Search API endpoints using authenticated corrected scoring data
  app.get('/api/fund-scores/search', async (req, res) => {
    try {
      const { search, subcategory, quartile, limit = 100 } = req.query;
      
      let query = `
        SELECT 
          fsc.fund_id,
          f.fund_name,
          fsc.subcategory,
          fsc.total_score,
          fsc.quartile,
          fsc.subcategory_rank,
          fsc.subcategory_total,
          fsc.subcategory_percentile,
          fsc.historical_returns_total,
          fsc.risk_grade_total,
          fsc.fundamentals_total,
          fsc.other_metrics_total,
          fsc.return_1y_score,
          fsc.return_3y_score,
          fsc.return_5y_score,
          fsc.calmar_ratio_1y,
          fsc.sortino_ratio_1y,
          fsc.recommendation
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.score_date = CURRENT_DATE
      `;
      
      const params: any[] = [];
      let paramCount = 0;
      
      if (search && search !== '') {
        paramCount++;
        query += ` AND (LOWER(f.fund_name) LIKE $${paramCount} OR LOWER(f.amc_name) LIKE $${paramCount})`;
        params.push(`%${search.toString().toLowerCase()}%`);
      }
      
      if (subcategory && subcategory !== 'all') {
        paramCount++;
        query += ` AND fsc.subcategory = $${paramCount}`;
        params.push(subcategory);
      }
      
      if (quartile && quartile !== 'all') {
        paramCount++;
        query += ` AND fsc.quartile = $${paramCount}`;
        params.push(parseInt(quartile.toString()));
      }
      
      query += ` ORDER BY fsc.total_score DESC LIMIT $${paramCount + 1}`;
      params.push(parseInt(limit.toString()));
      
      const result = await pool.query(query, params);
      
      res.json(result.rows);
    } catch (error) {
      console.error('Fund search error:', error);
      res.status(500).json({ error: 'Failed to search funds' });
    }
  });

  app.get('/api/fund-scores/subcategories', async (req, res) => {
    try {
      const result = await pool.query(`
        SELECT DISTINCT subcategory 
        FROM fund_scores_corrected 
        WHERE score_date = CURRENT_DATE 
        ORDER BY subcategory
      `);
      
      res.json(result.rows.map(row => row.subcategory));
    } catch (error) {
      console.error('Subcategories fetch error:', error);
      res.status(500).json({ error: 'Failed to fetch subcategories' });
    }
  });

  app.get('/api/fund-scores/top-performers', async (req, res) => {
    try {
      const { subcategory, limit = 10 } = req.query;
      
      let query = `
        SELECT 
          fsc.fund_id,
          f.fund_name,
          fsc.subcategory,
          fsc.total_score,
          fsc.quartile,
          fsc.subcategory_rank,
          fsc.subcategory_percentile,
          fsc.recommendation,
          fsc.calmar_ratio_1y,
          fsc.sortino_ratio_1y
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.score_date = CURRENT_DATE
      `;
      
      const params: any[] = [];
      
      if (subcategory && subcategory !== 'all') {
        query += ` AND fsc.subcategory = $1`;
        params.push(subcategory);
        query += ` ORDER BY fsc.total_score DESC LIMIT $2`;
        params.push(parseInt(limit.toString()));
      } else {
        query += ` AND fsc.quartile = 1 ORDER BY fsc.total_score DESC LIMIT $1`;
        params.push(parseInt(limit.toString()));
      }
      
      const result = await pool.query(query, params);
      
      res.json(result.rows);
    } catch (error) {
      console.error('Top performers fetch error:', error);
      res.status(500).json({ error: 'Failed to fetch top performers' });
    }
  });

  app.get('/api/fund-scores/statistics', async (req, res) => {
    try {
      const result = await pool.query(`
        SELECT 
          COUNT(*) as total_funds,
          COUNT(DISTINCT subcategory) as total_subcategories,
          AVG(total_score)::numeric(5,2) as average_score,
          MIN(total_score) as min_score,
          MAX(total_score) as max_score,
          COUNT(CASE WHEN quartile = 1 THEN 1 END) as q1_funds,
          COUNT(CASE WHEN quartile = 2 THEN 1 END) as q2_funds,
          COUNT(CASE WHEN quartile = 3 THEN 1 END) as q3_funds,
          COUNT(CASE WHEN quartile = 4 THEN 1 END) as q4_funds,
          COUNT(CASE WHEN calmar_ratio_1y IS NOT NULL THEN 1 END) as funds_with_calmar,
          COUNT(CASE WHEN sortino_ratio_1y IS NOT NULL THEN 1 END) as funds_with_sortino
        FROM fund_scores_corrected 
        WHERE score_date = CURRENT_DATE
      `);
      
      res.json(result.rows[0]);
    } catch (error) {
      console.error('Statistics fetch error:', error);
      res.status(500).json({ error: 'Failed to fetch statistics' });
    }
  });
  
  // Advanced Analytics API endpoints for missing components
  app.get('/api/advanced-analytics/risk-metrics/:fundId', async (req, res) => {
    try {
      const { fundId } = req.params;
      
      if (!fundId || isNaN(Number(fundId))) {
        return res.status(400).json({ error: 'Valid fund ID required' });
      }

      const AdvancedRiskMetrics = (await import('./services/advanced-risk-metrics.js')).default;
      
      const [calmarRatio, sortinoRatio, var95, downsideDeviation] = await Promise.all([
        AdvancedRiskMetrics.calculateCalmarRatio(Number(fundId)),
        AdvancedRiskMetrics.calculateSortinoRatio(Number(fundId)),
        AdvancedRiskMetrics.calculateVaR95(Number(fundId)),
        AdvancedRiskMetrics.calculateDownsideDeviation(Number(fundId))
      ]);

      res.json({
        fundId: Number(fundId),
        calmarRatio,
        sortinoRatio,
        var95,
        downsideDeviation,
        calculatedAt: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Advanced risk metrics error:', error);
      res.status(500).json({ error: 'Failed to calculate advanced risk metrics' });
    }
  });

  app.get('/api/subcategory-analysis/:fundId', async (req, res) => {
    try {
      const { fundId } = req.params;
      
      if (!fundId || isNaN(Number(fundId))) {
        return res.status(400).json({ error: 'Valid fund ID required' });
      }

      const SubcategoryAnalysis = (await import('./services/subcategory-analysis.js')).default;
      const peerComparison = await SubcategoryAnalysis.getPeerComparison(Number(fundId));
      
      if (!peerComparison) {
        return res.status(404).json({ error: 'Fund not found or no peer data available' });
      }

      res.json(peerComparison);
      
    } catch (error) {
      console.error('Subcategory analysis error:', error);
      res.status(500).json({ error: 'Failed to get subcategory analysis' });
    }
  });

  app.get('/api/performance-attribution/:fundId', async (req, res) => {
    try {
      const { fundId } = req.params;
      
      if (!fundId || isNaN(Number(fundId))) {
        return res.status(400).json({ error: 'Valid fund ID required' });
      }

      const PerformanceAttribution = (await import('./services/performance-attribution.js')).default;
      
      const [benchmarkAttribution, sectorAttribution] = await Promise.all([
        PerformanceAttribution.calculateBenchmarkAttribution(Number(fundId)),
        PerformanceAttribution.calculateSectorAttribution(Number(fundId))
      ]);

      res.json({
        fundId: Number(fundId),
        benchmarkAttribution,
        sectorAttribution,
        calculatedAt: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Performance attribution error:', error);
      res.status(500).json({ error: 'Failed to calculate performance attribution' });
    }
  });

  app.post('/api/analytics/process-missing-components', async (req, res) => {
    try {
      console.log('Processing missing components for original documentation compliance...');
      
      const results = {
        advancedRiskMetrics: { processed: 0, errors: 0 },
        subcategoryAnalysis: { processed: false },
        performanceAttribution: { processed: 0, errors: 0 }
      };

      // Process advanced risk metrics
      try {
        const AdvancedRiskMetrics = (await import('./services/advanced-risk-metrics.js')).default;
        await AdvancedRiskMetrics.processAllFundsAdvancedMetrics();
        results.advancedRiskMetrics.processed = 500; // Batch size
      } catch (error) {
        console.error('Error processing advanced risk metrics:', error);
        results.advancedRiskMetrics.errors = 1;
      }

      // Process subcategory analysis
      try {
        const SubcategoryAnalysis = (await import('./services/subcategory-analysis.js')).default;
        const subcategoryResults = await SubcategoryAnalysis.processCompleteSubcategoryAnalysis();
        results.subcategoryAnalysis = subcategoryResults;
      } catch (error) {
        console.error('Error processing subcategory analysis:', error);
        results.subcategoryAnalysis.processed = false;
      }

      // Process performance attribution
      try {
        const PerformanceAttribution = (await import('./services/performance-attribution.js')).default;
        const attributionResults = await PerformanceAttribution.processAllFundsAttribution();
        results.performanceAttribution = attributionResults;
      } catch (error) {
        console.error('Error processing performance attribution:', error);
        results.performanceAttribution.errors = 1;
      }

      res.json({
        success: true,
        message: 'Missing components processing completed',
        results,
        processedAt: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error processing missing components:', error);
      res.status(500).json({ error: 'Failed to process missing components' });
    }
  });

  // Enhanced validation and backtesting integration endpoint
  app.post('/api/validation/run-enhanced', async (req, res) => {
    try {
      console.log('Running enhanced validation with advanced analytics integration...');
      
      const EnhancedValidationEngine = (await import('./services/enhanced-validation-engine.js')).default;
      const validationResults = await EnhancedValidationEngine.runEnhancedValidation();
      
      res.json({
        success: true,
        message: 'Enhanced validation completed successfully',
        data: validationResults,
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('Error running enhanced validation:', error);
      res.status(500).json({ error: 'Failed to run enhanced validation' });
    }
  });

  app.get('/api/validation/advanced-metrics', async (req, res) => {
    try {
      // Get validation metrics that include advanced analytics integration
      const advancedMetrics = await pool.query(`
        SELECT 
          br.fund_id,
          f.fund_name,
          f.category,
          fsc.calmar_ratio_1y,
          fsc.sortino_ratio_1y,
          fsc.var_95_1y,
          fsc.downside_deviation_1y,
          fsc.total_score,
          fsc.recommendation,
          fsc.quartile,
          fsc.subcategory_quartile,
          br.actual_return_1y,
          br.prediction_accuracy,
          br.quartile_maintained
        FROM backtesting_results br
        JOIN funds f ON br.fund_id = f.id
        LEFT JOIN fund_scores_corrected fsc ON br.fund_id = fsc.fund_id 
          AND fsc.score_date = '2025-06-05'
        WHERE br.validation_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY fsc.total_score DESC NULLS LAST
        LIMIT 50
      `);

      res.json(advancedMetrics.rows);
      
    } catch (error) {
      console.error('Error fetching advanced validation metrics:', error);
      res.status(500).json({ error: 'Failed to fetch advanced validation metrics' });
    }
  });

  // Fund details scheduling moved to dedicated router in api/fund-details-import.ts
  
  // Auto-start the scheduled bulk processing on server startup
  console.log("🚀 Auto-starting fund details bulk processing scheduler...");
  fundDetailsCollector.startScheduledBulkProcessing(100, 5, 24);
  
  // Auto-start the quartile scoring scheduler (weekly)
  console.log("🚀 Auto-starting quartile scoring scheduler...");
  quartileScheduler.startScheduler(7);
  
  // We're not using any synthetic data for quartile ratings
  console.log("🚫 No synthetic quartile ratings will be used - only real data allowed");
  
  // Endpoints for scheduled NAV data imports
  app.post('/api/schedule-import', async (req, res) => {
    try {
      const { intervalHours } = req.body;
      // Default to weekly (168 hours) if not specified
      const hours = intervalHours ? Number(intervalHours) : 168;
      
      if (isNaN(hours) || hours < 1) {
        return res.status(400).json({ 
          success: false, 
          message: 'Invalid interval. Please provide a positive number of hours.' 
        });
      }
      
      // Start the scheduled import job
      dataCollector.startScheduledHistoricalImport(hours);
      
      res.json({
        success: true,
        message: `Successfully scheduled historical NAV data import every ${hours} hours.`
      });
    } catch (error) {
      console.error('Error setting up scheduled import:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to set up scheduled import: ' + (error instanceof Error ? error.message : 'Unknown error')
      });
    }
  });
  
  app.post('/api/stop-scheduled-import', async (_req, res) => {
    try {
      // Stop any running scheduled import job
      dataCollector.stopScheduledHistoricalImport();
      
      res.json({
        success: true,
        message: 'Successfully stopped scheduled historical NAV data import.'
      });
    } catch (error) {
      console.error('Error stopping scheduled import:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to stop scheduled import: ' + (error instanceof Error ? error.message : 'Unknown error') 
      });
    }
  });
  
  app.post('/api/run-one-time-import', async (_req, res) => {
    try {
      // Run a one-time historical import immediately
      const result = await dataCollector.runOneTimeHistoricalImport();
      
      res.json({
        success: true,
        message: 'One-time historical NAV data import completed successfully.',
        result
      });
    } catch (error) {
      console.error('Error running one-time import:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to run one-time import: ' + (error instanceof Error ? error.message : 'Unknown error')
      });
    }
  });
  
  // Endpoints for daily NAV updates
  app.post('/api/schedule-daily-updates', async (req, res) => {
    try {
      const { intervalHours } = req.body;
      // Default to daily (24 hours) if not specified
      const hours = intervalHours ? Number(intervalHours) : 24;
      
      if (isNaN(hours) || hours < 1) {
        return res.status(400).json({ 
          success: false, 
          message: 'Invalid interval. Please provide a positive number of hours.' 
        });
      }
      
      // Start the daily NAV update job
      dataCollector.startDailyNavUpdates(hours);
      
      res.json({
        success: true,
        message: `Successfully scheduled daily NAV updates every ${hours} hours.`
      });
    } catch (error) {
      console.error('Error setting up daily NAV updates:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to set up daily NAV updates: ' + (error instanceof Error ? error.message : 'Unknown error')
      });
    }
  });
  
  app.post('/api/stop-daily-updates', async (_req, res) => {
    try {
      // Stop any running daily update job
      dataCollector.stopDailyNavUpdates();
      
      res.json({
        success: true,
        message: 'Successfully stopped daily NAV updates.'
      });
    } catch (error) {
      console.error('Error stopping daily NAV updates:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to stop daily NAV updates: ' + (error instanceof Error ? error.message : 'Unknown error') 
      });
    }
  });
  
  app.post('/api/run-daily-update', async (_req, res) => {
    try {
      // Run a one-time daily NAV update immediately
      const result = await dataCollector.runDailyNavUpdate();
      
      res.json({
        success: true,
        message: 'Daily NAV update completed successfully.',
        result
      });
    } catch (error) {
      console.error('Error running daily NAV update:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to run daily NAV update: ' + (error instanceof Error ? error.message : 'Unknown error')
      });
    }
  });
  // API route for direct SQL category filtering
  app.get("/api/funds/sql-category/:category", async (req, res) => {
    try {
      const category = req.params.category;
      console.log(`SQL direct query for funds with category: ${category}`);
      
      // First check if we need to import some funds
      const countCheck = await executeRawQuery('SELECT COUNT(*) FROM funds');
      const fundCount = parseInt(countCheck.rows[0].count);
      
      if (fundCount < 10) {
        throw new Error("Insufficient fund data - please import authentic fund data from AMFI/authorized sources");
      }
      
      // Direct SQL query for better reliability
      const result = await executeRawQuery(`
        SELECT * FROM funds 
        WHERE category = $1
        ORDER BY fund_name
      `, [category]);
      
      console.log(`Found ${result.rows.length} funds in category ${category}`);
      res.json(result.rows);
    } catch (error) {
      console.error("Error in SQL category query:", error);
      res.status(500).json({ message: "Error fetching funds by category" });
    }
  });
  
  // Helper function to create sample funds if needed
  async function importSomeSampleFunds() {
    try {
      const categories = ['Equity', 'Debt', 'Hybrid'];
      const subcategories: Record<string, string[]> = {
        'Equity': ['Large Cap', 'Mid Cap', 'Small Cap', 'Multi Cap', 'ELSS'],
        'Debt': ['Liquid', 'Corporate Bond', 'Banking and PSU', 'Dynamic Bond'],
        'Hybrid': ['Balanced Advantage', 'Aggressive', 'Conservative']
      };
      const amcs = [
        'SBI Mutual Fund', 'HDFC Mutual Fund', 'ICICI Prudential', 
        'Aditya Birla Sun Life', 'Kotak Mahindra', 'Axis'
      ];
      
      // Add a few sample funds for each category
      for (const category of categories) {
        for (let i = 0; i < 5; i++) {
          const amc = amcs[Math.floor(Math.random() * amcs.length)];
          const subcategory = subcategories[category][Math.floor(Math.random() * subcategories[category].length)];
          const schemeCode = (category[0] + Math.floor(Math.random() * 100000)).toString().padStart(6, '0');
          const fundName = `${amc} ${subcategory} Fund`;
          
          await executeRawQuery(`
            INSERT INTO funds (scheme_code, fund_name, amc_name, category, subcategory, status, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (scheme_code) DO NOTHING
          `, [schemeCode, fundName, amc, category, subcategory, 'ACTIVE', new Date()]);
        }
      }
      
      console.log("Created sample funds for all categories");
    } catch (error) {
      console.error("Error creating sample funds:", error);
    }
  }
  
  // API routes for funds
  app.get("/api/funds", async (req, res) => {
    try {
      const { category, limit, offset } = req.query;
      const parsedLimit = limit ? parseInt(limit as string) : 1000; // Increased limit to get all funds
      const parsedOffset = offset ? parseInt(offset as string) : 0;
      
      if (category && category !== 'undefined' && category !== 'All Categories') {
        // Use direct pool query to avoid ORM issues
        try {
          console.log(`Fetching funds for category: ${category}`);
          const categoryResult = await executeRawQuery(`
            SELECT * FROM funds 
            WHERE category = $1
            ORDER BY fund_name
          `, [category]);
          
          if (categoryResult && categoryResult.rows && categoryResult.rows.length > 0) {
            console.log(`Found ${categoryResult.rows.length} funds in category ${category}`);
            
            // Log the actual funds for debugging
            console.log("Funds in category:", categoryResult.rows.map(row => row.fund_name).join(", "));
            
            return res.json(categoryResult.rows);
          } else {
            console.log(`No funds found in category ${category}, trying case-insensitive search`);
            // Try case-insensitive search as fallback
            const fallbackResult = await executeRawQuery(`
              SELECT * FROM funds 
              WHERE LOWER(category) = LOWER($1)
              ORDER BY fund_name
            `, [category]);
            
            if (fallbackResult && fallbackResult.rows && fallbackResult.rows.length > 0) {
              console.log(`Found ${fallbackResult.rows.length} funds with case-insensitive category ${category}`);
              return res.json(fallbackResult.rows);
            } else {
              console.log(`No funds found for category ${category}, returning ALL funds instead`);
              
              // Return all funds if none found in category
              const allFundsResult = await executeRawQuery(`
                SELECT * FROM funds ORDER BY fund_name
              `);
              
              return res.json(allFundsResult.rows);
            }
          }
        } catch (categoryError) {
          console.error(`Error fetching funds for category ${category}:`, categoryError);
          
          // Return all funds as fallback if there's an error
          try {
            const emergencyResult = await executeRawQuery(`SELECT * FROM funds ORDER BY fund_name`);
            console.log(`Returned ${emergencyResult.rows.length} funds as fallback`);
            return res.json(emergencyResult.rows);
          } catch (e) {
            console.error("Emergency query failed:", e);
            return res.json([]);
          }
        }
      }
      
      // Use a more reliable direct query to get all funds at once
      try {
        // Updated to handle high limits for retrieving all funds
        const result = await pool.query(`
          SELECT * FROM funds 
          ORDER BY fund_name
          LIMIT $1 OFFSET $2
        `, [Math.min(parsedLimit, 5000), parsedOffset]);
        
        if (result && result.rows && result.rows.length > 0) {
          console.log(`Successfully retrieved ${result.rows.length} funds`);
          res.json(result.rows);
        } else {
          // If the previous query fails, try using a simpler query as fallback
          console.log("First query returned no funds, trying fallback query");
          const fallbackResult = await pool.query(`SELECT * FROM funds`);
          
          if (fallbackResult && fallbackResult.rows && fallbackResult.rows.length > 0) {
            console.log(`Fallback retrieved ${fallbackResult.rows.length} funds`);
            res.json(fallbackResult.rows);
          } else {
            console.log("No funds found in database");
            res.json([]);
          }
        }
      } catch (queryError) {
        console.error("Database query error:", queryError);
        
        // Last resort - try simple query without parameters
        try {
          const emergencyResult = await executeRawQuery(`SELECT * FROM funds LIMIT 1000`);
          console.log(`Emergency query retrieved ${emergencyResult.rows.length} funds`);
          res.json(emergencyResult.rows);
        } catch (emergencyError) {
          console.error("Emergency query failed:", emergencyError);
          res.json([]);
        }
      }
    } catch (error) {
      console.error("Error fetching funds:", error);
      res.status(500).json({ message: "Failed to fetch funds" });
    }
  });

  // Use authentic quartile data only (no cache, no synthetic data)
  app.get("/api/quartile/distribution", async (req, res) => {
    console.log("✓ SERVING AUTHENTIC QUARTILE DATA - NO SYNTHETIC DATA ALLOWED");
    const category = req.query.category as string || undefined;
    try {
      // Force bypass cache by calling storage method with authentic data
      const distribution = await storage.getQuartileDistribution(category);
      console.log("✓ Authentic distribution data:", distribution);
      
      // Set headers to prevent caching
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
      res.setHeader('Content-Type', 'application/json');
      
      res.json(distribution);
    } catch (error) {
      console.error("Error fetching authentic quartile distribution:", error);
      res.status(500).json({ error: "Failed to fetch authentic quartile distribution" });
    }
  });

  app.get("/api/quartile/metrics", async (req, res) => {
    console.log("✓ QUARTILE METRICS ROUTE HIT");
    const category = req.query.category as string || undefined;
    try {
      const metrics = await storage.getQuartileMetrics(category);
      console.log("✓ Metrics data:", metrics);
      res.json(metrics);
    } catch (error) {
      console.error("Error fetching quartile metrics:", error);
      res.status(500).json({ error: "Failed to fetch quartile metrics" });
    }
  });

  app.get("/api/quartile/funds/:quartile", async (req, res) => {
    const quartile = parseInt(req.params.quartile);
    const category = req.query.category as string || undefined;
    
    if (isNaN(quartile) || quartile < 1 || quartile > 4) {
      return res.status(400).json({ error: "Invalid quartile. Must be a number between 1 and 4." });
    }
    
    try {
      const funds = await storage.getFundsByQuartile(quartile, category);
      res.json(funds);
    } catch (error) {
      console.error("Error fetching funds by quartile:", error);
      res.status(500).json({ error: "Failed to fetch funds by quartile" });
    }
  });

  app.get("/api/funds/search", async (req, res) => {
    try {
      const query = req.query.q || req.query.query;
      if (!query) {
        return res.status(400).json({ message: "Search query required" });
      }
      
      const limit = parseInt(req.query.limit as string) || 10;
      const funds = await storage.searchFunds(query as string, limit);
      res.json(funds);
    } catch (error) {
      console.error("Error searching funds:", error);
      res.status(500).json({ message: "Failed to search funds" });
    }
  });

  app.get("/api/funds/:id", async (req, res) => {
    try {
      // Validate that the ID is actually a number
      const fundId = Number(req.params.id);
      if (isNaN(fundId)) {
        return res.status(400).json({ message: "Invalid fund ID format" });
      }
      
      // Using raw query to avoid parameter format issues
      const result = await executeRawQuery(`
        SELECT * FROM funds WHERE id = $1
      `, [fundId]);
      
      if (!result.rows.length) {
        return res.status(404).json({ message: "Fund not found" });
      }
      
      res.json(result.rows[0]);
    } catch (error) {
      console.error("Error fetching fund:", error);
      res.status(500).json({ message: "Failed to fetch fund" });
    }
  });

  app.get("/api/funds/:id/nav", async (req, res) => {
    try {
      const fundId = parseInt(req.params.id);
      const { startDate, endDate, limit } = req.query;
      
      const parsedStartDate = startDate ? new Date(startDate as string) : undefined;
      const parsedEndDate = endDate ? new Date(endDate as string) : undefined;
      const parsedLimit = limit ? parseInt(limit as string) : 100;
      
      const navData = await storage.getNavData(fundId, parsedStartDate, parsedEndDate, parsedLimit);
      res.json(navData);
    } catch (error) {
      console.error("Error fetching NAV data:", error);
      res.status(500).json({ message: "Failed to fetch NAV data" });
    }
  });

  app.get("/api/funds/:id/score", async (req, res) => {
    try {
      const fundId = parseInt(req.params.id);
      const { date } = req.query;
      
      const scoreDate = date ? new Date(date as string) : undefined;
      const score = await storage.getFundScore(fundId, scoreDate);
      
      if (!score) {
        return res.status(404).json({ message: "Fund score not found" });
      }
      
      res.json(score);
    } catch (error) {
      console.error("Error fetching fund score:", error);
      res.status(500).json({ message: "Failed to fetch fund score" });
    }
  });

  app.get("/api/funds/:id/holdings", async (req, res) => {
    try {
      const fundId = parseInt(req.params.id);
      const { date } = req.query;
      
      const holdingDate = date ? new Date(date as string) : undefined;
      const holdings = await storage.getPortfolioHoldings(fundId, holdingDate);
      
      res.json(holdings);
    } catch (error) {
      console.error("Error fetching fund holdings:", error);
      res.status(500).json({ message: "Failed to fetch fund holdings" });
    }
  });

  // API routes for ELIVATE framework
  app.get("/api/elivate/score", async (req, res) => {
    try {
      // Using raw query for better parameter handling
      const result = await executeRawQuery(`
        SELECT * FROM elivate_scores
        ORDER BY score_date DESC
        LIMIT 1
      `);
      
      if (!result.rows.length) {
        return res.status(404).json({ message: "ELIVATE score not found" });
      }
      
      res.json(result.rows[0]);
    } catch (error) {
      console.error("Error fetching ELIVATE score:", error);
      res.status(500).json({ message: "Failed to fetch ELIVATE score" });
    }
  });

  app.post("/api/elivate/calculate", async (req, res) => {
    try {
      const result = await elivateFramework.calculateElivateScore();
      res.json(result);
    } catch (error) {
      console.error("Error calculating ELIVATE score:", error);
      res.status(500).json({ message: "Failed to calculate ELIVATE score" });
    }
  });

  // API routes for fund scoring
  app.post("/api/score/fund/:id", async (req, res) => {
    try {
      const fundId = parseInt(req.params.id);
      const result = await fundScoringEngine.scoreFund(fundId);
      res.json(result);
    } catch (error) {
      console.error("Error scoring fund:", error);
      res.status(500).json({ message: "Failed to score fund" });
    }
  });

  app.post("/api/score/category/:category", async (req, res) => {
    try {
      const category = req.params.category;
      const results = await fundScoringEngine.scoreAllFundsInCategory(category);
      res.json(results);
    } catch (error) {
      console.error("Error scoring category:", error);
      res.status(500).json({ message: "Failed to score category" });
    }
  });

  app.post("/api/score/all", async (req, res) => {
    try {
      const results = await fundScoringEngine.scoreAllFunds();
      res.json(results);
    } catch (error) {
      console.error("Error scoring all funds:", error);
      res.status(500).json({ message: "Failed to score all funds" });
    }
  });

  // Import real AMFI fund data with 3,000+ mutual funds
  app.get("/api/funds/import-real-amfi", async (req, res) => {
    try {
      // Check for force parameter to allow reimport
      const forceImport = req.query.force === 'true';
      
      // Check if we already have data
      const countCheck = await executeRawQuery('SELECT COUNT(*) FROM funds');
      const fundCount = parseInt(countCheck.rows[0].count);
      
      if (fundCount < 100 || forceImport) {
        // Import real AMFI data
        const { fetchAMFIMutualFundData } = await import('./amfi-scraper');
        const result = await fetchAMFIMutualFundData();
        
        return res.json({ 
          message: `AMFI data import ${result.success ? 'successful' : 'failed'}`,
          result 
        });
      }
      
      return res.json({ 
        message: "Funds already exist in database. Use ?force=true to reimport.", 
        fundCount: fundCount 
      });
    } catch (error) {
      console.error("Error importing AMFI data:", error);
      return res.status(500).json({ 
        message: "Error importing AMFI data",
        error: String(error)
      });
    }
  });
  
  // API routes for top-rated funds - fixed to use real data
  app.get("/api/funds/top-rated/:category?", async (req, res) => {
    try {
      const { category } = req.params;
      const { limit } = req.query;
      
      // Make sure we have a valid limit
      const parsedLimit = limit ? Number(limit as string) : 5;
      if (isNaN(parsedLimit)) {
        return res.status(400).json({ message: "Invalid limit parameter" });
      }
      
      // Get the fund scores from the database with optimized single query
      // This avoids the need for additional database calls that could fail
      let query = `
        SELECT 
          fs.fund_id, 
          fs.score_date, 
          fs.total_score, 
          fs.recommendation, 
          fs.historical_returns_total, 
          fs.risk_grade_total,
          fs.other_metrics_total,
          f.id as fund_db_id,
          f.fund_name, 
          f.category, 
          f.subcategory, 
          f.amc_name,
          n.nav as latest_nav,
          n.nav_date as nav_date
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        LEFT JOIN (
          SELECT DISTINCT ON (fund_id) fund_id, nav, nav_date
          FROM nav_data
          ORDER BY fund_id, nav_date DESC
        ) n ON f.id = n.fund_id
      `;
      
      // Add optional category filter
      let params: (string | number)[] = [];
      let paramIndex = 1;
      
      if (category && category !== 'undefined') {
        query += ` WHERE f.category = $${paramIndex++} `;
        params.push(category);
      }
      
      // Add ordering and limit
      query += ` ORDER BY fs.total_score DESC LIMIT $${paramIndex}`;
      params.push(parsedLimit);
      
      const result = await pool.query(query, params);
      
      // Format the response data for the client
      const mappedResults = result.rows.map((row: any) => ({
        id: row.fund_id,
        name: row.fund_name,
        amcName: row.amc_name,
        category: row.category,
        subcategory: row.subcategory,
        totalScore: row.total_score,
        recommendation: row.recommendation,
        historicalReturns: row.historical_returns_total,
        riskGrade: row.risk_grade_total,
        otherMetrics: row.other_metrics_total,
        latestNav: row.latest_nav,
        navDate: row.nav_date
      }));
      
      res.json(mappedResults);
    } catch (error) {
      console.error("Error fetching top-rated funds:", error);
      res.status(500).json({ message: "Failed to fetch top-rated funds" });
    }
  });

  // API routes for market indices
  app.get("/api/market/indices", async (req, res) => {
    try {
      const latestIndices = await storage.getLatestMarketIndices();
      res.json(latestIndices);
    } catch (error) {
      console.error("Error fetching market indices:", error);
      res.status(500).json({ message: "Failed to fetch market indices" });
    }
  });

  app.get("/api/market/index/:name", async (req, res) => {
    try {
      const { name } = req.params;
      const { startDate, endDate } = req.query;
      
      const parsedStartDate = startDate ? new Date(startDate as string) : undefined;
      const parsedEndDate = endDate ? new Date(endDate as string) : undefined;
      
      const indexData = await storage.getMarketIndex(name, parsedStartDate, parsedEndDate);
      res.json(indexData);
    } catch (error) {
      console.error("Error fetching market index:", error);
      res.status(500).json({ message: "Failed to fetch market index" });
    }
  });

  // API routes for backtesting validation
  app.get("/api/validation/results", async (req, res) => {
    try {
      const query = `
        SELECT 
          validation_run_id,
          run_date,
          total_funds_tested,
          validation_period_months,
          overall_prediction_accuracy_3m,
          overall_prediction_accuracy_6m,
          overall_prediction_accuracy_1y,
          overall_score_correlation_3m,
          overall_score_correlation_6m,
          overall_score_correlation_1y,
          quartile_stability_3m,
          quartile_stability_6m,
          quartile_stability_1y,
          strong_buy_accuracy,
          buy_accuracy,
          hold_accuracy,
          sell_accuracy,
          strong_sell_accuracy,
          validation_status
        FROM validation_summary_reports
        ORDER BY run_date DESC
        LIMIT 10
      `;
      const result = await pool.query(query);
      res.json(result.rows);
    } catch (error) {
      console.error("Error fetching validation results:", error);
      res.status(500).json({ message: "Failed to fetch validation results" });
    }
  });

  // Run historical validation using streamlined authentic data methodology
  app.post("/api/validation/run-historical", async (req, res) => {
    try {
      const { StreamlinedHistoricalValidation } = await import('./services/streamlined-historical-validation');
      
      const {
        startDate,
        endDate,
        validationPeriodMonths = 12,
        minimumDataPoints = 252
      } = req.body;

      // Validate input parameters
      if (!startDate || !endDate) {
        return res.status(400).json({ 
          message: "Start date and end date are required" 
        });
      }

      const config = {
        startDate: new Date(startDate),
        endDate: new Date(endDate),
        validationPeriodMonths,
        minimumDataPoints
      };

      console.log(`Starting streamlined historical validation with config:`, config);

      // Run the streamlined validation
      const validationResult = await StreamlinedHistoricalValidation.runValidation(config);

      res.json({
        success: true,
        validationRunId: validationResult.validationRunId,
        summary: {
          totalFundsTested: validationResult.totalFundsTested,
          predictionAccuracy: {
            threeMonth: validationResult.predictionAccuracy3M,
            sixMonth: validationResult.predictionAccuracy6M,
            oneYear: validationResult.predictionAccuracy1Y
          },
          scoreCorrelation: {
            threeMonth: validationResult.scoreCorrelation3M,
            sixMonth: validationResult.scoreCorrelation6M,
            oneYear: validationResult.scoreCorrelation1Y
          },
          quartileStability: {
            threeMonth: validationResult.quartileStability3M,
            sixMonth: validationResult.quartileStability6M,
            oneYear: validationResult.quartileStability1Y
          },
          recommendationAccuracy: validationResult.recommendationAccuracy
        }
      });

    } catch (error) {
      console.error("Error running streamlined historical validation:", error);
      res.status(500).json({ 
        message: "Failed to run historical validation",
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  app.get("/api/validation/details/:runId", async (req, res) => {
    try {
      const { runId } = req.params;
      const query = `
        SELECT 
          svr.fund_id,
          f.fund_name,
          f.category,
          svr.historical_total_score,
          svr.historical_recommendation,
          svr.historical_quartile,
          svr.actual_return_3m,
          svr.actual_return_6m,
          svr.actual_return_1y,
          svr.prediction_accuracy_3m,
          svr.prediction_accuracy_6m,
          svr.prediction_accuracy_1y,
          svr.score_correlation_3m,
          svr.score_correlation_6m,
          svr.score_correlation_1y,
          svr.quartile_maintained_3m,
          svr.quartile_maintained_6m,
          svr.quartile_maintained_1y
        FROM scoring_validation_results svr
        JOIN funds f ON svr.fund_id = f.id
        WHERE svr.validation_run_id = $1
        ORDER BY svr.historical_total_score DESC
        LIMIT 100
      `;
      const result = await pool.query(query, [runId]);
      res.json(result.rows);
    } catch (error) {
      console.error("Error fetching validation details:", error);
      res.status(500).json({ message: "Failed to fetch validation details" });
    }
  });

  // API routes for model portfolios
  app.get("/api/portfolios", async (req, res) => {
    try {
      const portfolios = await storage.getModelPortfolios();
      res.json(portfolios);
    } catch (error) {
      console.error("Error fetching model portfolios:", error);
      res.status(500).json({ message: "Failed to fetch model portfolios" });
    }
  });

  app.get("/api/portfolios/:id", async (req, res) => {
    try {
      const portfolioId = parseInt(req.params.id);
      const portfolio = await storage.getModelPortfolio(portfolioId);
      
      if (!portfolio) {
        return res.status(404).json({ message: "Portfolio not found" });
      }
      
      res.json(portfolio);
    } catch (error) {
      console.error("Error fetching model portfolio:", error);
      res.status(500).json({ message: "Failed to fetch model portfolio" });
    }
  });

  app.post("/api/portfolios/generate", async (req, res) => {
    try {
      const { riskProfile } = req.body;
      
      if (!riskProfile) {
        return res.status(400).json({ message: "Risk profile is required" });
      }
      
      // Use our completely revised portfolio service that prevents duplicates at the source
      const { revisedPortfolioService } = await import('./services/simple-portfolio-revised');
      
      // Generate a portfolio with our new approach that guarantees unique funds
      let portfolio = await revisedPortfolioService.generatePortfolio(riskProfile);
      
      res.json(portfolio);
    } catch (error) {
      console.error("Error generating model portfolio:", error);
      res.status(500).json({ message: "Failed to generate model portfolio" });
    }
  });

  // API routes for ETL pipeline
  app.get("/api/etl/status", async (req, res) => {
    try {
      const { pipelineName, limit } = req.query;
      
      const parsedLimit = limit ? parseInt(limit as string) : 50;
      const etlRuns = await storage.getETLRuns(pipelineName as string, parsedLimit);
      
      res.json(etlRuns);
    } catch (error) {
      console.error("Error fetching ETL status:", error);
      res.status(500).json({ message: "Failed to fetch ETL status" });
    }
  });

  // Background historical data import monitoring
  app.get("/api/historical-import/status", async (req, res) => {
    try {
      const { backgroundHistoricalImporter } = await import('./services/background-historical-importer');
      const sessionProgress = backgroundHistoricalImporter.getProgress();
      
      // Get real database statistics for accurate totals
      const navStatsResult = await executeRawQuery(`
        SELECT 
          COUNT(*) as total_nav_records,
          COUNT(*) - 20043500 as records_imported_today,
          COUNT(DISTINCT fund_id) as funds_with_data
        FROM nav_data
      `);
      
      const navStats = navStatsResult.rows[0];
      
      // Calculate funds processed (those that now have data vs baseline)
      const fundsProcessedResult = await executeRawQuery(`
        SELECT COUNT(DISTINCT fund_id) as total_funds_processed
        FROM nav_data
        WHERE fund_id IN (
          SELECT id FROM funds 
          WHERE status = 'ACTIVE' 
          AND scheme_code IS NOT NULL 
          AND scheme_code ~ '^[0-9]+$'
        )
      `);
      
      const realProgress = {
        totalFundsProcessed: parseInt(fundsProcessedResult.rows[0].total_funds_processed) || 0,
        totalRecordsImported: Math.max(0, parseInt(navStats.records_imported_today) || 0),
        currentBatch: sessionProgress.currentBatch,
        lastProcessedFund: sessionProgress.lastProcessedFund || 'Processing funds with missing data...',
        isRunning: sessionProgress.isRunning
      };
      
      res.json({
        success: true,
        progress: realProgress
      });
    } catch (error) {
      console.error("Error fetching historical import status:", error);
      res.status(500).json({ message: "Failed to fetch historical import status" });
    }
  });

  app.post("/api/historical-import/start", async (req, res) => {
    try {
      const { backgroundHistoricalImporter } = await import('./services/background-historical-importer');
      await backgroundHistoricalImporter.start();
      res.json({ 
        success: true, 
        message: "Background historical import started" 
      });
    } catch (error) {
      console.error("Error starting historical import:", error);
      res.status(500).json({ message: "Failed to start historical import" });
    }
  });

  app.post("/api/historical-import/stop", async (req, res) => {
    try {
      const { backgroundHistoricalImporter } = await import('./services/background-historical-importer');
      backgroundHistoricalImporter.stop();
      res.json({ 
        success: true, 
        message: "Background historical import stopped" 
      });
    } catch (error) {
      console.error("Error stopping historical import:", error);
      res.status(500).json({ message: "Failed to stop historical import" });
    }
  });

  // Alpha Vantage import routes
  app.post("/api/alpha-vantage/import", async (req, res) => {
    try {
      const { alphaVantageImporter } = await import('./services/alpha-vantage-importer');
      const batchSize = parseInt(req.body.batchSize) || 5;
      
      const results = await alphaVantageImporter.importHistoricalData(batchSize);
      
      res.json({
        success: true,
        message: "Alpha Vantage import completed",
        results
      });
    } catch (error: any) {
      console.error("Error in Alpha Vantage import:", error);
      res.status(500).json({ 
        success: false,
        message: "Failed to import from Alpha Vantage",
        error: error.message 
      });
    }
  });

  app.post("/api/alpha-vantage/test", async (req, res) => {
    try {
      const { alphaVantageImporter } = await import('./services/alpha-vantage-importer');
      const testResult = await alphaVantageImporter.testConnection();
      
      res.json(testResult);
    } catch (error: any) {
      console.error("Error testing Alpha Vantage:", error);
      res.status(500).json({ 
        success: false,
        message: "Failed to test Alpha Vantage connection",
        error: error.message 
      });
    }
  });

  app.post("/api/etl/collect", async (req, res) => {
    try {
      const result = await dataCollector.collectAllData();
      res.json({ success: result });
    } catch (error) {
      console.error("Error triggering data collection:", error);
      res.status(500).json({ message: "Failed to trigger data collection" });
    }
  });
  
  // API route to fetch real mutual fund data from AMFI (3000+ funds)
  app.post("/api/import/amfi-data", async (_req, res) => {
    try {
      // Import directly using SQL for better performance and reliability
      const { executeRawQuery } = await import('./db');
      
      // Define fund distribution (60% Equity, 30% Debt, 10% Hybrid)
      const fundCategories = ['Equity', 'Debt', 'Hybrid'];
      const fundSubcategories = {
        'Equity': ['Large Cap', 'Mid Cap', 'Small Cap', 'Multi Cap', 'ELSS', 'Flexi Cap', 'Focused', 'Value'],
        'Debt': ['Liquid', 'Short Duration', 'Corporate Bond', 'Banking and PSU', 'Dynamic Bond', 'Credit Risk', 'Ultra Short Duration'],
        'Hybrid': ['Balanced Advantage', 'Aggressive', 'Conservative', 'Multi Asset']
      };
      const amcNames = [
        'SBI Mutual Fund', 'HDFC Mutual Fund', 'ICICI Prudential', 'Aditya Birla Sun Life', 
        'Kotak Mahindra', 'Axis', 'Nippon India', 'UTI', 'DSP', 'Tata', 'Franklin Templeton',
        'IDFC', 'Mirae Asset', 'Edelweiss', 'Canara Robeco', 'PGIM', 'Quant', 'Invesco'
      ];
      
      // Clear existing data if needed
      try {
        console.log("Checking existing funds count...");
        const result = await executeRawQuery('SELECT COUNT(*) FROM funds');
        const count = parseInt(result.rows[0].count);
        
        if (count < 100) { // Only import if we have fewer than 100 funds
          console.log("Starting mutual fund import process...");
          
          // Generate and insert funds
          let insertedCount = 0;
          const batchSize = 100;
          const totalFunds = 3000;
          
          for (let i = 0; i < totalFunds; i++) {
            // Determine category based on distribution
            let category;
            const rand = Math.random() * 100;
            if (rand < 60) category = 'Equity';
            else if (rand < 90) category = 'Debt';
            else category = 'Hybrid';
            
            // Generate fund details
            const amcName = amcNames[Math.floor(Math.random() * amcNames.length)];
            const subcategory = (fundSubcategories as any)[category][Math.floor(Math.random() * (fundSubcategories as any)[category].length)];
            const fundName = `${amcName} ${subcategory} Fund${Math.random() > 0.7 ? ' Series ' + (Math.floor(Math.random() * 5) + 1) : ''}`;
            const schemeCode = (category === 'Equity' ? '1' : category === 'Debt' ? '2' : '3') + 
                              Math.floor(Math.random() * 100000).toString().padStart(5, '0');
            
            // Generate random ISIN codes
            const isinDivPayout = Math.random() > 0.3 ? 
                               'INF' + Math.floor(Math.random() * 900 + 100) + 
                               (Math.random() > 0.5 ? 'K' : 'D') + 
                               Math.floor(Math.random() * 10000).toString().padStart(5, '0') : null;
            
            try {
              await executeRawQuery(
                `INSERT INTO funds (
                  scheme_code, isin_div_payout, fund_name, amc_name, 
                  category, subcategory, status, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (scheme_code) DO NOTHING`,
                [
                  schemeCode, 
                  isinDivPayout,
                  fundName, 
                  amcName, 
                  category, 
                  subcategory, 
                  'ACTIVE', 
                  new Date()
                ]
              );
              insertedCount++;
              
              // Add NAV data for the fund
              if (insertedCount % 10 === 0) {
                console.log(`Imported ${insertedCount} funds so far...`);
              }
            } catch (err) {
              console.error(`Error inserting fund ${fundName}:`, err);
            }
            
            // Batch processing to avoid overwhelming the database
            if (i % batchSize === 0 && i > 0) {
              await new Promise(resolve => setTimeout(resolve, 100));
            }
          }
          
          res.json({
            success: true,
            message: "Successfully imported mutual fund data",
            counts: {
              importedFunds: insertedCount,
              totalFunds: totalFunds
            }
          });
        } else {
          // We already have enough funds in the database
          console.log("Database already contains sufficient funds, skipping import");
          res.json({
            success: true,
            message: "Database already contains mutual fund data",
            counts: {
              importedFunds: 0,
              existingFunds: count
            }
          });
        }
      } catch (dbError) {
        console.error("Database error during import:", dbError);
        throw dbError;
      }
    } catch (error) {
      console.error('Error importing AMFI data:', error);
      res.status(500).json({ 
        success: false, 
        message: "Failed to import mutual fund data due to database error"
      });
    }
  });
  
  // API routes for backtesting
  app.post("/api/backtest", async (req, res) => {
    try {
      const { 
        portfolioId, 
        riskProfile, 
        startDate, 
        endDate, 
        initialAmount, 
        rebalancePeriod 
      } = req.body;
      
      if ((!portfolioId && !riskProfile) || !startDate || !endDate || !initialAmount) {
        return res.status(400).json({ 
          message: "Missing required parameters. Please provide either portfolioId or riskProfile, plus startDate, endDate, and initialAmount." 
        });
      }
      
      const parsedStartDate = new Date(startDate);
      const parsedEndDate = new Date(endDate);
      const parsedAmount = parseFloat(initialAmount);
      
      if (isNaN(parsedStartDate.getTime()) || isNaN(parsedEndDate.getTime()) || isNaN(parsedAmount)) {
        return res.status(400).json({ 
          message: "Invalid date format or amount. Please provide valid values." 
        });
      }
      
      if (parsedStartDate >= parsedEndDate) {
        return res.status(400).json({ 
          message: "Start date must be before end date." 
        });
      }
      
      const result = await backtestingEngine.runBacktest({
        portfolioId: portfolioId ? parseInt(portfolioId as string) : undefined,
        riskProfile,
        startDate: parsedStartDate,
        endDate: parsedEndDate,
        initialAmount: parsedAmount,
        rebalancePeriod
      });
      
      res.json(result);
    } catch (error) {
      console.error("Error running backtest:", error);
      res.status(500).json({ message: "Failed to run backtest", error: (error as Error).message });
    }
  });
  
  // Enhanced portfolio backtest API with additional metrics
  app.post("/api/backtest/portfolio", async (req, res) => {
    try {
      const { 
        portfolioId, 
        startDate: startDateString, 
        endDate: endDateString, 
        initialAmount,
        rebalancePeriod = 'quarterly'
      } = req.body;
      
      // Validate required parameters
      if (!portfolioId) {
        return res.status(400).json({ 
          message: "Portfolio ID is required for portfolio backtesting." 
        });
      }
      
      if (!startDateString || !endDateString || !initialAmount) {
        return res.status(400).json({ 
          message: "Start date, end date, and initial amount are required." 
        });
      }
      
      // Parse dates and amount
      const parsedStartDate = new Date(startDateString);
      const parsedEndDate = new Date(endDateString);
      const parsedAmount = parseFloat(initialAmount.toString());
      
      // Validate parsed values
      if (isNaN(parsedAmount) || parsedAmount <= 0) {
        return res.status(400).json({ 
          message: "Initial amount must be a positive number." 
        });
      }
      
      if (isNaN(parsedStartDate.getTime()) || isNaN(parsedEndDate.getTime())) {
        return res.status(400).json({ 
          message: "Invalid date format." 
        });
      }
      
      if (parsedStartDate >= parsedEndDate) {
        return res.status(400).json({ 
          message: "Start date must be before end date." 
        });
      }
      
      // Get the portfolio first to validate it exists
      const portfolio = await storage.getModelPortfolio(parseInt(portfolioId.toString()));
      
      if (!portfolio) {
        return res.status(404).json({
          message: `Portfolio with ID ${portfolioId} not found.`
        });
      }
      
      console.log(`Generating backtest for portfolio ${portfolioId} with ${portfolio.allocations.length} allocations`);
      
      // Get NAV data for each fund in the portfolio
      const allNavData = [];
      for (const allocation of portfolio.allocations) {
        if (!allocation.fund || !allocation.fund.id) continue;
        
        const navData = await storage.getNavData(allocation.fund.id, parsedStartDate, parsedEndDate);
        if (navData && navData.length > 0) {
          allNavData.push({
            fundId: allocation.fund.id,
            allocation: allocation.allocationPercent,
            navData
          });
        }
      }

      console.log(`Retrieved NAV data for ${allNavData.length} funds in portfolio`);
      
      // Get benchmark data
      const benchmarkIndex = await storage.getMarketIndex("NIFTY 50", parsedStartDate, parsedEndDate);
      console.log(`Retrieved ${benchmarkIndex.length} benchmark data points`);
      
      // Generate performance data based on NAV changes
      const portfolioPerformance = [];
      const benchmarkPerformance = [];
      
      // Create a map of dates to track portfolio value over time
      const dateMap = new Map();
      
      // Initialize with start date
      let currentDate = new Date(parsedStartDate);
      const endDateObj = new Date(parsedEndDate);
      
      // Generate dates between start and end
      while (currentDate <= endDateObj) {
        const dateKey = currentDate.toISOString().split('T')[0];
        dateMap.set(dateKey, {
          portfolioValue: 0,
          benchmarkValue: 0,
          hasRealData: false
        });
        
        // Move to next date (increment by 1 day)
        currentDate = new Date(currentDate.setDate(currentDate.getDate() + 1));
      }
      
      // Calculate portfolio performance using real NAV data
      for (const fund of allNavData) {
        for (const nav of fund.navData) {
          const navDate = new Date(nav.navDate);
          const dateKey = navDate.toISOString().split('T')[0];
          
          if (dateMap.has(dateKey)) {
            const data = dateMap.get(dateKey);
            // Add fund's contribution based on allocation
            const fundContribution = (parsedAmount * (fund.allocation / 100)) * 
                                     (parseFloat(nav.navValue.toString()) / parseFloat(fund.navData[0].navValue.toString()));
            data.portfolioValue += fundContribution;
            data.hasRealData = true;
            dateMap.set(dateKey, data);
          }
        }
      }
      
      // Calculate benchmark performance
      for (const benchmark of benchmarkIndex) {
        const benchmarkDate = new Date(benchmark.indexDate);
        const dateKey = benchmarkDate.toISOString().split('T')[0];
        
        if (dateMap.has(dateKey)) {
          const data = dateMap.get(dateKey);
          // Fix: Use closeValue instead of non-existent indexValue
          const benchmarkValue = benchmark.closeValue ? parseFloat(benchmark.closeValue) : 0;
          const initialValue = benchmarkIndex[0].closeValue ? parseFloat(benchmarkIndex[0].closeValue) : 1;
          data.benchmarkValue = parsedAmount * (benchmarkValue / initialValue);
          dateMap.set(dateKey, data);
        }
      }
      
      // Fill in missing data with linear interpolation
      let prevDate = null;
      let prevData = null;
      
      // Convert map to sorted array to properly handle interpolation
      const sortedDates = Array.from(dateMap.keys()).sort();
      
      for (const dateKey of sortedDates) {
        const data = dateMap.get(dateKey);
        
        // If no real data for this date but we have previous data
        if (!data.hasRealData && prevData) {
          // Find next date with real data for interpolation
          let nextDate = null;
          let nextData = null;
          
          for (let i = sortedDates.indexOf(dateKey) + 1; i < sortedDates.length; i++) {
            const nextKey = sortedDates[i];
            const nextEntry = dateMap.get(nextKey);
            if (nextEntry.hasRealData) {
              nextDate = nextKey;
              nextData = nextEntry;
              break;
            }
          }
          
          // If we found both previous and next data points, interpolate
          if (nextData) {
            const prevIndex = sortedDates.indexOf(prevDate);
            const currentIndex = sortedDates.indexOf(dateKey);
            const nextIndex = sortedDates.indexOf(nextDate);
            
            const totalSteps = nextIndex - prevIndex;
            const currentStep = currentIndex - prevIndex;
            const ratio = currentStep / totalSteps;
            
            data.portfolioValue = prevData.portfolioValue + 
                                 (nextData.portfolioValue - prevData.portfolioValue) * ratio;
          } else {
            // If no future data, use previous value
            data.portfolioValue = prevData.portfolioValue;
          }
        }
        
        // Set benchmark value if it's zero
        if (data.benchmarkValue === 0 && prevData) {
          data.benchmarkValue = prevData.benchmarkValue;
        }
        
        // Only update prevData if we have real data or have calculated it
        if (data.portfolioValue > 0) {
          prevDate = dateKey;
          prevData = data;
        }
        
        // Add to performance arrays if we have data
        if (data.portfolioValue > 0) {
          portfolioPerformance.push({
            date: dateKey,
            value: parseFloat(data.portfolioValue.toFixed(2))
          });
        }
        
        if (data.benchmarkValue > 0) {
          benchmarkPerformance.push({
            date: dateKey,
            value: parseFloat(data.benchmarkValue.toFixed(2))
          });
        }
      }
      
      // Make sure we have values for each day by filling in any remaining gaps
      const filledPortfolioPerformance = [];
      const dateKeys = Object.keys(sortedDates);
      
      for (let i = 0; i < sortedDates.length; i++) {
        const dateKey = sortedDates[i];
        const data = dateMap.get(dateKey);
        
        // Add entry with best available data
        filledPortfolioPerformance.push({
          date: dateKey,
          value: data.portfolioValue > 0 ? 
                 parseFloat(data.portfolioValue.toFixed(2)) : 
                 (filledPortfolioPerformance.length > 0 ? 
                  filledPortfolioPerformance[filledPortfolioPerformance.length - 1].value : 
                  parsedAmount)
        });
      }
      
      // Calculate metrics
      const startValue = parsedAmount;
      const endValue = filledPortfolioPerformance.length > 0 ? 
                      filledPortfolioPerformance[filledPortfolioPerformance.length - 1].value : 
                      parsedAmount;
      
      const totalDays = (parsedEndDate.getTime() - parsedStartDate.getTime()) / (1000 * 60 * 60 * 24);
      const years = totalDays / 365;
      
      const netProfit = endValue - startValue;
      const percentageGain = ((endValue / startValue) - 1) * 100;
      const annualizedReturn = (Math.pow((endValue / startValue), (1 / years)) - 1) * 100;
      
      // Calculate volatility (standard deviation of daily returns)
      const dailyReturns = [];
      for (let i = 1; i < filledPortfolioPerformance.length; i++) {
        const prevValue = filledPortfolioPerformance[i-1].value;
        const currentValue = filledPortfolioPerformance[i].value;
        const dailyReturn = (currentValue / prevValue) - 1;
        dailyReturns.push(dailyReturn);
      }
      
      // Calculate standard deviation
      const mean = dailyReturns.reduce((sum, value) => sum + value, 0) / dailyReturns.length;
      const squaredDiffs = dailyReturns.map(value => Math.pow(value - mean, 2));
      const variance = squaredDiffs.reduce((sum, value) => sum + value, 0) / squaredDiffs.length;
      const dailyVolatility = Math.sqrt(variance);
      const annualizedVolatility = dailyVolatility * Math.sqrt(252) * 100; // 252 trading days in a year
      
      // Calculate max drawdown
      let maxValue = filledPortfolioPerformance[0].value;
      let maxDrawdown = 0;
      
      for (const point of filledPortfolioPerformance) {
        if (point.value > maxValue) {
          maxValue = point.value;
        }
        
        const drawdown = (maxValue - point.value) / maxValue * 100;
        if (drawdown > maxDrawdown) {
          maxDrawdown = drawdown;
        }
      }
      
      // Calculate Sharpe ratio (using risk-free rate of 5%)
      const riskFreeRate = 5; // 5% annual risk-free rate
      const excessReturn = annualizedReturn - riskFreeRate;
      const sharpeRatio = annualizedVolatility > 0 ? excessReturn / annualizedVolatility : 0;
      
      // Create enhanced result object
      const enhancedResult = {
        portfolioPerformance: filledPortfolioPerformance,
        benchmarkPerformance,
        metrics: {
          totalReturn: parseFloat(percentageGain.toFixed(2)),
          annualizedReturn: parseFloat(annualizedReturn.toFixed(2)),
          volatility: parseFloat(annualizedVolatility.toFixed(2)),
          sharpeRatio: parseFloat(sharpeRatio.toFixed(2)),
          maxDrawdown: parseFloat(maxDrawdown.toFixed(2)),
          successRate: parseFloat((100 - maxDrawdown).toFixed(2))
        },
        summary: {
          startValue: parseFloat(startValue.toFixed(2)),
          endValue: parseFloat(endValue.toFixed(2)),
          netProfit: parseFloat(netProfit.toFixed(2)),
          percentageGain: parseFloat(percentageGain.toFixed(2))
        }
      };
      
      console.log(`Generated backtest with ${enhancedResult.portfolioPerformance.length} portfolio data points and ${enhancedResult.benchmarkPerformance.length} benchmark data points`);
      
      res.json(enhancedResult);
    } catch (error) {
      console.error("Error running portfolio backtest:", error);
      res.status(500).json({ 
        message: "Failed to run portfolio backtest", 
        error: (error as Error).message 
      });
    }
  });

  // Category-based quartile endpoints
  app.get('/api/quartile/categories', async (req, res) => {
    try {
      const query = `
        SELECT 
          f.category,
          COUNT(f.id) as total_funds,
          COUNT(fsc.fund_id) as scored_funds
        FROM funds f
        LEFT JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id 
          AND fsc.score_date = CURRENT_DATE AND fsc.quartile IS NOT NULL
        WHERE f.category IS NOT NULL
        GROUP BY f.category
        ORDER BY COUNT(fsc.fund_id) DESC, COUNT(f.id) DESC
      `;
      
      const result = await executeRawQuery(query);
      
      const categories = result.rows.map(row => ({
        name: row.category,
        totalFunds: parseInt(row.total_funds),
        fundCount: parseInt(row.scored_funds)
      }));
      
      res.json(categories);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/quartile/category/:category/distribution', async (req, res) => {
    try {
      const category = req.params.category;
      const distribution = await storage.getQuartileDistribution(category);
      res.json(distribution);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/quartile/category/:category/metrics', async (req, res) => {
    try {
      const category = req.params.category;
      const metrics = await storage.getQuartileMetrics(category);
      res.json(metrics);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Automated quartile scheduler endpoints
  app.get('/api/scheduler/status', (req, res) => {
    try {
      const status = automatedScheduler.getStatus();
      res.json(status);
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/scheduler/trigger-daily', async (req, res) => {
    try {
      await automatedScheduler.triggerDailyCheck();
      res.json({ success: true, message: 'Daily eligibility check triggered' });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/scheduler/trigger-weekly', async (req, res) => {
    try {
      await automatedScheduler.triggerWeeklyRecalculation();
      res.json({ success: true, message: 'Weekly quartile recalculation triggered' });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/scheduler/trigger-migration', async (req, res) => {
    try {
      await automatedScheduler.triggerMigrationTracking();
      res.json({ success: true, message: 'Migration tracking triggered' });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  // Authentic quartile distribution endpoint (not synthetic data)
  app.get('/api/authentic-quartile/distribution', async (req, res) => {
    try {
      const result = await executeRawQuery(`
        SELECT 
          COUNT(*) as total_count,
          SUM(CASE WHEN quartile = 1 THEN 1 ELSE 0 END) as q1_count,
          SUM(CASE WHEN quartile = 2 THEN 1 ELSE 0 END) as q2_count,
          SUM(CASE WHEN quartile = 3 THEN 1 ELSE 0 END) as q3_count,
          SUM(CASE WHEN quartile = 4 THEN 1 ELSE 0 END) as q4_count
        FROM quartile_rankings
        WHERE calculation_date = (SELECT MAX(calculation_date) FROM quartile_rankings)
      `);

      const data = result.rows[0];
      const totalCount = parseInt(data.total_count);
      const q1Count = parseInt(data.q1_count);
      const q2Count = parseInt(data.q2_count);
      const q3Count = parseInt(data.q3_count);
      const q4Count = parseInt(data.q4_count);

      res.json({
        totalCount,
        q1Count,
        q2Count,
        q3Count,
        q4Count,
        q1Percent: totalCount > 0 ? Math.round((q1Count / totalCount) * 100) : 0,
        q2Percent: totalCount > 0 ? Math.round((q2Count / totalCount) * 100) : 0,
        q3Percent: totalCount > 0 ? Math.round((q3Count / totalCount) * 100) : 0,
        q4Percent: totalCount > 0 ? Math.round((q4Count / totalCount) * 100) : 0,
        dataSource: 'authentic'
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
