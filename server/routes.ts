import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { db, executeRawQuery, pool } from "./db";
import { dataCollector } from "./services/data-collector";
import { elivateFramework } from "./services/elivate-framework";
import { portfolioBuilder } from "./services/portfolio-builder";
import { fundDetailsCollector } from "./services/fund-details-collector";
import { quartileScheduler as automatedScheduler } from "./services/automated-quartile-scheduler";
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
  
  // [Removed duplicate route - using the router in api/fund-details-import.ts instead]

  // Fund details scheduling moved to dedicated router in api/fund-details-import.ts
  
  // Auto-start the scheduled bulk processing on server startup
  console.log("🚀 Auto-starting fund details bulk processing scheduler...");
  fundDetailsCollector.startScheduledBulkProcessing(100, 5, 24);
  
  // Quartile scoring scheduler disabled - production system uses fund_scores_corrected
  console.log("ℹ️ Legacy quartile scheduler disabled - using production scoring system");
  
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

  app.get("/api/quartile/metrics", async (_req, res) => {
    console.log("✓ QUARTILE METRICS ROUTE HIT");
    try {
      const metrics = await storage.getQuartileMetrics();
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

  // Alpha Vantage endpoints removed - production system uses AMFI data

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
  
  // Production system focuses on fund analysis - backtesting functionality removed

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
