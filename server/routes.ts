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
import amfiImportRoutes from "./api/amfi-import";
import fundDetailsImportRoutes from "./api/fund-details-import";

export async function registerRoutes(app: Express): Promise<Server> {
  // Register AMFI data import routes
  app.use('/api/amfi', amfiImportRoutes);
  
  // Register Fund Details routes
  app.use('/api/fund-details', fundDetailsImportRoutes);
  
  // [Removed duplicate route - using the router in api/fund-details-import.ts instead]

  // Fund details scheduling moved to dedicated router in api/fund-details-import.ts
  
  // Auto-start the scheduled bulk processing on server startup
  console.log("🚀 Auto-starting fund details bulk processing scheduler...");
  fundDetailsCollector.startScheduledBulkProcessing(100, 5, 24);
  
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
        console.log("Not enough funds in the database, creating some sample funds...");
        await importSomeSampleFunds();
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

  // Quartile Analysis API Routes - Using separate path to avoid conflicts
  app.get("/api/quartile/distribution", async (req, res) => {
    console.log("✓ QUARTILE DISTRIBUTION ROUTE HIT - BACKEND WORKING!");
    const category = req.query.category as string || undefined;
    try {
      const distribution = await storage.getQuartileDistribution(category);
      console.log("✓ Distribution data:", distribution);
      res.setHeader('Content-Type', 'application/json');
      res.json(distribution);
    } catch (error) {
      console.error("Error fetching quartile distribution:", error);
      res.status(500).json({ error: "Failed to fetch quartile distribution" });
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
      
      // Import our services
      const { simplePortfolioService } = await import('./services/simple-portfolio');
      const { removeDuplicateFundsFromPortfolio } = await import('./services/portfolio-deduplicator');
      
      // Generate a portfolio with real fund allocations
      let portfolio = await simplePortfolioService.generatePortfolio(riskProfile);
      
      // Apply our robust deduplication logic to ensure no duplicate funds
      console.log("Starting deduplication process to ensure unique fund recommendations...");
      portfolio = removeDuplicateFundsFromPortfolio(portfolio);
      
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
            const subcategory = fundSubcategories[category][Math.floor(Math.random() * fundSubcategories[category].length)];
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
                                     (nav.navValue / fund.navData[0].navValue);
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
          data.benchmarkValue = parsedAmount * (benchmark.indexValue / benchmarkIndex[0].indexValue);
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

  const httpServer = createServer(app);
  return httpServer;
}
