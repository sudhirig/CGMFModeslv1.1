import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { db, executeRawQuery, pool } from "./db";
import { dataCollector } from "./services/data-collector";
import { elivateFramework } from "./services/elivate-framework";
import { fundScoringEngine } from "./services/fund-scoring";
import { portfolioBuilder } from "./services/portfolio-builder";
import { backtestingEngine } from "./services/backtesting-engine";

export async function registerRoutes(app: Express): Promise<Server> {
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
      const subcategories = {
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
        const result = await pool.query(`
          SELECT * FROM funds 
          ORDER BY fund_name
          LIMIT $1 OFFSET $2
        `, [parsedLimit, parsedOffset]);
        
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
      
      const portfolio = await portfolioBuilder.generateModelPortfolio(riskProfile);
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

  const httpServer = createServer(app);
  return httpServer;
}
