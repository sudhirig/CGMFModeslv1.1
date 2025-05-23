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
  // API routes for funds
  app.get("/api/funds", async (req, res) => {
    try {
      const { category, limit, offset } = req.query;
      const parsedLimit = limit ? parseInt(limit as string) : 100;
      const parsedOffset = offset ? parseInt(offset as string) : 0;
      
      if (category) {
        const funds = await storage.getFundsByCategory(category as string);
        return res.json(funds);
      }
      
      const funds = await storage.getAllFunds(parsedLimit, parsedOffset);
      res.json(funds);
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
      
      // Get the fund scores from the database with simplified query using raw pool query
      let query = `
        SELECT 
          fs.fund_id, 
          fs.score_date, 
          fs.total_score, 
          fs.recommendation, 
          fs.historical_returns_total, 
          fs.risk_grade_total,
          fs.other_metrics_total,
          f.fund_name, 
          f.category, 
          f.subcategory, 
          f.amc_name 
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
      `;
      
      // Add optional category filter
      let params: (string | number)[] = [];
      if (category && category !== 'undefined') {
        query += ` WHERE f.category = $1 `;
        params.push(category);
        params.push(parsedLimit);
      } else {
        query += ` WHERE 1=1 `;
        params.push(parsedLimit);
      }
      
      // Add ordering and limit
      query += ` ORDER BY fs.total_score DESC LIMIT $${params.length}`;
      
      const result = await pool.query(query, params);
      
      res.json(result.rows);
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
