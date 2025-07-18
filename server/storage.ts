import { db, executeRawQuery } from "./db";
import { eq, and, desc, lte, gte, sql, like, ilike, InferInsertModel } from "drizzle-orm";
import { 
  users, type User, type InsertUser,
  funds, type Fund, type InsertFund,
  navData, type NavData, type InsertNavData,
  fundScores, type FundScore, type InsertFundScore,
  portfolioHoldings, type PortfolioHolding, type InsertPortfolioHolding,
  marketIndices, type MarketIndex, type InsertMarketIndex,
  elivateScores, type ElivateScore, type InsertElivateScore,
  modelPortfolios, type ModelPortfolio, type InsertModelPortfolio,
  modelPortfolioAllocations, type ModelPortfolioAllocation, type InsertModelPortfolioAllocation,
  etlPipelineRuns, type EtlPipelineRun, type InsertEtlPipelineRun
} from "@shared/schema";

export interface IStorage {
  // User methods
  getUser(id: number): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  createUser(user: InsertUser): Promise<User>;
  
  // Fund methods
  getFund(id: number): Promise<Fund | undefined>;
  getFundBySchemeCode(schemeCode: string): Promise<Fund | undefined>;
  getFundsByCategory(category: string): Promise<Fund[]>;
  getFundsByQuartile(quartile: number, category?: string): Promise<any[]>;
  getQuartileMetrics(): Promise<any>;
  getQuartileDistribution(category?: string): Promise<any>;
  getAllFunds(limit?: number, offset?: number): Promise<Fund[]>;
  createFund(fund: InsertFund): Promise<Fund>;
  updateFund(id: number, updatedFields: Partial<InsertFund>): Promise<Fund | undefined>;
  searchFunds(query: string, limit?: number): Promise<Fund[]>;
  
  // NAV data methods
  getNavData(fundId: number, startDate?: Date, endDate?: Date, limit?: number): Promise<NavData[]>;
  getLatestNav(fundId: number): Promise<NavData | undefined>;
  createNavData(nav: InsertNavData): Promise<NavData>;
  bulkInsertNavData(navs: InsertNavData[]): Promise<number>;
  
  // Fund score methods
  getFundScore(fundId: number, scoreDate?: Date): Promise<FundScore | undefined>;
  getLatestFundScores(limit?: number, category?: string): Promise<(FundScore & { fund: Fund })[]>;
  createFundScore(score: InsertFundScore): Promise<FundScore>;
  updateFundScore(fundId: number, scoreDate: Date, updatedFields: Partial<InsertFundScore>): Promise<FundScore | undefined>;
  
  // Portfolio holdings methods
  getPortfolioHoldings(fundId: number, holdingDate?: Date): Promise<PortfolioHolding[]>;
  createPortfolioHolding(holding: InsertPortfolioHolding): Promise<PortfolioHolding>;
  bulkInsertPortfolioHoldings(holdings: InsertPortfolioHolding[]): Promise<number>;
  
  // Market indices methods
  getMarketIndex(indexName: string, startDate?: Date, endDate?: Date): Promise<MarketIndex[]>;
  getLatestMarketIndices(): Promise<MarketIndex[]>;
  createMarketIndex(index: InsertMarketIndex): Promise<MarketIndex>;
  
  // ELIVATE framework methods
  getElivateScore(id?: number, scoreDate?: Date): Promise<ElivateScore | undefined>;
  getLatestElivateScore(): Promise<ElivateScore | undefined>;
  createElivateScore(score: InsertElivateScore): Promise<ElivateScore>;
  
  // Model portfolio methods
  getModelPortfolio(id: number): Promise<(ModelPortfolio & { allocations: (ModelPortfolioAllocation & { fund: Fund })[] }) | undefined>;
  getModelPortfolios(): Promise<ModelPortfolio[]>;
  createModelPortfolio(portfolio: InsertModelPortfolio, allocations: InsertModelPortfolioAllocation[]): Promise<ModelPortfolio>;
  
  // ETL pipeline methods
  getETLRuns(pipelineName?: string, limit?: number): Promise<EtlPipelineRun[]>;
  createETLRun(run: InsertEtlPipelineRun): Promise<EtlPipelineRun>;
  updateETLRun(id: number, updatedFields: Partial<InsertEtlPipelineRun>): Promise<EtlPipelineRun | undefined>;
}

export class DatabaseStorage implements IStorage {
  // User methods
  async getUser(id: number): Promise<User | undefined> {
    const [user] = await db.select().from(users).where(eq(users.id, id));
    return user;
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    const [user] = await db.select().from(users).where(eq(users.username, username));
    return user;
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const [user] = await db
      .insert(users)
      .values(insertUser)
      .returning();
    return user;
  }
  
  // Fund methods
  async getFund(id: number): Promise<Fund | undefined> {
    // Make sure we have a valid ID before querying
    if (!id || isNaN(id)) {
      console.warn(`Invalid fund ID provided: ${id}`);
      return undefined;
    }
    
    try {
      const [fund] = await db.select().from(funds).where(eq(funds.id, id));
      return fund;
    } catch (error) {
      console.error(`Error fetching fund with ID ${id}:`, error);
      return undefined;
    }
  }
  
  async getFundBySchemeCode(schemeCode: string): Promise<Fund | undefined> {
    const [fund] = await db.select().from(funds).where(eq(funds.schemeCode, schemeCode));
    return fund;
  }
  
  async getFundsByCategory(category: string): Promise<Fund[]> {
    return db.select().from(funds).where(eq(funds.category, category));
  }
  
  async getAllFunds(limit: number = 100, offset: number = 0): Promise<Fund[]> {
    return db.select().from(funds).limit(limit).offset(offset);
  }
  
  async createFund(fund: InsertFund): Promise<Fund> {
    const [newFund] = await db.insert(funds).values(fund).returning();
    return newFund;
  }
  
  async updateFund(id: number, updatedFields: Partial<InsertFund>): Promise<Fund | undefined> {
    const [updatedFund] = await db.update(funds)
      .set({ ...updatedFields, updatedAt: new Date() })
      .where(eq(funds.id, id))
      .returning();
    return updatedFund;
  }
  
  async searchFunds(query: string, limit: number = 10): Promise<Fund[]> {
    const result = await executeRawQuery(`
      SELECT f.*, aa.aum_crores 
      FROM funds f
      LEFT JOIN aum_analytics aa ON f.fund_name = aa.fund_name
      WHERE f.fund_name ILIKE $1 OR f.amc_name ILIKE $2
      LIMIT $3
    `, [`%${query}%`, `%${query}%`, limit]);
    
    return result.rows;
  }
  
  // NAV data methods
  async getNavData(fundId: number, startDate?: Date, endDate?: Date, limit: number = 100): Promise<NavData[]> {
    // Use raw SQL for better performance with large NAV dataset
    let query = `
      SELECT fund_id as "fundId", nav_date as "navDate", nav_value as "navValue", 
             nav_change as "navChange", nav_change_pct as "navChangePct", 
             aum_cr as "aumCr", created_at as "createdAt"
      FROM nav_data
      WHERE fund_id = $1
    `;
    
    const params: any[] = [fundId];
    let paramCount = 1;
    
    if (startDate) {
      paramCount++;
      query += ` AND nav_date >= $${paramCount}`;
      params.push(startDate);
    }
    
    if (endDate) {
      paramCount++;
      query += ` AND nav_date <= $${paramCount}`;
      params.push(endDate);
    }
    
    query += ` ORDER BY nav_date DESC LIMIT $${paramCount + 1}`;
    params.push(limit);
    
    const result = await executeRawQuery(query, params);
    return result.rows;
  }
  
  async getLatestNav(fundId: number): Promise<NavData | undefined> {
    const [latestNav] = await db.select().from(navData)
      .where(eq(navData.fundId, fundId))
      .orderBy(desc(navData.navDate))
      .limit(1);
    return latestNav;
  }
  
  async createNavData(nav: InsertNavData): Promise<NavData> {
    const [newNav] = await db.insert(navData).values(nav).returning();
    return newNav;
  }
  
  async bulkInsertNavData(navs: InsertNavData[]): Promise<number> {
    if (navs.length === 0) return 0;
    
    const result = await db.insert(navData).values(navs).returning();
    return result.length;
  }
  
  // Fund score methods - using authentic fund_scores_corrected data
  async getFundScore(fundId: number, scoreDate?: Date): Promise<any> {
    try {
      // Use raw SQL to fetch from fund_scores_corrected (the correct table)
      let query = `
        SELECT 
          fs.*,
          f.fund_name,
          f.category,
          f.subcategory,
          f.amc_name
        FROM fund_scores_corrected fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE fs.fund_id = $1
      `;
      
      const params = [fundId];
      
      if (scoreDate) {
        query += ` AND fs.score_date = $2`;
        params.push(scoreDate);
      } else {
        query += ` ORDER BY fs.score_date DESC LIMIT 1`;
      }
      
      const result = await executeRawQuery(query, params);
      return result.rows[0];
    } catch (error) {
      console.error('Error fetching fund score:', error);
      return undefined;
    }
  }
  
  async getLatestFundScores(limit: number = 20, category?: string): Promise<(FundScore & { fund: Fund })[]> {
    let query = db.select({
      ...fundScores,
      fund: funds
    })
    .from(fundScores)
    .innerJoin(funds, eq(fundScores.fundId, funds.id))
    .orderBy(desc(fundScores.totalScore));
    
    if (category) {
      query = query.where(eq(funds.category, category));
    }
    
    return query.limit(limit);
  }
  
  async createFundScore(score: InsertFundScore): Promise<FundScore> {
    const [newScore] = await db.insert(fundScores).values(score).returning();
    return newScore;
  }
  
  async updateFundScore(
    fundId: number, 
    scoreDate: Date, 
    updatedFields: Partial<InsertFundScore>
  ): Promise<FundScore | undefined> {
    const [updatedScore] = await db.update(fundScores)
      .set(updatedFields)
      .where(
        and(
          eq(fundScores.fundId, fundId),
          eq(fundScores.scoreDate, scoreDate)
        )
      )
      .returning();
    return updatedScore;
  }
  
  // Portfolio holdings methods
  async getPortfolioHoldings(fundId: number, holdingDate?: Date): Promise<PortfolioHolding[]> {
    let query = db.select().from(portfolioHoldings).where(eq(portfolioHoldings.fundId, fundId));
    
    if (holdingDate) {
      query = query.where(eq(portfolioHoldings.holdingDate, holdingDate));
    } else {
      // Get most recent holdings if date not specified
      const [latestHolding] = await db.select({ date: portfolioHoldings.holdingDate })
        .from(portfolioHoldings)
        .where(eq(portfolioHoldings.fundId, fundId))
        .orderBy(desc(portfolioHoldings.holdingDate))
        .limit(1);
      
      if (latestHolding) {
        query = query.where(eq(portfolioHoldings.holdingDate, latestHolding.date));
      }
    }
    
    return query;
  }
  
  async createPortfolioHolding(holding: InsertPortfolioHolding): Promise<PortfolioHolding> {
    const [newHolding] = await db.insert(portfolioHoldings).values(holding).returning();
    return newHolding;
  }
  
  async bulkInsertPortfolioHoldings(holdings: InsertPortfolioHolding[]): Promise<number> {
    if (holdings.length === 0) return 0;
    
    const result = await db.insert(portfolioHoldings).values(holdings).returning();
    return result.length;
  }
  
  // Market indices methods
  async getMarketIndex(indexName: string, startDate?: Date, endDate?: Date): Promise<MarketIndex[]> {
    let query = db.select().from(marketIndices).where(eq(marketIndices.indexName, indexName));
    
    if (startDate) {
      query = query.where(gte(marketIndices.indexDate, startDate));
    }
    
    if (endDate) {
      query = query.where(lte(marketIndices.indexDate, endDate));
    }
    
    return query.orderBy(desc(marketIndices.indexDate));
  }
  
  async getLatestMarketIndices(): Promise<MarketIndex[]> {
    // Optimized query using window function to get latest index for each name in a single query
    const query = `
      WITH ranked_indices AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY index_name ORDER BY index_date DESC) as rn
        FROM market_indices
      )
      SELECT 
        r1.index_name, 
        r1.index_date, 
        r1.open_value, 
        r1.high_value, 
        r1.low_value, 
        r1.close_value,
        r1.volume, 
        r1.market_cap, 
        r1.pe_ratio, 
        r1.pb_ratio, 
        r1.dividend_yield, 
        r1.created_at,
        -- Calculate change percent
        CASE 
          WHEN r2.close_value IS NOT NULL AND r2.close_value > 0 
          THEN ROUND(((r1.close_value - r2.close_value) / r2.close_value * 100)::numeric, 2)
          ELSE NULL
        END as change_percent
      FROM ranked_indices r1
      LEFT JOIN ranked_indices r2 
        ON r1.index_name = r2.index_name 
        AND r2.rn = 2  -- Previous day
      WHERE r1.rn = 1
      ORDER BY r1.index_name
    `;
    
    const result = await executeRawQuery(query);
    
    // Map the raw results to MarketIndex type with available columns
    return result.rows.map(row => ({
      indexName: row.index_name,
      indexDate: row.index_date,
      openValue: row.open_value,
      highValue: row.high_value,
      lowValue: row.low_value,
      closeValue: row.close_value,
      volume: row.volume,
      marketCap: row.market_cap,
      peRatio: row.pe_ratio,
      pbRatio: row.pb_ratio,
      dividendYield: row.dividend_yield,
      createdAt: row.created_at,
      changePercent: row.change_percent ? parseFloat(row.change_percent) : null
    }));
  }
  
  async createMarketIndex(index: InsertMarketIndex): Promise<MarketIndex> {
    const [newIndex] = await db.insert(marketIndices).values(index).returning();
    return newIndex;
  }
  
  // ELIVATE framework methods
  async getElivateScore(id?: number, scoreDate?: Date): Promise<ElivateScore | undefined> {
    if (id) {
      const [score] = await db.select().from(elivateScores).where(eq(elivateScores.id, id));
      return score;
    }
    
    if (scoreDate) {
      const [score] = await db.select().from(elivateScores).where(eq(elivateScores.scoreDate, scoreDate));
      return score;
    }
    
    // If neither id nor date provided, return latest
    return this.getLatestElivateScore();
  }
  
  async getLatestElivateScore(): Promise<ElivateScore | undefined> {
    const [latestScore] = await db.select().from(elivateScores)
      .orderBy(desc(elivateScores.scoreDate))
      .limit(1);
    return latestScore;
  }
  
  async createElivateScore(score: InsertElivateScore): Promise<ElivateScore> {
    const [newScore] = await db.insert(elivateScores).values(score).returning();
    return newScore;
  }
  
  // Model portfolio methods
  async getModelPortfolio(id: number): Promise<(ModelPortfolio & { allocations: (ModelPortfolioAllocation & { fund: Fund })[] }) | undefined> {
    const [portfolio] = await db.select().from(modelPortfolios).where(eq(modelPortfolios.id, id));
    
    if (!portfolio) return undefined;
    
    const allocations = await db.select({
      ...modelPortfolioAllocations,
      fund: funds
    })
    .from(modelPortfolioAllocations)
    .innerJoin(funds, eq(modelPortfolioAllocations.fundId, funds.id))
    .where(eq(modelPortfolioAllocations.portfolioId, id));
    
    return {
      ...portfolio,
      allocations
    };
  }
  
  async getModelPortfolios(): Promise<ModelPortfolio[]> {
    return db.select().from(modelPortfolios);
  }
  
  async createModelPortfolio(
    portfolio: InsertModelPortfolio, 
    allocations: InsertModelPortfolioAllocation[]
  ): Promise<ModelPortfolio> {
    // Using a transaction to ensure both the portfolio and its allocations are created
    const [newPortfolio] = await db.transaction(async (tx) => {
      const [createdPortfolio] = await tx.insert(modelPortfolios).values(portfolio).returning();
      
      if (allocations.length > 0) {
        await tx.insert(modelPortfolioAllocations).values(
          allocations.map(allocation => ({
            ...allocation,
            portfolioId: createdPortfolio.id
          }))
        );
      }
      
      return [createdPortfolio];
    });
    
    return newPortfolio;
  }
  
  // Quartile Analysis methods - using authentic fund_scores_corrected data
  async getFundsByQuartile(quartile: number, category?: string): Promise<any> {
    try {
      // Use authentic fund_scores_corrected with actual quartile assignments
      let whereClause = "WHERE fs.score_date = '2025-06-05' AND fs.quartile = $1";
      const params: any[] = [quartile];
      
      if (category) {
        whereClause += ` AND f.category = $${params.length + 1}`;
        params.push(category);
      }
      
      const query = `
        SELECT 
          f.id,
          f.fund_name as "fundName",
          f.category,
          f.subcategory,
          f.amc_name as "amc",
          fs.total_score as "totalScore",
          fs.recommendation,
          fs.quartile,
          ROW_NUMBER() OVER (PARTITION BY f.category ORDER BY fs.total_score DESC) as rank,
          PERCENT_RANK() OVER (PARTITION BY f.category ORDER BY fs.total_score DESC) * 100 as percentile
        FROM fund_scores_corrected fs
        JOIN funds f ON fs.fund_id = f.id
        ${whereClause}
        ORDER BY fs.total_score DESC
        LIMIT 100
      `;
      
      const result = await executeRawQuery(query, params);
      
      return { funds: result.rows };
    } catch (error) {
      console.error("Error getting funds by quartile:", error);
      throw error;
    }
  }

  async getQuartileMetrics(category?: string): Promise<any> {
    try {
      let whereClause = "WHERE fs.score_date = '2025-06-05' AND fs.quartile IS NOT NULL";
      const params: any[] = [];
      
      if (category) {
        whereClause += " AND f.category = $1";
        params.push(category);
      }
      
      // Query for authentic quartile performance data from fund_scores_corrected
      const performanceQuery = `
        SELECT 
          fs.quartile,
          ROUND(AVG(fs.total_score)::numeric, 2) as avg_score,
          ROUND(AVG(fs.total_score)::numeric, 2) as avg_percentile,
          COUNT(*) as fund_count,
          ROUND(AVG(fs.return_1y_absolute)::numeric, 2) as return_1y,
          ROUND(AVG(fs.return_3y_absolute)::numeric, 2) as return_3y
        FROM fund_scores_corrected fs
        JOIN funds f ON fs.fund_id = f.id
        ${whereClause}
        GROUP BY fs.quartile
        ORDER BY fs.quartile
      `;
      
      const performanceResult = await executeRawQuery(performanceQuery, params);
      
      // Transform authentic performance data from fund_scores_corrected
      const returnsData = performanceResult.rows.map(row => ({
        name: `Q${row.quartile}`,
        avgScore: parseFloat(row.avg_score) || 0,
        avgPercentile: parseFloat(row.avg_percentile) || 0,
        fundCount: parseInt(row.fund_count) || 0,
        return1Y: parseFloat(row.return_1y) || 0,
        return3Y: parseFloat(row.return_3y) || 0
      }));

      return {
        returnsData: returnsData.length > 0 ? returnsData : []
      };
    } catch (error) {
      console.error("Error getting quartile metrics:", error);
      throw error;
    }
  }

  async getQuartileDistribution(category?: string): Promise<any> {
    try {
      // Use authentic fund_scores_corrected with actual quartile assignments
      let whereClause = "WHERE fs.score_date = '2025-06-05' AND fs.quartile IS NOT NULL";
      const params: any[] = [];
      
      if (category) {
        whereClause += " AND f.category = $1";
        params.push(category);
      }
      
      // Get actual quartile distribution from fund_scores_corrected
      const query = `
        SELECT 
          COUNT(*) as total_count,
          COUNT(CASE WHEN fs.quartile = 1 THEN 1 END) as q1_count,
          COUNT(CASE WHEN fs.quartile = 2 THEN 1 END) as q2_count,
          COUNT(CASE WHEN fs.quartile = 3 THEN 1 END) as q3_count,
          COUNT(CASE WHEN fs.quartile = 4 THEN 1 END) as q4_count
        FROM fund_scores_corrected fs
        JOIN funds f ON fs.fund_id = f.id
        ${whereClause}
      `;
      
      const result = await executeRawQuery(query, params);
      
      if (result.rows.length === 0) {
        throw new Error("No authentic quartile data found in fund_scores_corrected");
      }
      
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
      
      return {
        totalCount,
        q1Count,
        q2Count,
        q3Count,
        q4Count,
        q1Percent,
        q2Percent,
        q3Percent,
        q4Percent,
        dataSource: 'fund_performance_metrics'
      };
    } catch (error) {
      console.error("Error getting authentic quartile distribution:", error);
      throw error;
    }
  }
  
  // ETL pipeline methods
  async getETLRuns(pipelineName?: string, limit: number = 50): Promise<EtlPipelineRun[]> {
    let query = db.select().from(etlPipelineRuns);
    
    if (pipelineName) {
      query = query.where(eq(etlPipelineRuns.pipelineName, pipelineName));
    }
    
    return query.orderBy(desc(etlPipelineRuns.startTime)).limit(limit);
  }
  
  async createETLRun(run: InsertEtlPipelineRun): Promise<EtlPipelineRun> {
    const [newRun] = await db.insert(etlPipelineRuns).values(run).returning();
    return newRun;
  }
  
  async updateETLRun(id: number, updatedFields: Partial<InsertEtlPipelineRun>): Promise<EtlPipelineRun | undefined> {
    const [updatedRun] = await db.update(etlPipelineRuns)
      .set(updatedFields)
      .where(eq(etlPipelineRuns.id, id))
      .returning();
    return updatedRun;
  }
}

export const storage = new DatabaseStorage();
