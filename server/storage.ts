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
    return db.select().from(funds)
      .where(
        sql`${funds.fundName} ILIKE ${'%' + query + '%'} OR ${funds.amcName} ILIKE ${'%' + query + '%'}`
      )
      .limit(limit);
  }
  
  // NAV data methods
  async getNavData(fundId: number, startDate?: Date, endDate?: Date, limit: number = 100): Promise<NavData[]> {
    let query = db.select().from(navData).where(eq(navData.fundId, fundId));
    
    if (startDate) {
      query = query.where(gte(navData.navDate, startDate));
    }
    
    if (endDate) {
      query = query.where(lte(navData.navDate, endDate));
    }
    
    return query.orderBy(desc(navData.navDate)).limit(limit);
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
  
  // Fund score methods
  async getFundScore(fundId: number, scoreDate?: Date): Promise<FundScore | undefined> {
    let query = db.select().from(fundScores).where(eq(fundScores.fundId, fundId));
    
    if (scoreDate) {
      query = query.where(eq(fundScores.scoreDate, scoreDate));
    } else {
      query = query.orderBy(desc(fundScores.scoreDate)).limit(1);
    }
    
    const [score] = await query;
    return score;
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
    // Get a list of unique index names
    const indexNames = await db.select({ name: marketIndices.indexName })
      .from(marketIndices)
      .groupBy(marketIndices.indexName);
    
    const latestIndices: MarketIndex[] = [];
    
    // For each index name, get the latest data
    for (const { name } of indexNames) {
      const [latestIndex] = await db.select().from(marketIndices)
        .where(eq(marketIndices.indexName, name))
        .orderBy(desc(marketIndices.indexDate))
        .limit(1);
      
      if (latestIndex) {
        latestIndices.push(latestIndex);
      }
    }
    
    return latestIndices;
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
  
  // Quartile Analysis methods
  async getFundsByQuartile(quartile: number, category?: string): Promise<any> {
    try {
      let query = db
        .select({
          id: funds.id,
          fundName: funds.fundName,
          category: funds.category,
          amc: funds.amcName,
          historicalReturnsTotal: fundScores.historicalReturnsTotal,
          riskGradeTotal: fundScores.riskGradeTotal,
          otherMetricsTotal: fundScores.otherMetricsTotal,
          totalScore: fundScores.totalScore,
          recommendation: fundScores.recommendation
        })
        .from(fundScores)
        .innerJoin(funds, eq(funds.id, fundScores.fundId))
        .where(eq(fundScores.quartile, quartile))
        .orderBy(desc(fundScores.totalScore));

      if (category) {
        query = query.where(eq(funds.category, category));
      }

      const results = await query.limit(100);
      
      return { funds: results };
    } catch (error) {
      console.error("Error getting funds by quartile:", error);
      throw error;
    }
  }

  async getQuartileMetrics(): Promise<any> {
    try {
      // Query for returns data by quartile
      const returnsQuery = `
        SELECT 
          fs.quartile,
          AVG(fs.return_1y_score) as avg_return_1y,
          AVG(fs.return_3y_score) as avg_return_3y,
          AVG(fs.return_5y_score) as avg_return_5y
        FROM fund_scores fs
        WHERE fs.quartile IS NOT NULL
        GROUP BY fs.quartile
        ORDER BY fs.quartile
      `;
      
      // Query for risk metrics by quartile
      const riskQuery = `
        SELECT 
          fs.quartile,
          AVG(fs.sharpe_ratio) as avg_sharpe_ratio,
          AVG(fs.max_drawdown) as avg_max_drawdown
        FROM fund_scores fs
        WHERE fs.quartile IS NOT NULL
        GROUP BY fs.quartile
        ORDER BY fs.quartile
      `;
      
      // Query for scoring breakdown by quartile
      const scoringQuery = `
        SELECT 
          fs.quartile,
          AVG(fs.returns_score) as avg_returns_score,
          AVG(fs.risk_score) as avg_risk_score,
          AVG(fs.other_metrics_score) as avg_other_metrics_score,
          AVG(fs.total_score) as avg_total_score
        FROM fund_scores fs
        WHERE fs.quartile IS NOT NULL
        GROUP BY fs.quartile
        ORDER BY fs.quartile
      `;
      
      const returnsResult = await executeRawQuery(returnsQuery);
      const riskResult = await executeRawQuery(riskQuery);
      const scoringResult = await executeRawQuery(scoringQuery);
      
      // Transform returns data
      const returnsData = returnsResult.rows.map(row => ({
        name: `Q${row.quartile}`,
        return1Y: parseFloat(row.avg_return_1y) || 0,
        return3Y: parseFloat(row.avg_return_3y) || 0,
        return5Y: parseFloat(row.avg_return_5y) || 0
      }));
      
      // Transform risk data
      const riskData = riskResult.rows.map(row => ({
        name: `Q${row.quartile}`,
        sharpeRatio: parseFloat(row.avg_sharpe_ratio) || 0,
        maxDrawdown: parseFloat(row.avg_max_drawdown) || 0
      }));
      
      // Transform scoring data
      const scoringData = scoringResult.rows.map(row => {
        const quartileLabels = {
          1: "Q1 (Top 25%)",
          2: "Q2 (26-50%)",
          3: "Q3 (51-75%)",
          4: "Q4 (Bottom 25%)"
        };
        
        return {
          name: `Q${row.quartile}`,
          label: quartileLabels[row.quartile as 1|2|3|4] || `Q${row.quartile}`,
          historicalReturns: parseFloat(row.avg_returns_score) || 0,
          riskGrade: parseFloat(row.avg_risk_score) || 0,
          otherMetrics: parseFloat(row.avg_other_metrics_score) || 0,
          totalScore: parseFloat(row.avg_total_score) || 0
        };
      });

      return {
        returnsData: returnsData.length > 0 ? returnsData : [],
        riskData: riskData.length > 0 ? riskData : [],
        scoringData: scoringData.length > 0 ? scoringData : []
      };
    } catch (error) {
      console.error("Error getting quartile metrics:", error);
      throw error;
    }
  }

  async getQuartileDistribution(category?: string): Promise<any> {
    try {
      // Build query conditions
      let whereClause = "";
      const params: any[] = [];
      
      if (category) {
        whereClause = "WHERE f.category = $1";
        params.push(category);
      }
      
      // Query to count funds by quartile
      const query = `
        SELECT 
          COUNT(*) as total_count,
          COUNT(CASE WHEN fs.quartile = 1 THEN 1 END) as q1_count,
          COUNT(CASE WHEN fs.quartile = 2 THEN 1 END) as q2_count,
          COUNT(CASE WHEN fs.quartile = 3 THEN 1 END) as q3_count,
          COUNT(CASE WHEN fs.quartile = 4 THEN 1 END) as q4_count
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        ${whereClause}
      `;
      
      const result = await executeRawQuery(query, params);
      
      if (result.rows.length === 0) {
        throw new Error("No quartile data found");
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
        q4Percent
      };
    } catch (error) {
      console.error("Error getting quartile distribution:", error);
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
