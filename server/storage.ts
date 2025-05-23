import { db } from "./db";
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
      // Sample data for development - in production these would be real database queries
      // Returns data by quartile
      const returnsData = [
        { name: "Q1", return1Y: 18.5, return3Y: 15.2 },
        { name: "Q2", return1Y: 12.7, return3Y: 10.3 },
        { name: "Q3", return1Y: 8.4, return3Y: 6.1 },
        { name: "Q4", return1Y: 3.2, return3Y: 2.5 }
      ];

      // Risk metrics by quartile
      const riskData = [
        { name: "Q1", sharpeRatio: 1.8, maxDrawdown: 12.5 },
        { name: "Q2", sharpeRatio: 1.3, maxDrawdown: 18.7 },
        { name: "Q3", sharpeRatio: 0.9, maxDrawdown: 24.3 },
        { name: "Q4", sharpeRatio: 0.4, maxDrawdown: 35.8 }
      ];

      // Scoring breakdown by quartile
      const scoringData = [
        { 
          name: "Q1", 
          label: "Q1 (Top 25%)", 
          historicalReturns: 32.6, 
          riskGrade: 24.8, 
          otherMetrics: 23.7, 
          totalScore: 81.1 
        },
        { 
          name: "Q2", 
          label: "Q2 (26-50%)", 
          historicalReturns: 26.3, 
          riskGrade: 19.1, 
          otherMetrics: 20.4, 
          totalScore: 65.8 
        },
        { 
          name: "Q3", 
          label: "Q3 (51-75%)", 
          historicalReturns: 19.7, 
          riskGrade: 15.6, 
          otherMetrics: 16.9, 
          totalScore: 52.2 
        },
        { 
          name: "Q4", 
          label: "Q4 (Bottom 25%)", 
          historicalReturns: 12.4, 
          riskGrade: 10.2, 
          otherMetrics: 12.1, 
          totalScore: 34.7 
        }
      ];

      return {
        returnsData,
        riskData,
        scoringData
      };
    } catch (error) {
      console.error("Error getting quartile metrics:", error);
      throw error;
    }
  }

  async getQuartileDistribution(category?: string): Promise<any> {
    try {
      // Sample data for development - in production this would be a real database query
      const distribution = {
        totalCount: 2985,
        q1Count: 746,
        q2Count: 747,
        q3Count: 746,
        q4Count: 746,
        q1Percent: 25,
        q2Percent: 25, 
        q3Percent: 25,
        q4Percent: 25
      };
      
      // Adjust slightly for categories
      if (category) {
        // Small adjustments to simulate different distributions by category
        if (category.startsWith('Equity')) {
          distribution.q1Count = Math.floor(distribution.q1Count * 0.9);
          distribution.q2Count = Math.floor(distribution.q2Count * 1.1);
          distribution.totalCount = distribution.q1Count + distribution.q2Count + 
                                  distribution.q3Count + distribution.q4Count;
        } else if (category.startsWith('Debt')) {
          distribution.q3Count = Math.floor(distribution.q3Count * 0.95);
          distribution.q4Count = Math.floor(distribution.q4Count * 1.05);
          distribution.totalCount = distribution.q1Count + distribution.q2Count + 
                                  distribution.q3Count + distribution.q4Count;
        }
        
        // Recalculate percentages
        distribution.q1Percent = Math.round((distribution.q1Count / distribution.totalCount) * 100);
        distribution.q2Percent = Math.round((distribution.q2Count / distribution.totalCount) * 100);
        distribution.q3Percent = Math.round((distribution.q3Count / distribution.totalCount) * 100);
        distribution.q4Percent = Math.round((distribution.q4Count / distribution.totalCount) * 100);
      }
      
      return distribution;
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
