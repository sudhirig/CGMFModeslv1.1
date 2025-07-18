import { pgTable, text, serial, integer, boolean, date, timestamp, decimal, real, foreignKey, index, uniqueIndex } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Original users table (keep for reference)
export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  username: text("username").notNull().unique(),
  password: text("password").notNull(),
});

export const insertUserSchema = createInsertSchema(users).pick({
  username: true,
  password: true,
});

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;

// Fund master data
export const funds = pgTable("funds", {
  id: serial("id").primaryKey(),
  schemeCode: text("scheme_code").notNull().unique(),
  isinDivPayout: text("isin_div_payout"),
  isinDivReinvest: text("isin_div_reinvest"),
  fundName: text("fund_name").notNull(),
  amcName: text("amc_name").notNull(),
  category: text("category").notNull(),
  subcategory: text("subcategory"),
  benchmarkName: text("benchmark_name"),
  fundManager: text("fund_manager"),
  inceptionDate: date("inception_date"),
  status: text("status").default("ACTIVE"),
  minimumInvestment: integer("minimum_investment"),
  minimumAdditional: integer("minimum_additional"),
  exitLoad: decimal("exit_load", { precision: 4, scale: 2 }),
  lockInPeriod: integer("lock_in_period"),
  expenseRatio: decimal("expense_ratio", { precision: 4, scale: 2 }),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const insertFundSchema = createInsertSchema(funds).omit({
  id: true,
  createdAt: true,
  updatedAt: true
});

export type InsertFund = z.infer<typeof insertFundSchema>;
export type Fund = typeof funds.$inferSelect;

// NAV data
export const navData = pgTable("nav_data", {
  fundId: integer("fund_id").references(() => funds.id),
  navDate: date("nav_date").notNull(),
  navValue: decimal("nav_value", { precision: 12, scale: 4 }).notNull(),
  navChange: decimal("nav_change", { precision: 12, scale: 4 }),
  navChangePct: decimal("nav_change_pct", { precision: 8, scale: 4 }),
  aumCr: decimal("aum_cr", { precision: 15, scale: 2 }),
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => {
  return {
    pk: uniqueIndex("nav_pk").on(table.fundId, table.navDate)
  };
});

export const insertNavDataSchema = createInsertSchema(navData).omit({
  createdAt: true
});

export type InsertNavData = z.infer<typeof insertNavDataSchema>;
export type NavData = typeof navData.$inferSelect;

// Fund scores and rankings
export const fundScores = pgTable("fund_scores", {
  fundId: integer("fund_id").references(() => funds.id),
  scoreDate: date("score_date").notNull(),
  
  // Raw return data (actual percentages)
  return1m: decimal("return_1m", { precision: 6, scale: 2 }),
  return3m: decimal("return_3m", { precision: 6, scale: 2 }),
  return6m: decimal("return_6m", { precision: 6, scale: 2 }),
  return1y: decimal("return_1y", { precision: 6, scale: 2 }),
  return3y: decimal("return_3y", { precision: 6, scale: 2 }),
  return5y: decimal("return_5y", { precision: 6, scale: 2 }),
  
  // Risk metrics - raw values
  volatility1y: decimal("volatility_1y", { precision: 6, scale: 4 }),
  volatility3y: decimal("volatility_3y", { precision: 6, scale: 4 }),
  sharpeRatio1y: decimal("sharpe_ratio_1y", { precision: 5, scale: 2 }),
  sharpeRatio3y: decimal("sharpe_ratio_3y", { precision: 5, scale: 2 }),
  sortinoRatio1y: decimal("sortino_ratio_1y", { precision: 5, scale: 2 }),
  sortinoRatio3y: decimal("sortino_ratio_3y", { precision: 5, scale: 2 }),
  maxDrawdown: decimal("max_drawdown", { precision: 5, scale: 2 }),
  upCaptureRatio: decimal("up_capture_ratio", { precision: 5, scale: 2 }),
  downCaptureRatio: decimal("down_capture_ratio", { precision: 5, scale: 2 }),
  
  // Quality metrics - raw values
  consistencyScore: decimal("consistency_score", { precision: 4, scale: 2 }),
  categoryMedianExpenseRatio: decimal("category_median_expense_ratio", { precision: 4, scale: 2 }),
  categoryStdDevExpenseRatio: decimal("category_std_dev_expense_ratio", { precision: 4, scale: 2 }),
  expenseRatioRank: decimal("expense_ratio_rank", { precision: 5, scale: 2 }),
  fundAum: decimal("fund_aum", { precision: 12, scale: 2 }),
  categoryMedianAum: decimal("category_median_aum", { precision: 12, scale: 2 }),
  fundSizeFactor: decimal("fund_size_factor", { precision: 4, scale: 2 }),
  
  // Context fields
  riskFreeRate: decimal("risk_free_rate", { precision: 4, scale: 2 }),
  categoryBenchmarkReturn1y: decimal("category_benchmark_return_1y", { precision: 6, scale: 2 }),
  categoryBenchmarkReturn3y: decimal("category_benchmark_return_3y", { precision: 6, scale: 2 }),
  medianReturns1y: decimal("median_returns_1y", { precision: 6, scale: 2 }),
  medianReturns3y: decimal("median_returns_3y", { precision: 6, scale: 2 }),
  aboveMedianMonthsCount: integer("above_median_months_count"),
  totalMonthsEvaluated: integer("total_months_evaluated"),
  
  // Historical returns scores (40 points)
  return3mScore: decimal("return_3m_score", { precision: 4, scale: 1 }),
  return6mScore: decimal("return_6m_score", { precision: 4, scale: 1 }),
  return1yScore: decimal("return_1y_score", { precision: 4, scale: 1 }),
  return3yScore: decimal("return_3y_score", { precision: 4, scale: 1 }),
  return5yScore: decimal("return_5y_score", { precision: 4, scale: 1 }),
  returnYtdScore: decimal("return_ytd_score", { precision: 4, scale: 1 }),
  historicalReturnsTotal: decimal("historical_returns_total", { precision: 5, scale: 1 }),
  
  // Risk grade scores (30 points)
  stdDev1yScore: decimal("std_dev_1y_score", { precision: 4, scale: 1 }),
  stdDev3yScore: decimal("std_dev_3y_score", { precision: 4, scale: 1 }),
  updownCapture1yScore: decimal("updown_capture_1y_score", { precision: 4, scale: 1 }),
  updownCapture3yScore: decimal("updown_capture_3y_score", { precision: 4, scale: 1 }),
  maxDrawdownScore: decimal("max_drawdown_score", { precision: 4, scale: 1 }),
  riskGradeTotal: decimal("risk_grade_total", { precision: 5, scale: 1 }),
  
  // Other metrics scores (30 points)
  sectoralSimilarityScore: decimal("sectoral_similarity_score", { precision: 4, scale: 1 }),
  forwardScore: decimal("forward_score", { precision: 4, scale: 1 }),
  aumSizeScore: decimal("aum_size_score", { precision: 4, scale: 1 }),
  expenseRatioScore: decimal("expense_ratio_score", { precision: 4, scale: 1 }),
  otherMetricsTotal: decimal("other_metrics_total", { precision: 5, scale: 1 }),
  
  // Final scoring
  totalScore: decimal("total_score", { precision: 5, scale: 1 }).notNull(),
  quartile: integer("quartile").notNull(),
  categoryRank: integer("category_rank"),
  categoryTotal: integer("category_total"),
  recommendation: text("recommendation").notNull(),
  
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => {
  return {
    pk: uniqueIndex("fund_scores_pk").on(table.fundId, table.scoreDate)
  };
});

export const insertFundScoreSchema = createInsertSchema(fundScores).omit({
  createdAt: true
});

export type InsertFundScore = z.infer<typeof insertFundScoreSchema>;
export type FundScore = typeof fundScores.$inferSelect;

// Portfolio holdings
export const portfolioHoldings = pgTable("portfolio_holdings", {
  id: serial("id").primaryKey(),
  fundId: integer("fund_id").references(() => funds.id),
  holdingDate: date("holding_date").notNull(),
  stockSymbol: text("stock_symbol"),
  stockName: text("stock_name"),
  holdingPercent: decimal("holding_percent", { precision: 5, scale: 2 }),
  marketValueCr: decimal("market_value_cr", { precision: 12, scale: 2 }),
  sector: text("sector"),
  industry: text("industry"),
  marketCapCategory: text("market_cap_category"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertPortfolioHoldingSchema = createInsertSchema(portfolioHoldings).omit({
  id: true,
  createdAt: true
});

export type InsertPortfolioHolding = z.infer<typeof insertPortfolioHoldingSchema>;
export type PortfolioHolding = typeof portfolioHoldings.$inferSelect;

// Market indices
export const marketIndices = pgTable("market_indices", {
  indexName: text("index_name").notNull(),
  indexDate: date("index_date").notNull(),
  openValue: decimal("open_value", { precision: 12, scale: 2 }),
  highValue: decimal("high_value", { precision: 12, scale: 2 }),
  lowValue: decimal("low_value", { precision: 12, scale: 2 }),
  closeValue: decimal("close_value", { precision: 12, scale: 2 }),
  volume: integer("volume"),
  marketCap: decimal("market_cap", { precision: 18, scale: 2 }),
  peRatio: decimal("pe_ratio", { precision: 6, scale: 2 }),
  pbRatio: decimal("pb_ratio", { precision: 6, scale: 2 }),
  dividendYield: decimal("dividend_yield", { precision: 4, scale: 2 }),
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => {
  return {
    pk: uniqueIndex("market_indices_pk").on(table.indexName, table.indexDate)
  };
});

export const insertMarketIndexSchema = createInsertSchema(marketIndices).omit({
  createdAt: true
});

export type InsertMarketIndex = z.infer<typeof insertMarketIndexSchema>;
export type MarketIndex = typeof marketIndices.$inferSelect;

// ELIVATE framework data
export const elivateScores = pgTable("elivate_scores", {
  id: serial("id").primaryKey(),
  scoreDate: date("score_date").notNull().unique(),
  
  // External Influence (20 points)
  usGdpGrowth: decimal("us_gdp_growth", { precision: 5, scale: 2 }),
  fedFundsRate: decimal("fed_funds_rate", { precision: 4, scale: 2 }),
  dxyIndex: decimal("dxy_index", { precision: 6, scale: 2 }),
  chinaPmi: decimal("china_pmi", { precision: 4, scale: 1 }),
  externalInfluenceScore: decimal("external_influence_score", { precision: 4, scale: 1 }),
  
  // Local Story (20 points)
  indiaGdpGrowth: decimal("india_gdp_growth", { precision: 5, scale: 2 }),
  gstCollectionCr: decimal("gst_collection_cr", { precision: 10, scale: 2 }),
  iipGrowth: decimal("iip_growth", { precision: 5, scale: 2 }),
  indiaPmi: decimal("india_pmi", { precision: 4, scale: 1 }),
  localStoryScore: decimal("local_story_score", { precision: 4, scale: 1 }),
  
  // Inflation & Rates (20 points)
  cpiInflation: decimal("cpi_inflation", { precision: 4, scale: 2 }),
  wpiInflation: decimal("wpi_inflation", { precision: 4, scale: 2 }),
  repoRate: decimal("repo_rate", { precision: 4, scale: 2 }),
  tenYearYield: decimal("ten_year_yield", { precision: 4, scale: 2 }),
  inflationRatesScore: decimal("inflation_rates_score", { precision: 4, scale: 1 }),
  
  // Valuation & Earnings (20 points)
  niftyPe: decimal("nifty_pe", { precision: 5, scale: 2 }),
  niftyPb: decimal("nifty_pb", { precision: 4, scale: 2 }),
  earningsGrowth: decimal("earnings_growth", { precision: 5, scale: 2 }),
  valuationEarningsScore: decimal("valuation_earnings_score", { precision: 4, scale: 1 }),
  
  // Allocation of Capital (10 points)
  fiiFlowsCr: decimal("fii_flows_cr", { precision: 8, scale: 2 }),
  diiFlowsCr: decimal("dii_flows_cr", { precision: 8, scale: 2 }),
  sipInflowsCr: decimal("sip_inflows_cr", { precision: 8, scale: 2 }),
  allocationCapitalScore: decimal("allocation_capital_score", { precision: 4, scale: 1 }),
  
  // Trends & Sentiments (10 points)
  stocksAbove200dmaPct: decimal("stocks_above_200dma_pct", { precision: 4, scale: 1 }),
  indiaVix: decimal("india_vix", { precision: 5, scale: 2 }),
  advanceDeclineRatio: decimal("advance_decline_ratio", { precision: 4, scale: 2 }),
  trendsSentimentsScore: decimal("trends_sentiments_score", { precision: 4, scale: 1 }),
  
  // Total ELIVATE score
  totalElivateScore: decimal("total_elivate_score", { precision: 5, scale: 1 }).notNull(),
  marketStance: text("market_stance").notNull(),
  
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertElivateScoreSchema = createInsertSchema(elivateScores).omit({
  id: true,
  createdAt: true
});

export type InsertElivateScore = z.infer<typeof insertElivateScoreSchema>;
export type ElivateScore = typeof elivateScores.$inferSelect;

// Model Portfolios
export const modelPortfolios = pgTable("model_portfolios", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  riskProfile: text("risk_profile").notNull(),
  elivateScoreId: integer("elivate_score_id").references(() => elivateScores.id),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const insertModelPortfolioSchema = createInsertSchema(modelPortfolios).omit({
  id: true,
  createdAt: true,
  updatedAt: true
});

export type InsertModelPortfolio = z.infer<typeof insertModelPortfolioSchema>;
export type ModelPortfolio = typeof modelPortfolios.$inferSelect;

// Model Portfolio Allocations
export const modelPortfolioAllocations = pgTable("model_portfolio_allocations", {
  id: serial("id").primaryKey(),
  portfolioId: integer("portfolio_id").references(() => modelPortfolios.id),
  fundId: integer("fund_id").references(() => funds.id),
  allocationPercent: decimal("allocation_percent", { precision: 5, scale: 2 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => {
  return {
    portfolioFundIdx: index("portfolio_fund_idx").on(table.portfolioId, table.fundId)
  };
});

export const insertModelPortfolioAllocationSchema = createInsertSchema(modelPortfolioAllocations).omit({
  id: true,
  createdAt: true
});

export type InsertModelPortfolioAllocation = z.infer<typeof insertModelPortfolioAllocationSchema>;
export type ModelPortfolioAllocation = typeof modelPortfolioAllocations.$inferSelect;

// ETL Pipeline Status
export const etlPipelineRuns = pgTable("etl_pipeline_runs", {
  id: serial("id").primaryKey(),
  pipelineName: text("pipeline_name").notNull(),
  status: text("status").notNull(),
  startTime: timestamp("start_time").notNull(),
  endTime: timestamp("end_time"),
  recordsProcessed: integer("records_processed"),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertEtlPipelineRunSchema = createInsertSchema(etlPipelineRuns).omit({
  id: true,
  createdAt: true
});

export type InsertEtlPipelineRun = z.infer<typeof insertEtlPipelineRunSchema>;
export type EtlPipelineRun = typeof etlPipelineRuns.$inferSelect;

// Fund Performance Metrics table
export const fundPerformanceMetrics = pgTable("fund_performance_metrics", {
  id: serial("id").primaryKey(),
  fundId: integer("fund_id").notNull().references(() => funds.id),
  calculationDate: timestamp("calculation_date").notNull(),
  returns1Y: decimal("returns_1y", { precision: 8, scale: 4 }),
  returns3Y: decimal("returns_3y", { precision: 8, scale: 4 }),
  returns5Y: decimal("returns_5y", { precision: 8, scale: 4 }),
  volatility: decimal("volatility", { precision: 8, scale: 4 }),
  sharpeRatio: decimal("sharpe_ratio", { precision: 8, scale: 4 }),
  maxDrawdown: decimal("max_drawdown", { precision: 8, scale: 4 }),
  consistencyScore: decimal("consistency_score", { precision: 6, scale: 4 }),
  alpha: decimal("alpha", { precision: 8, scale: 4 }),
  beta: decimal("beta", { precision: 8, scale: 4 }),
  informationRatio: decimal("information_ratio", { precision: 8, scale: 4 }),
  totalNavRecords: integer("total_nav_records").notNull(),
  dataQualityScore: decimal("data_quality_score", { precision: 6, scale: 4 }).notNull(),
  compositeScore: decimal("composite_score", { precision: 8, scale: 4 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => ({
  fundCalculationDateIdx: index("fund_performance_fund_calc_date_idx").on(table.fundId, table.calculationDate),
}));

export const insertFundPerformanceMetricsSchema = createInsertSchema(fundPerformanceMetrics).omit({
  id: true,
  createdAt: true,
});

export type InsertFundPerformanceMetrics = z.infer<typeof insertFundPerformanceMetricsSchema>;
export type FundPerformanceMetrics = typeof fundPerformanceMetrics.$inferSelect;

// Quartile Rankings table
export const quartileRankings = pgTable("quartile_rankings", {
  id: serial("id").primaryKey(),
  fundId: integer("fund_id").notNull().references(() => funds.id),
  category: text("category").notNull(),
  calculationDate: timestamp("calculation_date").notNull(),
  quartile: integer("quartile").notNull(), // 1, 2, 3, 4
  quartileLabel: text("quartile_label").notNull(), // 'BUY', 'HOLD', 'REVIEW', 'SELL'
  rank: integer("rank").notNull(),
  totalFunds: integer("total_funds").notNull(),
  percentile: decimal("percentile", { precision: 6, scale: 4 }).notNull(),
  compositeScore: decimal("composite_score", { precision: 8, scale: 4 }).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => ({
  fundCategoryDateIdx: index("quartile_fund_category_date_idx").on(table.fundId, table.category, table.calculationDate),
  categoryQuartileIdx: index("quartile_category_quartile_idx").on(table.category, table.quartile, table.calculationDate),
}));

export const insertQuartileRankingSchema = createInsertSchema(quartileRankings).omit({
  id: true,
  createdAt: true,
});

export type InsertQuartileRanking = z.infer<typeof insertQuartileRankingSchema>;
export type QuartileRanking = typeof quartileRankings.$inferSelect;

// AdvisorKhoj Data Tables
export const aumAnalytics = pgTable("aum_analytics", {
  id: serial("id").primaryKey(),
  amcName: text("amc_name"),
  fundName: text("fund_name"),
  aumCrores: decimal("aum_crores", { precision: 15, scale: 2 }),
  totalAumCrores: decimal("total_aum_crores", { precision: 15, scale: 2 }),
  fundCount: integer("fund_count"),
  category: text("category"),
  dataDate: date("data_date").notNull(),
  source: text("source").default("advisorkhoj"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});

export const insertAumAnalyticsSchema = createInsertSchema(aumAnalytics).omit({
  id: true,
  createdAt: true,
  updatedAt: true
});

export type InsertAumAnalytics = z.infer<typeof insertAumAnalyticsSchema>;
export type AumAnalytics = typeof aumAnalytics.$inferSelect;

export const portfolioOverlap = pgTable("portfolio_overlap", {
  id: serial("id").primaryKey(),
  fund1SchemeCode: text("fund1_scheme_code"),
  fund2SchemeCode: text("fund2_scheme_code"),
  fund1Name: text("fund1_name"),
  fund2Name: text("fund2_name"),
  overlapPercentage: decimal("overlap_percentage", { precision: 5, scale: 2 }),
  analysisDate: date("analysis_date").notNull(),
  source: text("source").default("advisorkhoj"),
  createdAt: timestamp("created_at").defaultNow()
});

export const insertPortfolioOverlapSchema = createInsertSchema(portfolioOverlap).omit({
  id: true,
  createdAt: true
});

export type InsertPortfolioOverlap = z.infer<typeof insertPortfolioOverlapSchema>;
export type PortfolioOverlap = typeof portfolioOverlap.$inferSelect;

export const managerAnalytics = pgTable("manager_analytics", {
  id: serial("id").primaryKey(),
  managerName: text("manager_name").notNull(),
  managedFundsCount: integer("managed_funds_count"),
  totalAumManaged: decimal("total_aum_managed", { precision: 15, scale: 2 }),
  avgPerformance1y: decimal("avg_performance_1y", { precision: 8, scale: 4 }),
  avgPerformance3y: decimal("avg_performance_3y", { precision: 8, scale: 4 }),
  analysisDate: date("analysis_date").notNull(),
  source: text("source").default("advisorkhoj"),
  createdAt: timestamp("created_at").defaultNow()
});

export const insertManagerAnalyticsSchema = createInsertSchema(managerAnalytics).omit({
  id: true,
  createdAt: true
});

export type InsertManagerAnalytics = z.infer<typeof insertManagerAnalyticsSchema>;
export type ManagerAnalytics = typeof managerAnalytics.$inferSelect;

export const categoryPerformance = pgTable("category_performance", {
  id: serial("id").primaryKey(),
  categoryName: text("category_name").notNull(),
  subcategory: text("subcategory"),
  avgReturn1y: decimal("avg_return_1y", { precision: 8, scale: 4 }),
  avgReturn3y: decimal("avg_return_3y", { precision: 8, scale: 4 }),
  avgReturn5y: decimal("avg_return_5y", { precision: 8, scale: 4 }),
  fundCount: integer("fund_count"),
  analysisDate: date("analysis_date").notNull(),
  source: text("source").default("advisorkhoj"),
  createdAt: timestamp("created_at").defaultNow()
});

export const insertCategoryPerformanceSchema = createInsertSchema(categoryPerformance).omit({
  id: true,
  createdAt: true
});

export type InsertCategoryPerformance = z.infer<typeof insertCategoryPerformanceSchema>;
export type CategoryPerformance = typeof categoryPerformance.$inferSelect;

// Fund Scores Corrected (Primary Scoring Table)
export const fundScoresCorrected = pgTable("fund_scores_corrected", {
  fundId: integer("fund_id").references(() => funds.id),
  scoreDate: date("score_date").notNull(),
  
  // Return metrics (absolute values)
  return1mAbsolute: decimal("return_1m_absolute", { precision: 8, scale: 4 }),
  return3mAbsolute: decimal("return_3m_absolute", { precision: 8, scale: 4 }),
  return6mAbsolute: decimal("return_6m_absolute", { precision: 8, scale: 4 }),
  return1yAbsolute: decimal("return_1y_absolute", { precision: 8, scale: 4 }),
  return3yAbsolute: decimal("return_3y_absolute", { precision: 8, scale: 4 }),
  return5yAbsolute: decimal("return_5y_absolute", { precision: 8, scale: 4 }),
  returnYtdAbsolute: decimal("return_ytd_absolute", { precision: 8, scale: 4 }),
  
  // Risk metrics
  volatility1yPercent: decimal("volatility_1y_percent", { precision: 8, scale: 4 }),
  maxDrawdown: decimal("max_drawdown", { precision: 8, scale: 4 }),
  
  // Advanced risk metrics
  calmarRatio1y: decimal("calmar_ratio_1y", { precision: 8, scale: 4 }),
  sortinoRatio1y: decimal("sortino_ratio_1y", { precision: 8, scale: 4 }),
  var95_1y: decimal("var_95_1y", { precision: 8, scale: 4 }),
  downsideDeviation1y: decimal("downside_deviation_1y", { precision: 8, scale: 4 }),
  trackingError1y: decimal("tracking_error_1y", { precision: 8, scale: 4 }),
  
  // Score components
  historicalReturnsTotal: decimal("historical_returns_total", { precision: 5, scale: 1 }),
  riskGradeTotal: decimal("risk_grade_total", { precision: 5, scale: 1 }),
  fundamentalsTotal: decimal("fundamentals_total", { precision: 5, scale: 1 }),
  otherMetricsTotal: decimal("other_metrics_total", { precision: 5, scale: 1 }),
  
  // Individual scores
  return1yScore: decimal("return_1y_score", { precision: 4, scale: 1 }),
  return3yScore: decimal("return_3y_score", { precision: 4, scale: 1 }),
  return5yScore: decimal("return_5y_score", { precision: 4, scale: 1 }),
  
  // Final results
  totalScore: decimal("total_score", { precision: 5, scale: 1 }).notNull(),
  quartile: integer("quartile").notNull(),
  recommendation: text("recommendation").notNull(),
  
  // Category and subcategory analysis
  category: text("category"),
  subcategory: text("subcategory"),
  subcategoryRank: integer("subcategory_rank"),
  subcategoryTotal: integer("subcategory_total"),
  subcategoryPercentile: decimal("subcategory_percentile", { precision: 5, scale: 2 }),
  subcategoryQuartile: integer("subcategory_quartile"),
  
  createdAt: timestamp("created_at").defaultNow(),
}, (table) => {
  return {
    pk: uniqueIndex("fund_scores_corrected_pk").on(table.fundId, table.scoreDate)
  };
});

export const insertFundScoreCorrectedSchema = createInsertSchema(fundScoresCorrected).omit({
  createdAt: true
});

export type InsertFundScoreCorrected = z.infer<typeof insertFundScoreCorrectedSchema>;
export type FundScoreCorrected = typeof fundScoresCorrected.$inferSelect;

// Backtesting Results
export const backtestingResults = pgTable("backtesting_results", {
  id: serial("id").primaryKey(),
  fundId: integer("fund_id").references(() => funds.id),
  validationDate: date("validation_date").notNull(),
  historicalScoreDate: date("historical_score_date").notNull(),
  historicalTotalScore: decimal("historical_total_score", { precision: 5, scale: 1 }),
  historicalRecommendation: text("historical_recommendation"),
  historicalQuartile: integer("historical_quartile"),
  actualReturn3m: decimal("actual_return_3m", { precision: 8, scale: 4 }),
  actualReturn6m: decimal("actual_return_6m", { precision: 8, scale: 4 }),
  actualReturn1y: decimal("actual_return_1y", { precision: 8, scale: 4 }),
  predictedPerformance: text("predicted_performance"),
  actualPerformance: text("actual_performance"),
  predictionAccuracy: boolean("prediction_accuracy"),
  quartileMaintained: boolean("quartile_maintained"),
  scoreAccuracy3m: decimal("score_accuracy_3m", { precision: 6, scale: 2 }),
  scoreAccuracy6m: decimal("score_accuracy_6m", { precision: 6, scale: 2 }),
  scoreAccuracy1y: decimal("score_accuracy_1y", { precision: 6, scale: 2 }),
  quartileAccuracyScore: decimal("quartile_accuracy_score", { precision: 6, scale: 4 }),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertBacktestingResultSchema = createInsertSchema(backtestingResults).omit({
  id: true,
  createdAt: true
});

export type InsertBacktestingResult = z.infer<typeof insertBacktestingResultSchema>;
export type BacktestingResult = typeof backtestingResults.$inferSelect;

// Validation Summary Reports
export const validationSummaryReports = pgTable("validation_summary_reports", {
  id: serial("id").primaryKey(),
  validationRunId: text("validation_run_id").notNull().unique(),
  runDate: date("run_date").notNull(),
  totalFundsTested: integer("total_funds_tested").notNull(),
  validationPeriodMonths: integer("validation_period_months").notNull(),
  overallPredictionAccuracy3m: decimal("overall_prediction_accuracy_3m", { precision: 5, scale: 2 }),
  overallPredictionAccuracy6m: decimal("overall_prediction_accuracy_6m", { precision: 5, scale: 2 }),
  overallPredictionAccuracy1y: decimal("overall_prediction_accuracy_1y", { precision: 5, scale: 2 }),
  overallScoreCorrelation3m: decimal("overall_score_correlation_3m", { precision: 5, scale: 2 }),
  overallScoreCorrelation6m: decimal("overall_score_correlation_6m", { precision: 5, scale: 2 }),
  overallScoreCorrelation1y: decimal("overall_score_correlation_1y", { precision: 5, scale: 2 }),
  quartileStability3m: decimal("quartile_stability_3m", { precision: 5, scale: 2 }),
  quartileStability6m: decimal("quartile_stability_6m", { precision: 5, scale: 2 }),
  quartileStability1y: decimal("quartile_stability_1y", { precision: 5, scale: 2 }),
  strongBuyAccuracy: decimal("strong_buy_accuracy", { precision: 5, scale: 2 }),
  buyAccuracy: decimal("buy_accuracy", { precision: 5, scale: 2 }),
  holdAccuracy: decimal("hold_accuracy", { precision: 5, scale: 2 }),
  sellAccuracy: decimal("sell_accuracy", { precision: 5, scale: 2 }),
  strongSellAccuracy: decimal("strong_sell_accuracy", { precision: 5, scale: 2 }),
  validationStatus: text("validation_status").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertValidationSummaryReportSchema = createInsertSchema(validationSummaryReports).omit({
  id: true,
  createdAt: true
});

export type InsertValidationSummaryReport = z.infer<typeof insertValidationSummaryReportSchema>;
export type ValidationSummaryReport = typeof validationSummaryReports.$inferSelect;
