import { storage } from '../storage';
import { pool } from '../db';
import type { InsertElivateScore } from '@shared/schema';

// ELIVATE Framework Service
export class ElivateFramework {
  private static instance: ElivateFramework;
  
  private constructor() {}
  
  public static getInstance(): ElivateFramework {
    if (!ElivateFramework.instance) {
      ElivateFramework.instance = new ElivateFramework();
    }
    return ElivateFramework.instance;
  }
  
  // Calculate ELIVATE score
  async calculateElivateScore(): Promise<{ id: number; totalElivateScore: number; marketStance: string }> {
    try {
      // Fetch latest market data from our database
      const latestIndices = await storage.getLatestMarketIndices();
      
      console.log("Fetched latest market indices for ELIVATE calculation:", 
        latestIndices.map(i => `${i.indexName}: ${i.closeValue}`).join(', '));
      
      if (!latestIndices || latestIndices.length === 0) {
        throw new Error("Failed to fetch market indices data for ELIVATE calculation");
      }
      
      // Use real financial data from our database
      // This data comes from authorized sources via our ETL pipelines
      
      // External Influence (20 points)
      const externalInfluenceScore = await this.calculateExternalInfluenceScore();
      
      // Local Story (20 points)
      const localStoryScore = await this.calculateLocalStoryScore();
      
      // Inflation & Rates (20 points)
      const inflationRatesScore = await this.calculateInflationRatesScore();
      
      // Valuation & Earnings (20 points)
      const valuationEarningsScore = await this.calculateValuationEarningsScore(latestIndices);
      
      // Allocation of Capital (10 points)
      const allocationCapitalScore = await this.calculateAllocationCapitalScore();
      
      // Trends & Sentiments (10 points)
      const trendsSentimentsScore = await this.calculateTrendsSentimentsScore(latestIndices);
      
      // Calculate total ELIVATE score
      const totalElivateScore = 
        externalInfluenceScore.score + 
        localStoryScore.score + 
        inflationRatesScore.score + 
        valuationEarningsScore.score + 
        allocationCapitalScore.score + 
        trendsSentimentsScore.score;
      
      // Determine market stance
      const marketStance = this.determineMarketStance(totalElivateScore);
      
      // Today's date for score
      const today = new Date();
      const formattedDate = today.toISOString().split('T')[0];
      
      console.log(`Calculating ELIVATE score for ${formattedDate} with total score: ${totalElivateScore}`);
      
      // Create ELIVATE score record
      const elivateScoreData: InsertElivateScore = {
        scoreDate: formattedDate,
        
        // External Influence
        usGdpGrowth: externalInfluenceScore.usGdpGrowth.toString(),
        fedFundsRate: externalInfluenceScore.fedFundsRate.toString(),
        dxyIndex: externalInfluenceScore.dxyIndex.toString(),
        chinaPmi: externalInfluenceScore.chinaPmi.toString(),
        externalInfluenceScore: externalInfluenceScore.score.toString(),
        
        // Local Story
        indiaGdpGrowth: localStoryScore.indiaGdpGrowth.toString(),
        gstCollectionCr: localStoryScore.gstCollectionCr.toString(),
        iipGrowth: localStoryScore.iipGrowth.toString(),
        indiaPmi: localStoryScore.indiaPmi.toString(),
        localStoryScore: localStoryScore.score.toString(),
        
        // Inflation & Rates
        cpiInflation: inflationRatesScore.cpiInflation.toString(),
        wpiInflation: inflationRatesScore.wpiInflation.toString(),
        repoRate: inflationRatesScore.repoRate.toString(),
        tenYearYield: inflationRatesScore.tenYearYield.toString(),
        inflationRatesScore: inflationRatesScore.score.toString(),
        
        // Valuation & Earnings
        niftyPe: valuationEarningsScore.niftyPe.toString(),
        niftyPb: valuationEarningsScore.niftyPb.toString(),
        earningsGrowth: valuationEarningsScore.earningsGrowth.toString(),
        valuationEarningsScore: valuationEarningsScore.score.toString(),
        
        // Allocation of Capital
        fiiFlowsCr: allocationCapitalScore.fiiFlowsCr.toString(),
        diiFlowsCr: allocationCapitalScore.diiFlowsCr.toString(),
        sipInflowsCr: allocationCapitalScore.sipInflowsCr.toString(),
        allocationCapitalScore: allocationCapitalScore.score.toString(),
        
        // Trends & Sentiments
        stocksAbove200dmaPct: trendsSentimentsScore.stocksAbove200dmaPct.toString(),
        indiaVix: trendsSentimentsScore.indiaVix.toString(),
        advanceDeclineRatio: trendsSentimentsScore.advanceDeclineRatio.toString(),
        trendsSentimentsScore: trendsSentimentsScore.score.toString(),
        
        // Total score and stance
        totalElivateScore: totalElivateScore.toString(),
        marketStance,
      };
      
      // First check if we already have a score for today
      const existingScore = await storage.getElivateScore(undefined, today);
      let savedScore;
      
      if (existingScore) {
        // Update existing score instead of creating a new one to avoid uniqueness constraint violations
        console.log(`Updating existing ELIVATE score (ID: ${existingScore.id}) for ${formattedDate}`);
        savedScore = await pool.query(
          `UPDATE elivate_scores SET
           us_gdp_growth = $1, fed_funds_rate = $2, dxy_index = $3, china_pmi = $4, external_influence_score = $5,
           india_gdp_growth = $6, gst_collection_cr = $7, iip_growth = $8, india_pmi = $9, local_story_score = $10,
           cpi_inflation = $11, wpi_inflation = $12, repo_rate = $13, ten_year_yield = $14, inflation_rates_score = $15,
           nifty_pe = $16, nifty_pb = $17, earnings_growth = $18, valuation_earnings_score = $19,
           fii_flows_cr = $20, dii_flows_cr = $21, sip_inflows_cr = $22, allocation_capital_score = $23,
           stocks_above_200dma_pct = $24, india_vix = $25, advance_decline_ratio = $26, trends_sentiments_score = $27,
           total_elivate_score = $28, market_stance = $29
           WHERE id = $30
           RETURNING *`,
          [
            elivateScoreData.usGdpGrowth, elivateScoreData.fedFundsRate, elivateScoreData.dxyIndex, elivateScoreData.chinaPmi, elivateScoreData.externalInfluenceScore,
            elivateScoreData.indiaGdpGrowth, elivateScoreData.gstCollectionCr, elivateScoreData.iipGrowth, elivateScoreData.indiaPmi, elivateScoreData.localStoryScore,
            elivateScoreData.cpiInflation, elivateScoreData.wpiInflation, elivateScoreData.repoRate, elivateScoreData.tenYearYield, elivateScoreData.inflationRatesScore,
            elivateScoreData.niftyPe, elivateScoreData.niftyPb, elivateScoreData.earningsGrowth, elivateScoreData.valuationEarningsScore,
            elivateScoreData.fiiFlowsCr, elivateScoreData.diiFlowsCr, elivateScoreData.sipInflowsCr, elivateScoreData.allocationCapitalScore,
            elivateScoreData.stocksAbove200dmaPct, elivateScoreData.indiaVix, elivateScoreData.advanceDeclineRatio, elivateScoreData.trendsSentimentsScore,
            elivateScoreData.totalElivateScore, elivateScoreData.marketStance,
            existingScore.id
          ]
        );
        savedScore = savedScore.rows[0];
      } else {
        // Create a new score if one doesn't exist for today
        console.log(`Creating new ELIVATE score for ${formattedDate}`);
        savedScore = await storage.createElivateScore(elivateScoreData);
      }
      
      return {
        id: savedScore.id,
        totalElivateScore,
        marketStance
      };
    } catch (error) {
      console.error('Error calculating ELIVATE score:', error);
      throw error;
    }
  }
  
  // E - External Influence (20 points)
  private async calculateExternalInfluenceScore(): Promise<{
    usGdpGrowth: number;
    fedFundsRate: number;
    dxyIndex: number;
    chinaPmi: number;
    score: number;
  }> {
    console.log("Fetching external influence data from API sources");
    
    // Fetch latest data from our market indices table which gets data from real APIs
    const dxyData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'US DOLLAR INDEX' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const fedData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'US FED RATE' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const usGdpData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'US GDP GROWTH' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const chinaPmiData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'CHINA PMI' 
      ORDER BY index_date DESC LIMIT 1`);
    
    // Only use authentic data - reject synthetic values
    if (usGdpData.rows.length === 0 || fedData.rows.length === 0 || dxyData.rows.length === 0 || chinaPmiData.rows.length === 0) {
      throw new Error("ELIVATE Framework requires authentic market data. Missing external influence data from authorized sources.");
    }
    
    const usGdpGrowth = parseFloat(usGdpData.rows[0].close_value);
    const fedFundsRate = parseFloat(fedData.rows[0].close_value);
    const dxyIndex = parseFloat(dxyData.rows[0].close_value);
    const chinaPmi = parseFloat(chinaPmiData.rows[0].close_value);
    
    // Calculate component scores (each out of their proportion of 20 points)
    const usGdpScore = this.scoreGdpGrowth(usGdpGrowth, 5);
    const fedRateScore = this.scoreFedRate(fedFundsRate, 5);
    const dxyScore = this.scoreDxyIndex(dxyIndex, 5);
    const chinaPmiScore = this.scorePmi(chinaPmi, 5);
    
    // Total score (out of 20)
    const score = usGdpScore + fedRateScore + dxyScore + chinaPmiScore;
    
    return {
      usGdpGrowth,
      fedFundsRate,
      dxyIndex,
      chinaPmi,
      score
    };
  }
  
  // L - Local Story (20 points)
  private async calculateLocalStoryScore(): Promise<{
    indiaGdpGrowth: number;
    gstCollectionCr: number;
    iipGrowth: number;
    indiaPmi: number;
    score: number;
  }> {
    console.log("Fetching local economic story data from API sources");
    
    // Fetch latest data from market indices table which pulls from real economic APIs
    const gdpData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'INDIA GDP GROWTH' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const gstData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'GST COLLECTION' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const iipData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'IIP GROWTH' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const pmiData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'INDIA PMI' 
      ORDER BY index_date DESC LIMIT 1`);
    
    // Only use authentic data - reject synthetic values
    if (gdpData.rows.length === 0 || gstData.rows.length === 0 || iipData.rows.length === 0 || pmiData.rows.length === 0) {
      throw new Error("ELIVATE Framework requires authentic market data. Missing local story data from authorized sources.");
    }
    
    const indiaGdpGrowth = parseFloat(gdpData.rows[0].close_value);
    const gstCollectionCr = parseFloat(gstData.rows[0].close_value);
    const iipGrowth = parseFloat(iipData.rows[0].close_value);
    const indiaPmi = parseFloat(pmiData.rows[0].close_value);
    
    // Calculate component scores
    const gdpScore = this.scoreGdpGrowth(indiaGdpGrowth, 7);
    const gstScore = this.scoreGstCollection(gstCollectionCr, 5);
    const iipScore = this.scoreIipGrowth(iipGrowth, 3);
    const pmiScore = this.scorePmi(indiaPmi, 5);
    
    // Total score (out of 20)
    const score = gdpScore + gstScore + iipScore + pmiScore;
    
    return {
      indiaGdpGrowth,
      gstCollectionCr,
      iipGrowth,
      indiaPmi,
      score
    };
  }
  
  // I - Inflation & Rates (20 points)
  private async calculateInflationRatesScore(): Promise<{
    cpiInflation: number;
    wpiInflation: number;
    repoRate: number;
    tenYearYield: number;
    score: number;
  }> {
    console.log("Fetching inflation and rates data from RBI API sources");
    
    // Fetch latest data from market indices table which gets data from RBI API
    const cpiData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'CPI INFLATION' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const wpiData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'WPI INFLATION' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const repoData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'REPO RATE' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const yieldData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = '10Y GSEC YIELD' 
      ORDER BY index_date DESC LIMIT 1`);
    
    // Only use authentic data - reject synthetic values
    if (cpiData.rows.length === 0 || wpiData.rows.length === 0 || repoData.rows.length === 0 || yieldData.rows.length === 0) {
      throw new Error("ELIVATE Framework requires authentic market data. Missing inflation and rates data from authorized sources.");
    }
    
    const cpiInflation = parseFloat(cpiData.rows[0].close_value);
    const wpiInflation = parseFloat(wpiData.rows[0].close_value);
    const repoRate = parseFloat(repoData.rows[0].close_value);
    const tenYearYield = parseFloat(yieldData.rows[0].close_value);
    
    // Calculate component scores
    const cpiScore = this.scoreInflation(cpiInflation, 6);
    const wpiScore = this.scoreInflation(wpiInflation, 4);
    const repoScore = this.scoreRepoRate(repoRate, 5);
    const yieldScore = this.scoreYield(tenYearYield, repoRate, 5);
    
    // Total score (out of 20)
    const score = cpiScore + wpiScore + repoScore + yieldScore;
    
    return {
      cpiInflation,
      wpiInflation,
      repoRate,
      tenYearYield,
      score
    };
  }
  
  // V - Valuation & Earnings (20 points)
  private async calculateValuationEarningsScore(latestIndices: any[]): Promise<{
    niftyPe: number;
    niftyPb: number;
    earningsGrowth: number;
    score: number;
  }> {
    // Extract Nifty 50 data if available
    const nifty50 = latestIndices.find(index => index.indexName === 'NIFTY 50');
    
    // Only use authentic data - reject synthetic values
    if (!nifty50 || !nifty50.peRatio || !nifty50.pbRatio) {
      throw new Error("ELIVATE Framework requires authentic market data. Missing Nifty 50 valuation data from authorized sources.");
    }
    
    const niftyPe = nifty50.peRatio;
    const niftyPb = nifty50.pbRatio;
    
    // Fetch earnings growth from market data
    const earningsData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'EARNINGS GROWTH' 
      ORDER BY index_date DESC LIMIT 1`);
    
    if (earningsData.rows.length === 0) {
      throw new Error("ELIVATE Framework requires authentic market data. Missing earnings growth data from authorized sources.");
    }
    
    const earningsGrowth = parseFloat(earningsData.rows[0].close_value);
    
    // Calculate component scores
    const peScore = this.scorePeRatio(niftyPe, 8);
    const pbScore = this.scorePbRatio(niftyPb, 4);
    const earningsScore = this.scoreEarningsGrowth(earningsGrowth, 8);
    
    // Total score (out of 20)
    const score = peScore + pbScore + earningsScore;
    
    return {
      niftyPe,
      niftyPb,
      earningsGrowth,
      score
    };
  }
  
  // A - Allocation of Capital (10 points)
  private async calculateAllocationCapitalScore(): Promise<{
    fiiFlowsCr: number;
    diiFlowsCr: number;
    sipInflowsCr: number;
    score: number;
  }> {
    console.log("Fetching capital allocation data from AMFI/SEBI API sources");
    
    // Fetch latest data from market indices table which gets data from SEBI/AMFI APIs
    const fiiData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'FII FLOWS' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const diiData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'DII FLOWS' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const sipData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'SIP INFLOWS' 
      ORDER BY index_date DESC LIMIT 1`);
    
    // Only use authentic data - no fallback values
    if (fiiData.rows.length === 0 || diiData.rows.length === 0 || sipData.rows.length === 0) {
      throw new Error('Authentic market data required - please ensure FII/DII/SIP data is available from authorized sources');
    }
    
    const fiiFlowsCr = parseFloat(fiiData.rows[0].close_value);
    const diiFlowsCr = parseFloat(diiData.rows[0].close_value);
    const sipInflowsCr = parseFloat(sipData.rows[0].close_value);
    
    // Calculate component scores
    const fiiScore = this.scoreInvestorFlows(fiiFlowsCr, 3);
    const diiScore = this.scoreInvestorFlows(diiFlowsCr, 3);
    const sipScore = this.scoreSipFlows(sipInflowsCr, 4);
    
    // Total score (out of 10)
    const score = fiiScore + diiScore + sipScore;
    
    return {
      fiiFlowsCr,
      diiFlowsCr,
      sipInflowsCr,
      score
    };
  }
  
  // T - Trends & Sentiments (10 points)
  private async calculateTrendsSentimentsScore(latestIndices: any[]): Promise<{
    stocksAbove200dmaPct: number;
    indiaVix: number;
    advanceDeclineRatio: number;
    score: number;
  }> {
    console.log("Fetching trends and sentiments data from authorized market sources");
    
    // Fetch authentic data from market indices table
    const dmaData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'STOCKS ABOVE 200DMA' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const vixData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'INDIA VIX' 
      ORDER BY index_date DESC LIMIT 1`);
    
    const adRatioData = await pool.query(`
      SELECT close_value FROM market_indices 
      WHERE index_name = 'ADVANCE DECLINE RATIO' 
      ORDER BY index_date DESC LIMIT 1`);
    
    // Only use authentic data - reject synthetic values
    if (dmaData.rows.length === 0 || vixData.rows.length === 0 || adRatioData.rows.length === 0) {
      throw new Error("ELIVATE Framework requires authentic market data. Missing trends and sentiments data from authorized sources.");
    }
    
    const stocksAbove200dmaPct = parseFloat(dmaData.rows[0].close_value);
    const indiaVix = parseFloat(vixData.rows[0].close_value);
    const advanceDeclineRatio = parseFloat(adRatioData.rows[0].close_value);
    
    // Calculate component scores
    const dmaScore = this.scoreDmaPercent(stocksAbove200dmaPct, 4);
    const vixScore = this.scoreVix(indiaVix, 3);
    const adRatioScore = this.scoreAdvanceDecline(advanceDeclineRatio, 3);
    
    // Total score (out of 10)
    const score = dmaScore + vixScore + adRatioScore;
    
    return {
      stocksAbove200dmaPct,
      indiaVix,
      advanceDeclineRatio,
      score
    };
  }
  
  // Determine market stance based on total score
  private determineMarketStance(totalScore: number): string {
    if (totalScore >= 75) {
      return 'BULLISH';
    } else if (totalScore >= 50) {
      return 'NEUTRAL';
    } else {
      return 'BEARISH';
    }
  }
  
  // Scoring helper functions
  
  private scoreGdpGrowth(gdpGrowth: number, maxPoints: number): number {
    if (gdpGrowth >= 6) return maxPoints;
    if (gdpGrowth >= 4) return maxPoints * 0.8;
    if (gdpGrowth >= 2) return maxPoints * 0.6;
    if (gdpGrowth >= 0) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreFedRate(rate: number, maxPoints: number): number {
    if (rate <= 3) return maxPoints;
    if (rate <= 4) return maxPoints * 0.8;
    if (rate <= 5) return maxPoints * 0.6;
    if (rate <= 6) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreDxyIndex(dxy: number, maxPoints: number): number {
    if (dxy <= 95) return maxPoints;
    if (dxy <= 100) return maxPoints * 0.8;
    if (dxy <= 105) return maxPoints * 0.6;
    if (dxy <= 110) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scorePmi(pmi: number, maxPoints: number): number {
    if (pmi >= 55) return maxPoints;
    if (pmi >= 52) return maxPoints * 0.8;
    if (pmi >= 50) return maxPoints * 0.6;
    if (pmi >= 48) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreGstCollection(gstCr: number, maxPoints: number): number {
    if (gstCr >= 170000) return maxPoints;
    if (gstCr >= 160000) return maxPoints * 0.8;
    if (gstCr >= 150000) return maxPoints * 0.6;
    if (gstCr >= 140000) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreIipGrowth(iip: number, maxPoints: number): number {
    if (iip >= 6) return maxPoints;
    if (iip >= 4) return maxPoints * 0.8;
    if (iip >= 2) return maxPoints * 0.6;
    if (iip >= 0) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreInflation(inflation: number, maxPoints: number): number {
    if (inflation <= 4) return maxPoints;
    if (inflation <= 5) return maxPoints * 0.8;
    if (inflation <= 6) return maxPoints * 0.6;
    if (inflation <= 7) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreRepoRate(rate: number, maxPoints: number): number {
    if (rate <= 5) return maxPoints;
    if (rate <= 6) return maxPoints * 0.8;
    if (rate <= 7) return maxPoints * 0.6;
    if (rate <= 8) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreYield(yield10y: number, repoRate: number, maxPoints: number): number {
    const spread = yield10y - repoRate;
    if (spread <= 0.25) return maxPoints;
    if (spread <= 0.5) return maxPoints * 0.8;
    if (spread <= 0.75) return maxPoints * 0.6;
    if (spread <= 1) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scorePeRatio(pe: number, maxPoints: number): number {
    if (pe <= 16) return maxPoints;
    if (pe <= 18) return maxPoints * 0.8;
    if (pe <= 20) return maxPoints * 0.6;
    if (pe <= 22) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scorePbRatio(pb: number, maxPoints: number): number {
    if (pb <= 2.5) return maxPoints;
    if (pb <= 3) return maxPoints * 0.8;
    if (pb <= 3.5) return maxPoints * 0.6;
    if (pb <= 4) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreEarningsGrowth(growth: number, maxPoints: number): number {
    if (growth >= 20) return maxPoints;
    if (growth >= 15) return maxPoints * 0.8;
    if (growth >= 10) return maxPoints * 0.6;
    if (growth >= 5) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreInvestorFlows(flows: number, maxPoints: number): number {
    if (flows >= 15000) return maxPoints;
    if (flows >= 10000) return maxPoints * 0.8;
    if (flows >= 5000) return maxPoints * 0.6;
    if (flows >= 0) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreSipFlows(sip: number, maxPoints: number): number {
    if (sip >= 17000) return maxPoints;
    if (sip >= 15000) return maxPoints * 0.8;
    if (sip >= 13000) return maxPoints * 0.6;
    if (sip >= 11000) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreDmaPercent(pct: number, maxPoints: number): number {
    if (pct >= 70) return maxPoints;
    if (pct >= 60) return maxPoints * 0.8;
    if (pct >= 50) return maxPoints * 0.6;
    if (pct >= 40) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreVix(vix: number, maxPoints: number): number {
    if (vix <= 12) return maxPoints;
    if (vix <= 15) return maxPoints * 0.8;
    if (vix <= 18) return maxPoints * 0.6;
    if (vix <= 21) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  private scoreAdvanceDecline(ratio: number, maxPoints: number): number {
    if (ratio >= 1.5) return maxPoints;
    if (ratio >= 1.2) return maxPoints * 0.8;
    if (ratio >= 1) return maxPoints * 0.6;
    if (ratio >= 0.8) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
}

// Export singleton instance
export const elivateFramework = ElivateFramework.getInstance();
