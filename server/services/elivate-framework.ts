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
      // Fetch latest market data
      const latestIndices = await storage.getLatestMarketIndices();
      
      // In a production environment, we would fetch all the required economic indicators
      // For this implementation, we're using simulated data
      
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
      
      // Create ELIVATE score record
      const elivateScoreData: InsertElivateScore = {
        scoreDate: new Date(),
        
        // External Influence
        usGdpGrowth: externalInfluenceScore.usGdpGrowth,
        fedFundsRate: externalInfluenceScore.fedFundsRate,
        dxyIndex: externalInfluenceScore.dxyIndex,
        chinaPmi: externalInfluenceScore.chinaPmi,
        externalInfluenceScore: externalInfluenceScore.score,
        
        // Local Story
        indiaGdpGrowth: localStoryScore.indiaGdpGrowth,
        gstCollectionCr: localStoryScore.gstCollectionCr,
        iipGrowth: localStoryScore.iipGrowth,
        indiaPmi: localStoryScore.indiaPmi,
        localStoryScore: localStoryScore.score,
        
        // Inflation & Rates
        cpiInflation: inflationRatesScore.cpiInflation,
        wpiInflation: inflationRatesScore.wpiInflation,
        repoRate: inflationRatesScore.repoRate,
        tenYearYield: inflationRatesScore.tenYearYield,
        inflationRatesScore: inflationRatesScore.score,
        
        // Valuation & Earnings
        niftyPe: valuationEarningsScore.niftyPe,
        niftyPb: valuationEarningsScore.niftyPb,
        earningsGrowth: valuationEarningsScore.earningsGrowth,
        valuationEarningsScore: valuationEarningsScore.score,
        
        // Allocation of Capital
        fiiFlowsCr: allocationCapitalScore.fiiFlowsCr,
        diiFlowsCr: allocationCapitalScore.diiFlowsCr,
        sipInflowsCr: allocationCapitalScore.sipInflowsCr,
        allocationCapitalScore: allocationCapitalScore.score,
        
        // Trends & Sentiments
        stocksAbove200dmaPct: trendsSentimentsScore.stocksAbove200dmaPct,
        indiaVix: trendsSentimentsScore.indiaVix,
        advanceDeclineRatio: trendsSentimentsScore.advanceDeclineRatio,
        trendsSentimentsScore: trendsSentimentsScore.score,
        
        // Total score and stance
        totalElivateScore,
        marketStance,
      };
      
      const savedScore = await storage.createElivateScore(elivateScoreData);
      
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
    // In a production environment, we would fetch real data
    // For this implementation, using simulated data
    
    const usGdpGrowth = 2.1;  // Positive but moderate growth
    const fedFundsRate = 5.25; // Current rate
    const dxyIndex = 102.5;    // USD strength
    const chinaPmi = 50.2;     // Slightly expansionary
    
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
    // Simulated data
    const indiaGdpGrowth = 6.8;      // Strong growth
    const gstCollectionCr = 175000;  // Good GST collections
    const iipGrowth = 4.2;          // Moderate IIP growth
    const indiaPmi = 57.5;          // Strong PMI
    
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
    // Simulated data
    const cpiInflation = 4.7;    // Moderate CPI inflation
    const wpiInflation = 3.1;    // Low WPI inflation
    const repoRate = 6.5;        // Current repo rate
    const tenYearYield = 7.1;    // 10-year G-Sec yield
    
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
    
    // Use actual data if available, otherwise use simulated data
    const niftyPe = nifty50?.peRatio ?? 20.5;  // Current P/E ratio
    const niftyPb = nifty50?.pbRatio ?? 3.2;   // Current P/B ratio
    const earningsGrowth = 15.3;              // YoY earnings growth
    
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
    // Simulated data
    const fiiFlowsCr = 15000;    // FII flows in crores (net)
    const diiFlowsCr = 12000;    // DII flows in crores (net)
    const sipInflowsCr = 18000;  // SIP inflows in crores
    
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
    // Extract VIX data if available
    const vixData = latestIndices.find(index => index.indexName === 'INDIA VIX');
    
    // Simulated data
    const stocksAbove200dmaPct = 65.3;  // % of Nifty 500 stocks above 200-DMA
    const indiaVix = vixData?.closeValue ?? 13.5;  // Current VIX level
    const advanceDeclineRatio = 1.2;   // Advance-decline ratio
    
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
