/**
 * Correct ELIVATE Scoring Structure Implementation
 * Uses authentic point-based system from Spark production plan
 * Total: 100 points (not percentage-based)
 */

import axios from 'axios';
import { executeRawQuery } from './server/db.ts';

class AuthenticELIVATECalculator {
  constructor() {
    this.headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      'Accept': 'application/json'
    };
  }

  /**
   * Calculate authentic ELIVATE score using correct point allocations
   */
  async calculateAuthenticELIVATE() {
    console.log('🚀 Calculating Authentic ELIVATE with correct point structure...');
    
    // Get existing FRED data
    const fredData = await this.getExistingFREDData();
    
    // Get Yahoo Finance market data  
    const yahooData = await this.getYahooFinanceData();
    
    // Calculate each component using correct point allocations
    const scores = {
      externalInfluence: this.calculateExternalInfluence(fredData), // 0-20 points
      localStory: this.calculateLocalStory(fredData), // 0-20 points
      inflationRates: this.calculateInflationRates(fredData), // 0-20 points
      valuationEarnings: this.calculateValuationEarnings(yahooData), // 0-20 points
      allocationCapital: this.calculateAllocationCapital(yahooData), // 0-10 points
      trendsSentiments: this.calculateTrendsSentiments(yahooData) // 0-10 points
    };
    
    // Calculate total score (max 100 points)
    const totalScore = Object.values(scores).reduce((sum, score) => sum + score, 0);
    const interpretation = this.interpretScore(totalScore);
    
    // Store corrected score
    await this.storeCorrectedScore(totalScore, scores);
    
    console.log('\n📊 AUTHENTIC ELIVATE RESULTS (Correct Point Structure):');
    console.log('=======================================================');
    console.log(`Total Score: ${totalScore.toFixed(1)}/100 points (${interpretation})`);
    
    console.log('\n🔧 COMPONENT BREAKDOWN (Authentic Points):');
    console.log(`External Influence: ${scores.externalInfluence.toFixed(1)}/20 points`);
    console.log(`Local Story: ${scores.localStory.toFixed(1)}/20 points`);
    console.log(`Inflation & Rates: ${scores.inflationRates.toFixed(1)}/20 points`);
    console.log(`Valuation & Earnings: ${scores.valuationEarnings.toFixed(1)}/20 points`);
    console.log(`Allocation of Capital: ${scores.allocationCapital.toFixed(1)}/10 points`);
    console.log(`Trends & Sentiments: ${scores.trendsSentiments.toFixed(1)}/10 points`);
    
    return {
      totalScore,
      interpretation,
      scores,
      maxPossible: 100,
      framework: 'Authentic ELIVATE Point-Based'
    };
  }

  /**
   * External Influence: 0-20 points
   */
  calculateExternalInfluence(fredData) {
    const usGDP = fredData['US_GDP_GROWTH'] || 2.5;
    const usFedRate = fredData['US_FED_RATE'] || 4.33;
    const usInflation = fredData['US_CPI_INFLATION'] || 320;
    
    let score = 0;
    
    // US GDP component (0-7 points)
    if (usGDP > 3.5) score += 7;
    else if (usGDP > 2.5) score += 5;
    else if (usGDP > 1.5) score += 3;
    else score += 1;
    
    // Fed Rate component (0-7 points) - lower rates = higher score
    if (usFedRate < 2.0) score += 7;
    else if (usFedRate < 3.5) score += 5;
    else if (usFedRate < 5.0) score += 3;
    else score += 1;
    
    // US Inflation component (0-6 points) - moderate inflation preferred
    const normalizedInflation = (usInflation - 300) / 10; // Convert to percentage-like
    if (normalizedInflation < 2.5 && normalizedInflation > 1.5) score += 6;
    else if (normalizedInflation < 3.5 && normalizedInflation > 1.0) score += 4;
    else if (normalizedInflation < 5.0) score += 2;
    else score += 0;
    
    return Math.min(score, 20);
  }

  /**
   * Local Story: 0-20 points
   */
  calculateLocalStory(fredData) {
    const indiaGDP = fredData['INDIA_GDP_GROWTH'] || 5.99;
    const indiaInflation = fredData['INDIA_CPI_INFLATION'] || 157.55;
    const indiaYield = fredData['INDIA_10Y_YIELD'] || 6.68;
    
    let score = 0;
    
    // India GDP component (0-8 points)
    if (indiaGDP > 7.0) score += 8;
    else if (indiaGDP > 6.0) score += 6;
    else if (indiaGDP > 5.0) score += 4;
    else score += 2;
    
    // India Inflation component (0-6 points) - lower is better
    const inflationRate = (indiaInflation - 140) / 5; // Approximate percentage
    if (inflationRate < 3.0) score += 6;
    else if (inflationRate < 4.5) score += 4;
    else if (inflationRate < 6.0) score += 2;
    else score += 0;
    
    // India 10Y Yield component (0-6 points) - moderate levels preferred
    if (indiaYield < 6.5) score += 6;
    else if (indiaYield < 7.5) score += 4;
    else if (indiaYield < 8.5) score += 2;
    else score += 0;
    
    return Math.min(score, 20);
  }

  /**
   * Inflation & Rates: 0-20 points
   */
  calculateInflationRates(fredData) {
    const indiaInflation = fredData['INDIA_CPI_INFLATION'] || 157.55;
    const usInflation = fredData['US_CPI_INFLATION'] || 320.32;
    const usdInrRate = fredData['USD_INR_RATE'] || 84.5;
    const repoRate = fredData['INDIA_REPO_RATE'] || 5.15;
    
    let score = 0;
    
    // India CPI component (0-6 points)
    const indiaCPI = (indiaInflation - 140) / 5;
    if (indiaCPI < 3.5) score += 6;
    else if (indiaCPI < 5.0) score += 4;
    else if (indiaCPI < 6.5) score += 2;
    else score += 0;
    
    // US Inflation component (0-5 points)
    const usCPI = (usInflation - 300) / 10;
    if (usCPI < 2.5) score += 5;
    else if (usCPI < 4.0) score += 3;
    else if (usCPI < 6.0) score += 1;
    else score += 0;
    
    // USD/INR stability (0-5 points) - stability preferred
    if (usdInrRate < 83) score += 5;
    else if (usdInrRate < 85) score += 3;
    else if (usdInrRate < 87) score += 1;
    else score += 0;
    
    // Repo Rate component (0-4 points) - moderate levels
    if (repoRate < 5.5) score += 4;
    else if (repoRate < 6.5) score += 2;
    else score += 0;
    
    return Math.min(score, 20);
  }

  /**
   * Valuation & Earnings: 0-20 points
   */
  calculateValuationEarnings(yahooData) {
    const niftyData = yahooData['NIFTY_50'] || {};
    const vixData = yahooData['INDIA_VIX'] || {};
    
    let score = 0;
    
    // Market Level component (0-8 points) - relative to historical levels
    const niftyLevel = niftyData.price || 25000;
    if (niftyData.fiftyTwoWeekHigh) {
      const fromHigh = (niftyLevel / niftyData.fiftyTwoWeekHigh) * 100;
      if (fromHigh > 95) score += 3; // Near highs = expensive
      else if (fromHigh > 85) score += 5;
      else if (fromHigh > 75) score += 8; // Sweet spot
      else if (fromHigh > 65) score += 6;
      else score += 4; // Very cheap but concerning
    } else {
      score += 5; // Neutral when no historical data
    }
    
    // VIX component (0-6 points) - low volatility preferred
    const vixLevel = vixData.price || 15;
    if (vixLevel < 12) score += 6;
    else if (vixLevel < 16) score += 4;
    else if (vixLevel < 20) score += 2;
    else score += 0;
    
    // Sector Performance component (0-6 points)
    const sectorData = [yahooData['NIFTY_IT'], yahooData['NIFTY_BANK']].filter(d => d);
    if (sectorData.length > 0) {
      const avgChange = sectorData.reduce((sum, d) => sum + (d.changePercent || 0), 0) / sectorData.length;
      if (avgChange > 1.0) score += 6;
      else if (avgChange > 0.0) score += 4;
      else if (avgChange > -1.0) score += 2;
      else score += 0;
    } else {
      score += 3; // Neutral when no data
    }
    
    return Math.min(score, 20);
  }

  /**
   * Allocation of Capital: 0-10 points
   */
  calculateAllocationCapital(yahooData) {
    let score = 0;
    
    // Volume Trends component (0-5 points)
    const marketData = Object.values(yahooData).filter(d => d && d.volume);
    if (marketData.length > 0) {
      const avgVolume = marketData.reduce((sum, d) => sum + d.volume, 0) / marketData.length;
      if (avgVolume > 100000000) score += 5; // High volume = good participation
      else if (avgVolume > 50000000) score += 3;
      else if (avgVolume > 25000000) score += 1;
      else score += 0;
    } else {
      score += 2; // Neutral when no volume data
    }
    
    // Market Breadth component (0-5 points) - based on sector performance spread
    const sectorPerformances = [
      yahooData['NIFTY_IT']?.changePercent || 0,
      yahooData['NIFTY_BANK']?.changePercent || 0,
      yahooData['NIFTY_50']?.changePercent || 0
    ];
    
    const performanceSpread = Math.max(...sectorPerformances) - Math.min(...sectorPerformances);
    if (performanceSpread < 1.0) score += 5; // Narrow spread = broad participation
    else if (performanceSpread < 2.0) score += 3;
    else if (performanceSpread < 3.0) score += 1;
    else score += 0;
    
    return Math.min(score, 10);
  }

  /**
   * Trends & Sentiments: 0-10 points
   */
  calculateTrendsSentiments(yahooData) {
    let score = 0;
    
    // Market Momentum component (0-6 points)
    const marketChanges = Object.values(yahooData)
      .filter(d => d && typeof d.changePercent === 'number')
      .map(d => d.changePercent);
      
    if (marketChanges.length > 0) {
      const avgChange = marketChanges.reduce((sum, change) => sum + change, 0) / marketChanges.length;
      if (avgChange > 1.5) score += 6;
      else if (avgChange > 0.5) score += 4;
      else if (avgChange > -0.5) score += 2;
      else score += 0;
    }
    
    // VIX Sentiment component (0-4 points)
    const vixLevel = yahooData['INDIA_VIX']?.price || 15;
    if (vixLevel < 12) score += 4; // Low fear
    else if (vixLevel < 16) score += 3;
    else if (vixLevel < 20) score += 1;
    else score += 0; // High fear
    
    return Math.min(score, 10);
  }

  /**
   * Get existing FRED economic data
   */
  async getExistingFREDData() {
    const result = await executeRawQuery(`
      SELECT index_name, close_value
      FROM market_indices 
      WHERE index_name IN ('US_GDP_GROWTH', 'US_FED_RATE', 'US_CPI_INFLATION', 
                           'INDIA_GDP_GROWTH', 'INDIA_CPI_INFLATION', 'INDIA_10Y_YIELD', 
                           'INDIA_REPO_RATE', 'USD_INR_RATE')
      ORDER BY index_date DESC
    `);

    const indicators = {};
    result.rows.forEach(row => {
      indicators[row.index_name] = parseFloat(row.close_value);
    });

    return indicators;
  }

  /**
   * Get Yahoo Finance market data
   */
  async getYahooFinanceData() {
    const symbols = {
      'NIFTY_50': '^NSEI',
      'NIFTY_IT': '^CNXIT',
      'NIFTY_BANK': '^NSEBANK',
      'INDIA_VIX': '^INDIAVIX'
    };

    const marketData = {};
    
    for (const [name, symbol] of Object.entries(symbols)) {
      try {
        const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}`;
        const response = await axios.get(url, { 
          headers: this.headers,
          timeout: 10000 
        });

        if (response.status === 200 && response.data?.chart?.result?.[0]) {
          const result = response.data.chart.result[0];
          const meta = result.meta;
          
          marketData[name] = {
            price: meta.regularMarketPrice || meta.previousClose,
            changePercent: ((meta.regularMarketPrice || meta.previousClose) - meta.previousClose) / meta.previousClose * 100,
            volume: result.indicators?.quote?.[0]?.volume?.slice(-1)[0] || 0,
            fiftyTwoWeekHigh: meta.fiftyTwoWeekHigh,
            fiftyTwoWeekLow: meta.fiftyTwoWeekLow
          };
        }
      } catch (error) {
        console.log(`Failed to fetch ${name}: ${error.message}`);
      }
      
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    return marketData;
  }

  /**
   * Store corrected ELIVATE score
   */
  async storeCorrectedScore(score, components) {
    await executeRawQuery(`
      INSERT INTO market_indices (index_name, close_value, index_date)
      VALUES ($1, $2, CURRENT_DATE)
      ON CONFLICT (index_name, index_date) 
      DO UPDATE SET close_value = EXCLUDED.close_value
    `, [
      'ELIVATE_AUTHENTIC_CORRECTED',
      score
    ]);
  }

  /**
   * Interpret score based on 100-point scale
   */
  interpretScore(score) {
    if (score >= 75) return 'BULLISH';
    if (score >= 50) return 'NEUTRAL';
    return 'BEARISH';
  }
}

// Execute corrected ELIVATE calculation
async function runAuthenticELIVATE() {
  try {
    const calculator = new AuthenticELIVATECalculator();
    await calculator.calculateAuthenticELIVATE();
    console.log('\n✅ Authentic ELIVATE calculation completed successfully');
  } catch (error) {
    console.error('❌ Authentic ELIVATE calculation failed:', error.message);
  }
}

runAuthenticELIVATE();