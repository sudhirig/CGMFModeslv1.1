/**
 * Recalibrate Historical Returns Scoring Methodology
 * Uses improved percentile distribution to better utilize the full 0-40 point range
 */

import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL);
const db = drizzle(sql);

class RecalibratedHistoricalScoring {
  /**
   * Improved scoring methodology that uses broader percentile bands
   * to achieve better distribution across the 0-40 point range
   */
  static calculateImprovedReturnScore(returnValue, maxPoints, fundReturns) {
    if (!returnValue || !fundReturns || fundReturns.length === 0) return 0;
    
    // Sort all returns to calculate percentiles
    const sortedReturns = fundReturns.sort((a, b) => b - a);
    const totalFunds = sortedReturns.length;
    
    // Find position of current fund
    const position = sortedReturns.findIndex(r => r <= returnValue);
    const percentile = (position / totalFunds) * 100;
    
    // Improved scoring bands that better utilize the full range
    if (percentile <= 5) return maxPoints; // Top 5% get maximum points
    if (percentile <= 10) return Math.round(maxPoints * 0.9); // Top 10% get 90%
    if (percentile <= 20) return Math.round(maxPoints * 0.8); // Top 20% get 80%
    if (percentile <= 30) return Math.round(maxPoints * 0.7); // Top 30% get 70%
    if (percentile <= 40) return Math.round(maxPoints * 0.6); // Top 40% get 60%
    if (percentile <= 50) return Math.round(maxPoints * 0.5); // Top 50% get 50%
    if (percentile <= 60) return Math.round(maxPoints * 0.4); // Top 60% get 40%
    if (percentile <= 70) return Math.round(maxPoints * 0.3); // Top 70% get 30%
    if (percentile <= 80) return Math.round(maxPoints * 0.2); // Top 80% get 20%
    if (percentile <= 90) return Math.round(maxPoints * 0.1); // Top 90% get 10%
    
    return 0; // Bottom 10% get 0 points
  }

  /**
   * Recalculate all historical returns scores with improved methodology
   */
  static async recalibrateAllHistoricalScores() {
    console.log('Starting recalibration of historical returns scoring...');
    
    // Get all fund returns data for percentile calculations
    const fundReturnsData = await sql`
      SELECT 
        fund_id,
        returns_3m,
        returns_6m,
        returns_1y,
        returns_3y,
        returns_5y
      FROM fund_performance_metrics
      WHERE returns_1y IS NOT NULL
    `;
    
    // Extract arrays for percentile calculations
    const returns3m = fundReturnsData.filter(f => f.returns_3m).map(f => f.returns_3m);
    const returns6m = fundReturnsData.filter(f => f.returns_6m).map(f => f.returns_6m);
    const returns1y = fundReturnsData.filter(f => f.returns_1y).map(f => f.returns_1y);
    const returns3y = fundReturnsData.filter(f => f.returns_3y).map(f => f.returns_3y);
    const returns5y = fundReturnsData.filter(f => f.returns_5y).map(f => f.returns_5y);
    
    console.log(`Recalibrating scores for ${fundReturnsData.length} funds...`);
    
    let processed = 0;
    const batchSize = 500;
    
    for (let i = 0; i < fundReturnsData.length; i += batchSize) {
      const batch = fundReturnsData.slice(i, i + batchSize);
      
      for (const fund of batch) {
        // Calculate improved scores for each time period
        const improved3mScore = this.calculateImprovedReturnScore(fund.returns_3m, 5, returns3m);
        const improved6mScore = this.calculateImprovedReturnScore(fund.returns_6m, 10, returns6m);
        const improved1yScore = this.calculateImprovedReturnScore(fund.returns_1y, 10, returns1y);
        const improved3yScore = this.calculateImprovedReturnScore(fund.returns_3y, 8, returns3y);
        const improved5yScore = this.calculateImprovedReturnScore(fund.returns_5y, 7, returns5y);
        
        // Calculate total historical returns score
        const totalHistoricalScore = improved3mScore + improved6mScore + improved1yScore + improved3yScore + improved5yScore;
        
        // Update the corrected scores table
        await sql`
          UPDATE fund_scores_corrected 
          SET 
            return_3m_score = ${improved3mScore},
            return_6m_score = ${improved6mScore},
            return_1y_score = ${improved1yScore},
            return_3y_score = ${improved3yScore},
            return_5y_score = ${improved5yScore},
            historical_returns_total = ${totalHistoricalScore}
          WHERE fund_id = ${fund.fund_id} 
            AND score_date = CURRENT_DATE
        `;
        
        processed++;
      }
      
      console.log(`Processed ${Math.min(processed, fundReturnsData.length)} / ${fundReturnsData.length} funds...`);
    }
    
    console.log('Historical returns scoring recalibration completed!');
    return processed;
  }

  /**
   * Verify the improved scoring distribution
   */
  static async verifyImprovedScoring() {
    console.log('\nVerifying improved historical returns scoring...');
    
    const results = await sql`
      SELECT 
        COUNT(*) as total_funds,
        ROUND(AVG(return_3m_score), 2) as avg_3m_score,
        ROUND(AVG(return_6m_score), 2) as avg_6m_score,
        ROUND(AVG(return_1y_score), 2) as avg_1y_score,
        ROUND(AVG(return_3y_score), 2) as avg_3y_score,
        ROUND(AVG(return_5y_score), 2) as avg_5y_score,
        ROUND(AVG(historical_returns_total), 2) as avg_historical_total,
        ROUND(MIN(historical_returns_total), 2) as min_total,
        ROUND(MAX(historical_returns_total), 2) as max_total
      FROM fund_scores_corrected
      WHERE score_date = CURRENT_DATE
    `;
    
    console.log('Improved Historical Returns Scoring Results:');
    console.log(`Total funds: ${results[0].total_funds}`);
    console.log(`Average 3M score: ${results[0].avg_3m_score}/5 (${(results[0].avg_3m_score/5*100).toFixed(1)}%)`);
    console.log(`Average 6M score: ${results[0].avg_6m_score}/10 (${(results[0].avg_6m_score/10*100).toFixed(1)}%)`);
    console.log(`Average 1Y score: ${results[0].avg_1y_score}/10 (${(results[0].avg_1y_score/10*100).toFixed(1)}%)`);
    console.log(`Average 3Y score: ${results[0].avg_3y_score}/8 (${(results[0].avg_3y_score/8*100).toFixed(1)}%)`);
    console.log(`Average 5Y score: ${results[0].avg_5y_score}/7 (${(results[0].avg_5y_score/7*100).toFixed(1)}%)`);
    console.log(`Average historical total: ${results[0].avg_historical_total}/40 (${(results[0].avg_historical_total/40*100).toFixed(1)}%)`);
    console.log(`Range: ${results[0].min_total} - ${results[0].max_total}`);
    
    return results[0];
  }
}

async function runRecalibration() {
  try {
    const processed = await RecalibratedHistoricalScoring.recalibrateAllHistoricalScores();
    const verification = await RecalibratedHistoricalScoring.verifyImprovedScoring();
    
    console.log(`\nRecalibration complete! Processed ${processed} funds.`);
    console.log(`Historical returns average improved to ${verification.avg_historical_total}/40 points`);
    
  } catch (error) {
    console.error('Recalibration failed:', error);
  } finally {
    await sql.end();
  }
}

runRecalibration();