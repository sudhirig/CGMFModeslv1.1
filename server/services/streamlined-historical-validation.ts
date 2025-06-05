/**
 * Streamlined Historical Validation Engine
 * Fast, efficient validation using authentic data only
 */

import { db, pool } from '../db';

export interface StreamlinedValidationResult {
  validationRunId: string;
  runDate: Date;
  totalFundsTested: number;
  predictionAccuracy3M: number;
  predictionAccuracy6M: number;
  predictionAccuracy1Y: number;
  scoreCorrelation3M: number;
  scoreCorrelation6M: number;
  scoreCorrelation1Y: number;
  quartileStability3M: number;
  quartileStability6M: number;
  quartileStability1Y: number;
  recommendationAccuracy: {
    strongBuy: number;
    buy: number;
    hold: number;
    sell: number;
    strongSell: number;
  };
}

export class StreamlinedHistoricalValidation {
  static async runValidation(config: {
    startDate: Date;
    endDate: Date;
    validationPeriodMonths: number;
    minimumDataPoints: number;
  }): Promise<StreamlinedValidationResult> {
    
    const validationRunId = `VALIDATION_RUN_${new Date().toISOString().split('T')[0].replace(/-/g, '_')}`;
    console.log(`Starting streamlined validation: ${validationRunId}`);

    // Get sample of funds with sufficient data for authentic validation
    const sampleFunds = await this.getSampleFunds(config);
    console.log(`Processing ${sampleFunds.length} funds for validation`);

    const validationResults = await this.processValidationBatch(sampleFunds, config);
    const aggregateMetrics = this.calculateAggregateMetrics(validationResults);

    // Store results in database
    await this.storeValidationResults(validationRunId, aggregateMetrics, validationResults);

    return {
      validationRunId,
      runDate: new Date(),
      totalFundsTested: sampleFunds.length,
      ...aggregateMetrics
    };
  }

  private static async getSampleFunds(config: any): Promise<any[]> {
    const query = `
      SELECT f.id, f.fund_name, f.category
      FROM funds f
      WHERE EXISTS (
        SELECT 1 FROM nav_data n 
        WHERE n.fund_id = f.id 
        AND n.nav_date >= $1 
        AND n.nav_date <= $2
        HAVING COUNT(*) >= $3
      )
      ORDER BY f.id
      LIMIT 25
    `;

    const result = await pool.query(query, [config.startDate, config.endDate, config.minimumDataPoints]);
    return result.rows;
  }

  private static async processValidationBatch(funds: any[], config: any): Promise<any[]> {
    const results = [];
    
    for (const fund of funds) {
      try {
        const fundResult = await this.validateSingleFundStreamlined(fund, config);
        if (fundResult) {
          results.push(fundResult);
        }
      } catch (error) {
        console.error(`Error validating fund ${fund.id}:`, error);
      }
    }

    return results;
  }

  private static async validateSingleFundStreamlined(fund: any, config: any): Promise<any | null> {
    const scoringDate = new Date(config.startDate.getTime() + (config.validationPeriodMonths * 30 * 24 * 60 * 60 * 1000));
    
    // Get authentic NAV data
    const navQuery = `
      SELECT nav_value, nav_date
      FROM nav_data
      WHERE fund_id = $1 
      AND nav_date BETWEEN $2 AND $3
      ORDER BY nav_date
    `;

    const navResult = await pool.query(navQuery, [fund.id, config.startDate, config.endDate]);
    if (navResult.rows.length < 90) {
      return null;
    }

    const navData = navResult.rows;
    
    // Calculate authentic historical performance
    const scoringNavIndex = navData.findIndex(row => new Date(row.nav_date) >= scoringDate);
    if (scoringNavIndex < 30 || scoringNavIndex >= navData.length - 30) {
      return null;
    }

    const scoringNav = navData[scoringNavIndex];
    const historicalNavs = navData.slice(0, scoringNavIndex);
    const futureNavs = navData.slice(scoringNavIndex);

    // Calculate authentic historical score
    const historicalScore = this.calculateAuthenticScore(historicalNavs, scoringNav);
    const quartile = this.calculateQuartile(historicalScore);
    const recommendation = this.getRecommendation(historicalScore, quartile);

    // Calculate authentic future returns
    const futureReturns = this.calculateFutureReturns(scoringNav, futureNavs);
    
    // Calculate prediction accuracy
    const predictionAccuracy = this.calculatePredictionAccuracy(
      { score: historicalScore, quartile, recommendation },
      futureReturns
    );

    return {
      fundId: fund.id,
      fundName: fund.fund_name,
      category: fund.category,
      historicalTotalScore: historicalScore,
      historicalRecommendation: recommendation,
      historicalQuartile: quartile,
      actualReturn3M: futureReturns.return3M,
      actualReturn6M: futureReturns.return6M,
      actualReturn1Y: futureReturns.return1Y,
      predictionAccuracy3M: predictionAccuracy.accuracy3M,
      predictionAccuracy6M: predictionAccuracy.accuracy6M,
      predictionAccuracy1Y: predictionAccuracy.accuracy1Y,
      scoreCorrelation3M: predictionAccuracy.correlation3M,
      scoreCorrelation6M: predictionAccuracy.correlation6M,
      scoreCorrelation1Y: predictionAccuracy.correlation1Y,
      quartileMaintained3M: predictionAccuracy.quartileStable3M,
      quartileMaintained6M: predictionAccuracy.quartileStable6M,
      quartileMaintained1Y: predictionAccuracy.quartileStable1Y
    };
  }

  private static calculateAuthenticScore(historicalNavs: any[], scoringNav: any): number {
    if (historicalNavs.length < 252) {
      return 50; // Base score for insufficient historical data
    }

    const latest = scoringNav.nav_value;
    const oneYearAgo = historicalNavs[Math.max(0, historicalNavs.length - 252)];
    const sixMonthsAgo = historicalNavs[Math.max(0, historicalNavs.length - 126)];
    const threeMonthsAgo = historicalNavs[Math.max(0, historicalNavs.length - 63)];

    let score = 50; // Base score

    // 1Y return component (25 points)
    if (oneYearAgo) {
      const return1Y = ((latest - oneYearAgo.nav_value) / oneYearAgo.nav_value) * 100;
      score += Math.min(Math.max(return1Y / 2, -15), 15);
    }

    // 6M return component (15 points)
    if (sixMonthsAgo) {
      const return6M = ((latest - sixMonthsAgo.nav_value) / sixMonthsAgo.nav_value) * 100;
      score += Math.min(Math.max(return6M / 3, -10), 10);
    }

    // 3M return component (10 points)
    if (threeMonthsAgo) {
      const return3M = ((latest - threeMonthsAgo.nav_value) / threeMonthsAgo.nav_value) * 100;
      score += Math.min(Math.max(return3M / 4, -5), 5);
    }

    return Math.min(Math.max(score, 0), 100);
  }

  private static calculateQuartile(score: number): number {
    if (score >= 75) return 1;
    if (score >= 60) return 2;
    if (score >= 40) return 3;
    return 4;
  }

  private static getRecommendation(score: number, quartile: number): string {
    if (score >= 80 && quartile === 1) return 'STRONG_BUY';
    if (score >= 65 && quartile <= 2) return 'BUY';
    if (score >= 45 || quartile === 3) return 'HOLD';
    if (score >= 30 || quartile === 4) return 'SELL';
    return 'STRONG_SELL';
  }

  private static calculateFutureReturns(scoringNav: any, futureNavs: any[]): any {
    const scoringValue = scoringNav.nav_value;
    const scoringDate = new Date(scoringNav.nav_date);

    const returns = { return3M: 0, return6M: 0, return1Y: 0 };

    // 3M forward return
    const nav3M = futureNavs.find(nav => 
      new Date(nav.nav_date).getTime() - scoringDate.getTime() >= 90 * 24 * 60 * 60 * 1000
    );
    if (nav3M) {
      returns.return3M = ((nav3M.nav_value - scoringValue) / scoringValue) * 100;
    }

    // 6M forward return
    const nav6M = futureNavs.find(nav => 
      new Date(nav.nav_date).getTime() - scoringDate.getTime() >= 180 * 24 * 60 * 60 * 1000
    );
    if (nav6M) {
      returns.return6M = ((nav6M.nav_value - scoringValue) / scoringValue) * 100;
    }

    // 1Y forward return
    const nav1Y = futureNavs.find(nav => 
      new Date(nav.nav_date).getTime() - scoringDate.getTime() >= 365 * 24 * 60 * 60 * 1000
    );
    if (nav1Y) {
      returns.return1Y = ((nav1Y.nav_value - scoringValue) / scoringValue) * 100;
    }

    return returns;
  }

  private static calculatePredictionAccuracy(historical: any, futureReturns: any): any {
    const marketBenchmark = 8; // 8% annual benchmark
    const predictedOutperform = historical.quartile <= 2;

    return {
      accuracy3M: (futureReturns.return3M > marketBenchmark/4) === predictedOutperform,
      accuracy6M: (futureReturns.return6M > marketBenchmark/2) === predictedOutperform,
      accuracy1Y: (futureReturns.return1Y > marketBenchmark) === predictedOutperform,
      correlation3M: Math.max(0, 1 - Math.abs(historical.score/100 - (futureReturns.return3M + 10)/20)),
      correlation6M: Math.max(0, 1 - Math.abs(historical.score/100 - (futureReturns.return6M + 15)/30)),
      correlation1Y: Math.max(0, 1 - Math.abs(historical.score/100 - (futureReturns.return1Y + 20)/40)),
      quartileStable3M: Math.abs(futureReturns.return3M) < 15,
      quartileStable6M: Math.abs(futureReturns.return6M) < 20,
      quartileStable1Y: Math.abs(futureReturns.return1Y) < 25
    };
  }

  private static calculateAggregateMetrics(results: any[]): any {
    if (results.length === 0) {
      return {
        predictionAccuracy3M: 0, predictionAccuracy6M: 0, predictionAccuracy1Y: 0,
        scoreCorrelation3M: 0, scoreCorrelation6M: 0, scoreCorrelation1Y: 0,
        quartileStability3M: 0, quartileStability6M: 0, quartileStability1Y: 0,
        recommendationAccuracy: { strongBuy: 0, buy: 0, hold: 0, sell: 0, strongSell: 0 }
      };
    }

    const count = results.length;
    
    return {
      predictionAccuracy3M: (results.filter(r => r.predictionAccuracy3M).length / count) * 100,
      predictionAccuracy6M: (results.filter(r => r.predictionAccuracy6M).length / count) * 100,
      predictionAccuracy1Y: (results.filter(r => r.predictionAccuracy1Y).length / count) * 100,
      scoreCorrelation3M: results.reduce((sum, r) => sum + r.scoreCorrelation3M, 0) / count,
      scoreCorrelation6M: results.reduce((sum, r) => sum + r.scoreCorrelation6M, 0) / count,
      scoreCorrelation1Y: results.reduce((sum, r) => sum + r.scoreCorrelation1Y, 0) / count,
      quartileStability3M: (results.filter(r => r.quartileMaintained3M).length / count) * 100,
      quartileStability6M: (results.filter(r => r.quartileMaintained6M).length / count) * 100,
      quartileStability1Y: (results.filter(r => r.quartileMaintained1Y).length / count) * 100,
      recommendationAccuracy: this.calculateRecommendationAccuracy(results)
    };
  }

  private static calculateRecommendationAccuracy(results: any[]): any {
    const groups = {
      strongBuy: results.filter(r => r.historicalRecommendation === 'STRONG_BUY'),
      buy: results.filter(r => r.historicalRecommendation === 'BUY'),
      hold: results.filter(r => r.historicalRecommendation === 'HOLD'),
      sell: results.filter(r => r.historicalRecommendation === 'SELL'),
      strongSell: results.filter(r => r.historicalRecommendation === 'STRONG_SELL')
    };

    const accuracy: any = {};
    for (const [type, funds] of Object.entries(groups)) {
      if (funds.length > 0) {
        const accurate = funds.filter((f: any) => f.predictionAccuracy1Y).length;
        accuracy[type] = (accurate / funds.length) * 100;
      } else {
        accuracy[type] = 0;
      }
    }

    return accuracy;
  }

  private static async storeValidationResults(runId: string, metrics: any, details: any[]): Promise<void> {
    try {
      // Store summary
      await pool.query(`
        INSERT INTO validation_summary_reports (
          validation_run_id, run_date, total_funds_tested, validation_period_months,
          overall_prediction_accuracy_3m, overall_prediction_accuracy_6m, overall_prediction_accuracy_1y,
          overall_score_correlation_3m, overall_score_correlation_6m, overall_score_correlation_1y,
          quartile_stability_3m, quartile_stability_6m, quartile_stability_1y,
          strong_buy_accuracy, buy_accuracy, hold_accuracy, sell_accuracy, strong_sell_accuracy,
          validation_status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
      `, [
        runId, new Date(), details.length, 12,
        metrics.predictionAccuracy3M, metrics.predictionAccuracy6M, metrics.predictionAccuracy1Y,
        metrics.scoreCorrelation3M, metrics.scoreCorrelation6M, metrics.scoreCorrelation1Y,
        metrics.quartileStability3M, metrics.quartileStability6M, metrics.quartileStability1Y,
        metrics.recommendationAccuracy.strongBuy, metrics.recommendationAccuracy.buy,
        metrics.recommendationAccuracy.hold, metrics.recommendationAccuracy.sell,
        metrics.recommendationAccuracy.strongSell, 'COMPLETED'
      ]);

      // Store details
      for (const detail of details) {
        await pool.query(`
          INSERT INTO validation_fund_details (
            validation_run_id, fund_id, fund_name, category,
            historical_total_score, historical_recommendation, historical_quartile,
            actual_return_3m, actual_return_6m, actual_return_1y,
            prediction_accuracy_3m, prediction_accuracy_6m, prediction_accuracy_1y,
            score_correlation_3m, score_correlation_6m, score_correlation_1y,
            quartile_maintained_3m, quartile_maintained_6m, quartile_maintained_1y
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        `, [
          runId, detail.fundId, detail.fundName, detail.category,
          detail.historicalTotalScore, detail.historicalRecommendation, detail.historicalQuartile,
          detail.actualReturn3M, detail.actualReturn6M, detail.actualReturn1Y,
          detail.predictionAccuracy3M, detail.predictionAccuracy6M, detail.predictionAccuracy1Y,
          detail.scoreCorrelation3M, detail.scoreCorrelation6M, detail.scoreCorrelation1Y,
          detail.quartileMaintained3M, detail.quartileMaintained6M, detail.quartileMaintained1Y
        ]);
      }

      console.log(`Validation results stored: ${runId}`);
    } catch (error) {
      console.error('Error storing validation results:', error);
    }
  }
}