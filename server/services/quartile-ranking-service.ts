import { executeRawQuery } from '../db';
import { calculateFundPerformance, calculateCompositeScore } from './fund-performance-engine';

interface QuartileResult {
  fundId: number;
  fundName: string;
  category: string;
  quartile: number;
  quartileLabel: string;
  rank: number;
  totalFunds: number;
  percentile: number;
  compositeScore: number;
}

/**
 * Calculate and store quartile rankings for all funds with sufficient historical data
 */
export async function calculateQuartileRankings(): Promise<{
  success: boolean;
  message: string;
  processedFunds: number;
  quartileDistribution: Record<string, Record<number, number>>;
}> {
  try {
    console.log('=== Starting Quartile Ranking Calculation ===');

    // Step 1: Get funds with sufficient NAV data for performance calculation
    const fundsResult = await executeRawQuery(`
      SELECT f.id, f.fund_name, f.category, COUNT(n.nav_date) as nav_count
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id
      WHERE f.category IN ('Equity', 'Debt', 'Hybrid', 'Index Fund', 'Fund of Funds', 'Gold ETF', 'Silver ETF', 'Target Maturity Fund', 'Sectoral Fund')
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(n.nav_date) >= 252  -- At least 1 year of data
      ORDER BY f.category, f.id
    `);

    const eligibleFunds = fundsResult.rows;
    console.log(`Found ${eligibleFunds.length} funds with sufficient historical data`);

    if (eligibleFunds.length === 0) {
      return {
        success: false,
        message: 'No funds found with sufficient historical data for quartile calculation',
        processedFunds: 0,
        quartileDistribution: {}
      };
    }

    // Step 2: Calculate performance metrics for each fund
    const performanceMetrics = [];
    let processedCount = 0;

    for (const fund of eligibleFunds) {
      try {
        const metrics = await calculateFundPerformance(fund.id);
        if (metrics && metrics.dataQualityScore > 0.5) { // Only include funds with decent data quality
          const compositeScore = calculateCompositeScore(metrics);
          performanceMetrics.push({
            ...metrics,
            fundName: fund.fund_name,
            category: fund.category,
            compositeScore
          });
          processedCount++;
          
          if (processedCount % 10 === 0) {
            console.log(`Processed ${processedCount}/${eligibleFunds.length} funds`);
          }
        }
      } catch (error: any) {
        console.error(`Error calculating performance for fund ${fund.id}:`, error.message);
      }
    }

    console.log(`Successfully calculated performance for ${performanceMetrics.length} funds`);

    // Step 3: Group by category and calculate quartile rankings
    const categories = ['Equity', 'Debt', 'Hybrid', 'Index Fund', 'Fund of Funds', 'Gold ETF', 'Silver ETF', 'Target Maturity Fund', 'Sectoral Fund'];
    const quartileDistribution: Record<string, Record<number, number>> = {};
    const calculationDate = new Date();

    for (const category of categories) {
      const categoryFunds = performanceMetrics.filter(f => f.category === category);
      
      if (categoryFunds.length < 4) {
        console.log(`Skipping ${category} category - insufficient funds (${categoryFunds.length})`);
        continue;
      }

      // Sort by composite score (descending - higher is better)
      categoryFunds.sort((a, b) => b.compositeScore - a.compositeScore);

      // Assign quartiles
      const totalFunds = categoryFunds.length;
      const quartileSize = Math.floor(totalFunds / 4);
      
      quartileDistribution[category] = { 1: 0, 2: 0, 3: 0, 4: 0 };

      for (let i = 0; i < categoryFunds.length; i++) {
        const fund = categoryFunds[i];
        const rank = i + 1;
        const percentile = (rank / totalFunds) * 100;
        
        // Determine quartile (Q1 = top 25%, Q4 = bottom 25%)
        let quartile: number;
        let quartileLabel: string;
        
        if (i < quartileSize) {
          quartile = 1;
          quartileLabel = 'BUY';
        } else if (i < quartileSize * 2) {
          quartile = 2;
          quartileLabel = 'HOLD';
        } else if (i < quartileSize * 3) {
          quartile = 3;
          quartileLabel = 'REVIEW';
        } else {
          quartile = 4;
          quartileLabel = 'SELL';
        }

        quartileDistribution[category][quartile]++;

        // Store performance metrics
        await executeRawQuery(`
          INSERT INTO fund_performance_metrics (
            fund_id, calculation_date, returns_1y, returns_3y, returns_5y,
            volatility, sharpe_ratio, max_drawdown, consistency_score,
            alpha, beta, information_ratio, total_nav_records,
            data_quality_score, composite_score
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
          ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
            returns_1y = EXCLUDED.returns_1y,
            returns_3y = EXCLUDED.returns_3y,
            returns_5y = EXCLUDED.returns_5y,
            volatility = EXCLUDED.volatility,
            sharpe_ratio = EXCLUDED.sharpe_ratio,
            max_drawdown = EXCLUDED.max_drawdown,
            consistency_score = EXCLUDED.consistency_score,
            alpha = EXCLUDED.alpha,
            beta = EXCLUDED.beta,
            information_ratio = EXCLUDED.information_ratio,
            total_nav_records = EXCLUDED.total_nav_records,
            data_quality_score = EXCLUDED.data_quality_score,
            composite_score = EXCLUDED.composite_score
        `, [
          fund.fundId, calculationDate,
          fund.returns1Y, fund.returns3Y, fund.returns5Y,
          fund.volatility, fund.sharpeRatio, fund.maxDrawdown, fund.consistencyScore,
          fund.alpha, fund.beta, fund.informationRatio, fund.totalNavRecords,
          fund.dataQualityScore, fund.compositeScore
        ]);

        // Store quartile ranking
        await executeRawQuery(`
          INSERT INTO quartile_rankings (
            fund_id, category, calculation_date, quartile, quartile_label,
            rank, total_funds, percentile, composite_score
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
          ON CONFLICT (fund_id, category, calculation_date) DO UPDATE SET
            quartile = EXCLUDED.quartile,
            quartile_label = EXCLUDED.quartile_label,
            rank = EXCLUDED.rank,
            total_funds = EXCLUDED.total_funds,
            percentile = EXCLUDED.percentile,
            composite_score = EXCLUDED.composite_score
        `, [
          fund.fundId, category, calculationDate, quartile, quartileLabel,
          rank, totalFunds, percentile, fund.compositeScore
        ]);
      }

      console.log(`${category} category rankings:`, quartileDistribution[category]);
    }

    return {
      success: true,
      message: `Successfully calculated quartile rankings for ${processedCount} funds across ${Object.keys(quartileDistribution).length} categories`,
      processedFunds: processedCount,
      quartileDistribution
    };

  } catch (error) {
    console.error('Error in quartile ranking calculation:', error);
    return {
      success: false,
      message: `Quartile calculation failed: ${error.message}`,
      processedFunds: 0,
      quartileDistribution: {}
    };
  }
}

/**
 * Get quartile rankings for a specific category
 */
export async function getQuartileRankings(category?: string, quartile?: number): Promise<QuartileResult[]> {
  try {
    let query = `
      SELECT 
        qr.fund_id,
        f.fund_name,
        qr.category,
        qr.quartile,
        qr.quartile_label,
        qr.rank,
        qr.total_funds,
        qr.percentile,
        qr.composite_score
      FROM quartile_rankings qr
      JOIN funds f ON qr.fund_id = f.id
      WHERE qr.calculation_date = (
        SELECT MAX(calculation_date) 
        FROM quartile_rankings 
        WHERE category = qr.category
      )
    `;

    const params: any[] = [];
    let paramCount = 0;

    if (category) {
      paramCount++;
      query += ` AND qr.category = $${paramCount}`;
      params.push(category);
    }

    if (quartile) {
      paramCount++;
      query += ` AND qr.quartile = $${paramCount}`;
      params.push(quartile);
    }

    query += ` ORDER BY qr.category, qr.rank`;

    const result = await executeRawQuery(query, params);
    
    return result.rows.map(row => ({
      fundId: row.fund_id,
      fundName: row.fund_name,
      category: row.category,
      quartile: row.quartile,
      quartileLabel: row.quartile_label,
      rank: row.rank,
      totalFunds: row.total_funds,
      percentile: parseFloat(row.percentile),
      compositeScore: parseFloat(row.composite_score)
    }));

  } catch (error) {
    console.error('Error fetching quartile rankings:', error);
    return [];
  }
}

/**
 * Get quartile distribution summary
 */
export async function getQuartileDistribution(): Promise<Record<string, Record<string, number>>> {
  try {
    const result = await executeRawQuery(`
      SELECT 
        category,
        quartile,
        quartile_label,
        COUNT(*) as fund_count
      FROM quartile_rankings qr
      WHERE calculation_date = (
        SELECT MAX(calculation_date) 
        FROM quartile_rankings 
        WHERE category = qr.category
      )
      GROUP BY category, quartile, quartile_label
      ORDER BY category, quartile
    `);

    const distribution: Record<string, Record<string, number>> = {};
    
    for (const row of result.rows) {
      if (!distribution[row.category]) {
        distribution[row.category] = {};
      }
      distribution[row.category][row.quartile_label] = parseInt(row.fund_count);
    }

    return distribution;

  } catch (error) {
    console.error('Error fetching quartile distribution:', error);
    return {};
  }
}

/**
 * Get fund performance metrics
 */
export async function getFundPerformanceMetrics(fundId: number): Promise<any> {
  try {
    const result = await executeRawQuery(`
      SELECT * FROM fund_performance_metrics
      WHERE fund_id = $1
      ORDER BY calculation_date DESC
      LIMIT 1
    `, [fundId]);

    if (result.rows.length === 0) return null;

    const metrics = result.rows[0];
    return {
      ...metrics,
      returns1Y: parseFloat(metrics.returns_1y),
      returns3Y: parseFloat(metrics.returns_3y),
      returns5Y: parseFloat(metrics.returns_5y),
      volatility: parseFloat(metrics.volatility),
      sharpeRatio: parseFloat(metrics.sharpe_ratio),
      maxDrawdown: parseFloat(metrics.max_drawdown),
      consistencyScore: parseFloat(metrics.consistency_score),
      alpha: parseFloat(metrics.alpha),
      beta: parseFloat(metrics.beta),
      informationRatio: parseFloat(metrics.information_ratio),
      dataQualityScore: parseFloat(metrics.data_quality_score),
      compositeScore: parseFloat(metrics.composite_score)
    };

  } catch (error) {
    console.error('Error fetching fund performance metrics:', error);
    return null;
  }
}