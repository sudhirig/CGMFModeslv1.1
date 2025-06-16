import { executeRawQuery } from '../db';
import cron from 'node-cron';

interface FundEligibilityCheck {
  fundId: number;
  fundName: string;
  category: string;
  navCount: number;
  isEligible: boolean;
  dataQualityScore: number;
}

interface QuartileRecalculationResult {
  category: string;
  fundsProcessed: number;
  quartileDistribution: {
    BUY: number;
    HOLD: number;
    REVIEW: number;
    SELL: number;
  };
}

export class AutomatedQuartileScheduler {
  private isRunning = false;
  private lastFullRecalculation: Date | null = null;

  constructor() {
    this.setupScheduledTasks();
  }

  private setupScheduledTasks() {
    // ALL SCHEDULED TASKS DISABLED TO PREVENT DATABASE CORRUPTION
    console.log('⚠️ Automated quartile scheduler DISABLED to prevent database corruption');
    console.log('Manual triggers available via API endpoints for controlled execution');
  }

  async runDailyEligibilityCheck(): Promise<FundEligibilityCheck[]> {
    try {
      console.log('=== Daily Fund Eligibility Assessment ===');
      
      // Get all funds with their NAV data counts
      const fundsResult = await executeRawQuery(`
        SELECT 
          f.id,
          f.fund_name,
          f.category,
          COUNT(n.nav_date) as nav_count,
          MIN(n.nav_date) as earliest_nav,
          MAX(n.nav_date) as latest_nav,
          (MAX(n.nav_date) - MIN(n.nav_date)) as days_span
        FROM funds f
        LEFT JOIN nav_data n ON f.id = n.fund_id
        WHERE n.created_at > '2025-05-30 06:45:00'  -- Only authentic imported data
        GROUP BY f.id, f.fund_name, f.category
        ORDER BY f.category, COUNT(n.nav_date) DESC
      `);

      const eligibilityResults: FundEligibilityCheck[] = [];

      for (const fund of fundsResult.rows) {
        const navCount = parseInt(fund.nav_count) || 0;
        const daysSpan = parseFloat(fund.days_span) || 0;
        
        // Eligibility criteria
        const hasMinimumData = navCount >= 252; // 1 year minimum
        const hasRecentData = daysSpan >= 365; // At least 1 year span
        const dataQualityScore = Math.min(1.0, navCount / (5 * 252)); // 5 years = perfect score
        
        const isEligible = hasMinimumData && hasRecentData && dataQualityScore > 0.2;

        eligibilityResults.push({
          fundId: fund.id,
          fundName: fund.fund_name,
          category: fund.category,
          navCount,
          isEligible,
          dataQualityScore
        });

        // Log new eligible funds
        if (isEligible) {
          const existingMetrics = await executeRawQuery(`
            SELECT id FROM fund_performance_metrics 
            WHERE fund_id = $1 AND calculation_date >= CURRENT_DATE - INTERVAL '7 days'
          `, [fund.id]);

          if (existingMetrics.rows.length === 0) {
            console.log(`✓ New eligible fund detected: ${fund.fund_name} (${navCount} NAV records)`);
            await this.calculateFundPerformanceMetrics(fund.id);
          }
        }
      }

      // Summary by category
      const categoryStats = eligibilityResults.reduce((acc, fund) => {
        if (!acc[fund.category]) {
          acc[fund.category] = { total: 0, eligible: 0 };
        }
        acc[fund.category].total++;
        if (fund.isEligible) acc[fund.category].eligible++;
        return acc;
      }, {} as Record<string, { total: number; eligible: number }>);

      console.log('Fund eligibility by category:');
      Object.entries(categoryStats).forEach(([category, stats]) => {
        console.log(`  ${category}: ${stats.eligible}/${stats.total} eligible (${Math.round(stats.eligible/stats.total*100)}%)`);
      });

      return eligibilityResults;
    } catch (error: any) {
      console.error('Error in daily eligibility check:', error.message);
      return [];
    }
  }

  async runWeeklyQuartileRecalculation(): Promise<QuartileRecalculationResult[]> {
    if (this.isRunning) {
      console.log('Quartile recalculation already in progress, skipping...');
      return [];
    }

    this.isRunning = true;
    try {
      console.log('=== Weekly Quartile Recalculation ===');
      
      const calculationDate = new Date();
      const results: QuartileRecalculationResult[] = [];

      // Get all categories with eligible funds
      const categoriesResult = await executeRawQuery(`
        SELECT DISTINCT f.category, COUNT(*) as fund_count
        FROM funds f
        JOIN fund_performance_metrics pm ON f.id = pm.fund_id
        WHERE pm.calculation_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY f.category
        HAVING COUNT(*) >= 1
        ORDER BY COUNT(*) DESC
      `);

      for (const categoryRow of categoriesResult.rows) {
        const category = categoryRow.category;
        console.log(`\nRecalculating quartiles for ${category} category...`);

        // Recalculate performance metrics for all funds in category
        await this.recalculateCategoryPerformance(category, calculationDate);

        // Assign new quartile rankings
        const quartileResult = await this.assignQuartileRankings(category, calculationDate);
        results.push(quartileResult);

        console.log(`${category} updated: ${quartileResult.fundsProcessed} funds processed`);
      }

      this.lastFullRecalculation = calculationDate;
      console.log(`Weekly quartile recalculation completed for ${results.length} categories`);

      return results;
    } finally {
      this.isRunning = false;
    }
  }

  private async calculateFundPerformanceMetrics(fundId: number): Promise<void> {
    try {
      const calculationDate = new Date();

      // Calculate authentic performance metrics from NAV data
      const metricsResult = await executeRawQuery(`
        WITH nav_analysis AS (
          SELECT 
            (SELECT nav_value FROM nav_data WHERE fund_id = $1 ORDER BY nav_date ASC LIMIT 1) as first_nav,
            (SELECT nav_value FROM nav_data WHERE fund_id = $1 ORDER BY nav_date DESC LIMIT 1) as latest_nav,
            (SELECT nav_date FROM nav_data WHERE fund_id = $1 ORDER BY nav_date ASC LIMIT 1) as start_date,
            (SELECT nav_date FROM nav_data WHERE fund_id = $1 ORDER BY nav_date DESC LIMIT 1) as end_date,
            COUNT(*) as total_records
          FROM nav_data WHERE fund_id = $1
        )
        SELECT 
          first_nav,
          latest_nav,
          total_records,
          ROUND((POWER((latest_nav / first_nav), 365.0 / (end_date - start_date)) - 1) * 100, 4) as annualized_return_pct
        FROM nav_analysis
      `, [fundId]);

      if (metricsResult.rows.length > 0) {
        const metrics = metricsResult.rows[0];
        const dataQuality = Math.min(1.0, parseFloat(metrics.total_records) / (5 * 252));
        const compositeScore = parseFloat(metrics.annualized_return_pct) || 0;

        await executeRawQuery(`
          INSERT INTO fund_performance_metrics (
            fund_id, calculation_date, returns_1y, total_nav_records,
            data_quality_score, composite_score
          ) VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
            returns_1y = EXCLUDED.returns_1y,
            composite_score = EXCLUDED.composite_score,
            total_nav_records = EXCLUDED.total_nav_records,
            data_quality_score = EXCLUDED.data_quality_score
        `, [
          fundId, calculationDate, metrics.annualized_return_pct,
          metrics.total_records, dataQuality, compositeScore
        ]);
      }
    } catch (error: any) {
      console.error(`Error calculating metrics for fund ${fundId}:`, error.message);
    }
  }

  private async recalculateCategoryPerformance(category: string, calculationDate: Date): Promise<void> {
    // Get all funds in this category that need recalculation
    const fundsResult = await executeRawQuery(`
      SELECT DISTINCT f.id
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id
      WHERE f.category = $1 
      AND n.created_at > '2025-05-30 06:45:00'
      GROUP BY f.id
      HAVING COUNT(n.nav_date) >= 252
    `, [category]);

    for (const fund of fundsResult.rows) {
      await this.calculateFundPerformanceMetrics(fund.id);
    }
  }

  private async assignQuartileRankings(category: string, calculationDate: Date): Promise<QuartileRecalculationResult> {
    // Get funds in this category sorted by performance
    const categoryFundsResult = await executeRawQuery(`
      SELECT DISTINCT ON (f.id) 
        f.id, f.fund_name, pm.composite_score
      FROM funds f
      JOIN fund_performance_metrics pm ON f.id = pm.fund_id
      WHERE f.category = $1 AND pm.calculation_date::date = $2::date
      ORDER BY f.id, pm.composite_score DESC
    `, [category, calculationDate]);

    const categoryFunds = categoryFundsResult.rows.sort((a, b) => 
      parseFloat(b.composite_score) - parseFloat(a.composite_score)
    );

    const totalFunds = categoryFunds.length;
    const quartileDistribution = { BUY: 0, HOLD: 0, REVIEW: 0, SELL: 0 };

    if (totalFunds === 0) {
      return { category, fundsProcessed: 0, quartileDistribution };
    }

    // Calculate quartile thresholds
    const q1Threshold = Math.max(1, Math.ceil(totalFunds * 0.25));
    const q2Threshold = Math.max(1, Math.ceil(totalFunds * 0.50));
    const q3Threshold = Math.max(1, Math.ceil(totalFunds * 0.75));

    // Clear existing rankings for this category and date
    await executeRawQuery(`
      DELETE FROM quartile_rankings 
      WHERE category = $1 AND calculation_date::date = $2::date
    `, [category, calculationDate]);

    // Assign new quartiles
    for (let i = 0; i < categoryFunds.length; i++) {
      const fund = categoryFunds[i];
      const rank = i + 1;
      const percentile = parseFloat((rank / totalFunds * 100).toFixed(2));

      let quartile: number, quartileLabel: string;
      if (rank <= q1Threshold) {
        quartile = 1;
        quartileLabel = 'BUY';
      } else if (rank <= q2Threshold) {
        quartile = 2;
        quartileLabel = 'HOLD';
      } else if (rank <= q3Threshold) {
        quartile = 3;
        quartileLabel = 'REVIEW';
      } else {
        quartile = 4;
        quartileLabel = 'SELL';
      }

      quartileDistribution[quartileLabel as keyof typeof quartileDistribution]++;

      await executeRawQuery(`
        INSERT INTO quartile_rankings (
          fund_id, category, calculation_date, quartile, quartile_label,
          rank, total_funds, percentile, composite_score
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      `, [
        fund.id, category, calculationDate, quartile, quartileLabel,
        rank, totalFunds, percentile,
        Math.min(9999.99, parseFloat(parseFloat(fund.composite_score).toFixed(2)))
      ]);
    }

    return { category, fundsProcessed: totalFunds, quartileDistribution };
  }

  async trackQuartileMigrations(): Promise<void> {
    try {
      console.log('=== Monthly Quartile Migration Tracking ===');

      // Compare current quartiles with previous month
      const migrationResult = await executeRawQuery(`
        WITH current_rankings AS (
          SELECT fund_id, category, quartile_label, composite_score
          FROM quartile_rankings 
          WHERE calculation_date >= CURRENT_DATE - INTERVAL '7 days'
        ),
        previous_rankings AS (
          SELECT fund_id, category, quartile_label, composite_score
          FROM quartile_rankings 
          WHERE calculation_date >= CURRENT_DATE - INTERVAL '37 days'
          AND calculation_date < CURRENT_DATE - INTERVAL '30 days'
        )
        SELECT 
          f.fund_name,
          f.category,
          prev.quartile_label as previous_quartile,
          curr.quartile_label as current_quartile,
          curr.composite_score - prev.composite_score as performance_change
        FROM current_rankings curr
        JOIN previous_rankings prev ON curr.fund_id = prev.fund_id
        JOIN funds f ON curr.fund_id = f.id
        WHERE curr.quartile_label != prev.quartile_label
        ORDER BY curr.category, f.fund_name
      `);

      console.log(`Found ${migrationResult.rows.length} funds with quartile migrations:`);
      migrationResult.rows.forEach(row => {
        const direction = row.previous_quartile === 'SELL' && row.current_quartile === 'REVIEW' ? '↑' : 
                         row.previous_quartile === 'REVIEW' && row.current_quartile === 'HOLD' ? '↑' :
                         row.previous_quartile === 'HOLD' && row.current_quartile === 'BUY' ? '↑' : '↓';
        console.log(`  ${direction} ${row.fund_name}: ${row.previous_quartile} → ${row.current_quartile} (${row.performance_change > 0 ? '+' : ''}${parseFloat(row.performance_change).toFixed(2)}%)`);
      });

    } catch (error: any) {
      console.error('Error tracking quartile migrations:', error.message);
    }
  }

  // Manual trigger methods for testing
  async triggerDailyCheck(): Promise<void> {
    console.log('Manually triggering daily eligibility check...');
    await this.runDailyEligibilityCheck();
  }

  async triggerWeeklyRecalculation(): Promise<void> {
    console.log('Manually triggering weekly quartile recalculation...');
    await this.runWeeklyQuartileRecalculation();
  }

  async triggerMigrationTracking(): Promise<void> {
    console.log('Manually triggering migration tracking...');
    await this.trackQuartileMigrations();
  }

  getStatus() {
    return {
      isRunning: this.isRunning,
      lastFullRecalculation: this.lastFullRecalculation,
      scheduledTasks: ['Daily 6AM UTC', 'Weekly Sunday 7AM UTC', 'Monthly 1st 8AM UTC']
    };
  }
}

// Export singleton instance
export const quartileScheduler = new AutomatedQuartileScheduler();