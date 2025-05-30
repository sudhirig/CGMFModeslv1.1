import cron from 'node-cron';
import { executeRawQuery } from '../db.js';

/**
 * Automated Quartile Analysis Updater
 * Continuously monitors for new NAV data and updates quartile rankings automatically
 */
class AutoQuartileUpdater {
  private isRunning = false;
  private lastUpdateTime = new Date();
  private lastFundCount = 0;

  constructor() {
    this.startMonitoring();
  }

  private startMonitoring() {
    // Check for new data every 2 minutes
    cron.schedule('*/2 * * * *', async () => {
      await this.checkAndUpdateQuartiles();
    });

    console.log('🔄 Auto-quartile updater started - monitoring every 2 minutes');
  }

  private async checkAndUpdateQuartiles() {
    try {
      // Check if new funds have sufficient NAV data for scoring
      const fundsWithDataQuery = `
        SELECT COUNT(DISTINCT f.id) as fund_count 
        FROM funds f 
        JOIN nav_data n ON f.id = n.fund_id 
        WHERE f.status = 'ACTIVE'
        GROUP BY f.id 
        HAVING COUNT(n.id) >= 50
      `;

      const result = await executeRawQuery(fundsWithDataQuery);
      const currentFundCount = result.rows.length;

      // If fund count changed, update quartile rankings
      if (currentFundCount !== this.lastFundCount) {
        console.log(`📊 Detected ${currentFundCount - this.lastFundCount} new funds with sufficient data`);
        console.log(`📈 Updating quartile analysis automatically...`);
        
        await this.updateQuartileRankings();
        
        this.lastFundCount = currentFundCount;
        this.lastUpdateTime = new Date();
        
        console.log(`✅ Quartile analysis updated with ${currentFundCount} funds`);
      }
    } catch (error) {
      console.error('Error in auto-quartile updater:', error);
    }
  }

  private async updateQuartileRankings() {
    try {
      // Calculate performance metrics for all funds with sufficient data
      const fundsWithDataQuery = `
        SELECT f.id, f.fund_name, f.category,
               COUNT(n.id) as nav_count,
               MIN(n.nav_date) as earliest_date,
               MAX(n.nav_date) as latest_date
        FROM funds f 
        JOIN nav_data n ON f.id = n.fund_id 
        WHERE f.status = 'ACTIVE'
        GROUP BY f.id, f.fund_name, f.category
        HAVING COUNT(n.id) >= 50
        ORDER BY f.category, f.fund_name
      `;

      const fundsResult = await executeRawQuery(fundsWithDataQuery);
      
      for (const fund of fundsResult.rows) {
        await this.calculateAndStoreFundScore(fund);
      }

      // Calculate quartile rankings
      await this.calculateQuartileRankings();
      
    } catch (error) {
      console.error('Error updating quartile rankings:', error);
    }
  }

  private async calculateAndStoreFundScore(fund: any) {
    try {
      // Get NAV data for performance calculation
      const navQuery = `
        SELECT nav_date, nav_value 
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC 
        LIMIT 1000
      `;

      const navResult = await executeRawQuery(navQuery, [fund.id]);
      const navData = navResult.rows;

      if (navData.length < 50) return;

      // Calculate annual return
      const latestNav = parseFloat(navData[0].nav_value);
      const yearAgoNav = navData.find(n => {
        const date = new Date(n.nav_date);
        const yearAgo = new Date();
        yearAgo.setFullYear(yearAgo.getFullYear() - 1);
        return date <= yearAgo;
      });

      if (!yearAgoNav) return;

      const annualReturn = ((latestNav / parseFloat(yearAgoNav.nav_value)) - 1) * 100;

      // Store or update fund score
      const upsertScoreQuery = `
        INSERT INTO fund_scores (
          fund_id, score_date, return_1y, annual_return, 
          total_score, quartile, recommendation
        ) VALUES ($1, CURRENT_DATE, $2, $3, $4, 1, 'HOLD')
        ON CONFLICT (fund_id, score_date) 
        DO UPDATE SET 
          return_1y = EXCLUDED.return_1y,
          annual_return = EXCLUDED.annual_return,
          total_score = EXCLUDED.total_score,
          updated_at = CURRENT_TIMESTAMP
      `;

      await executeRawQuery(upsertScoreQuery, [
        fund.id,
        annualReturn.toFixed(4),
        annualReturn.toFixed(4),
        annualReturn.toFixed(4)
      ]);

    } catch (error) {
      console.error(`Error calculating score for fund ${fund.id}:`, error);
    }
  }

  private async calculateQuartileRankings() {
    try {
      // Get all scored funds and calculate quartiles
      const scoredFundsQuery = `
        SELECT fs.fund_id, fs.total_score, f.category
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE fs.score_date = CURRENT_DATE
        ORDER BY CAST(fs.total_score AS NUMERIC) DESC
      `;

      const scoredFunds = await executeRawQuery(scoredFundsQuery);
      const funds = scoredFunds.rows;

      if (funds.length === 0) return;

      // Calculate quartile thresholds
      const quartileSize = Math.ceil(funds.length / 4);

      for (let i = 0; i < funds.length; i++) {
        const quartile = Math.min(Math.ceil((i + 1) / quartileSize), 4);
        
        const updateQuartileQuery = `
          UPDATE fund_scores 
          SET quartile = $1 
          WHERE fund_id = $2 AND score_date = CURRENT_DATE
        `;

        await executeRawQuery(updateQuartileQuery, [quartile, funds[i].fund_id]);
      }

      console.log(`📊 Updated quartile rankings for ${funds.length} funds`);
      
    } catch (error) {
      console.error('Error calculating quartile rankings:', error);
    }
  }

  public getStatus() {
    return {
      isRunning: this.isRunning,
      lastUpdateTime: this.lastUpdateTime,
      lastFundCount: this.lastFundCount
    };
  }
}

// Create and export the singleton instance
export const autoQuartileUpdater = new AutoQuartileUpdater();