/**
 * AdvisorKhoj Scraper Service
 * TypeScript wrapper for Python scraper integration
 */

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import { db } from '../db';
import { 
  aumAnalytics, 
  portfolioOverlap, 
  managerAnalytics, 
  categoryPerformance 
} from '@shared/schema';
import { desc, gte, eq, and, sql } from 'drizzle-orm';
import { format, subDays } from 'date-fns';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface ScraperResult {
  success: boolean;
  recordsScraped: {
    aum: number;
    overlap: number;
    managers: number;
    categories: number;
    indices: number;
  };
  errors: string[];
}

interface ScraperOptions {
  categories?: string[];
  skipExisting?: boolean;
  dateRange?: {
    start: Date;
    end: Date;
  };
}

export class AdvisorKhojScraperService {
  private pythonPath = process.env.PYTHON_PATH || 'python3';
  private scraperPath = path.join(__dirname, '../scrapers/advisorkhoj/scraper.py');

  /**
   * Run the AdvisorKhoj scraper with options
   */
  async runScraper(options?: ScraperOptions): Promise<ScraperResult> {
    console.log('🚀 Starting AdvisorKhoj scraper...');
    
    return new Promise((resolve, reject) => {
      const args = [];
      
      if (options?.categories && options.categories.length > 0) {
        args.push('--categories', options.categories.join(','));
      }
      
      if (options?.skipExisting) {
        args.push('--skip-existing');
      }
      
      if (options?.dateRange) {
        args.push(
          '--start-date', format(options.dateRange.start, 'yyyy-MM-dd'),
          '--end-date', format(options.dateRange.end, 'yyyy-MM-dd')
        );
      }

      const pythonProcess = spawn(this.pythonPath, [this.scraperPath, ...args]);
      
      let output = '';
      let errorOutput = '';

      pythonProcess.stdout.on('data', (data) => {
        output += data.toString();
      });

      pythonProcess.stderr.on('data', (data) => {
        errorOutput += data.toString();
        console.error('Scraper error:', data.toString());
      });

      pythonProcess.on('close', (code) => {
        if (code === 0) {
          try {
            // Parse the last line of output as JSON
            const lines = output.trim().split('\n');
            const jsonLine = lines[lines.length - 1];
            const result = JSON.parse(jsonLine);
            
            console.log('✅ Scraper completed successfully');
            resolve(result);
          } catch (e) {
            console.error('Failed to parse scraper output:', e);
            resolve({
              success: false,
              recordsScraped: { aum: 0, overlap: 0, managers: 0, categories: 0, indices: 0 },
              errors: ['Failed to parse scraper output']
            });
          }
        } else {
          console.error('Scraper failed with code:', code);
          resolve({
            success: false,
            recordsScraped: { aum: 0, overlap: 0, managers: 0, categories: 0, indices: 0 },
            errors: [errorOutput || 'Scraper process failed']
          });
        }
      });

      pythonProcess.on('error', (err) => {
        console.error('Failed to start scraper:', err);
        reject(err);
      });
    });
  }

  /**
   * Get the latest AUM data
   */
  async getLatestAumData(limit = 100) {
    try {
      const data = await db
        .select({
          id: aumAnalytics.id,
          amcName: aumAnalytics.amcName,
          fundName: aumAnalytics.fundName,
          aumCrores: aumAnalytics.aumCrores,
          totalAumCrores: aumAnalytics.totalAumCrores,
          fundCount: aumAnalytics.fundCount,
          category: aumAnalytics.category,
          dataDate: aumAnalytics.dataDate
        })
        .from(aumAnalytics)
        .orderBy(desc(aumAnalytics.dataDate), desc(aumAnalytics.totalAumCrores))
        .limit(limit);

      return data.map(row => ({
        ...row,
        aumCrores: row.aumCrores ? parseFloat(row.aumCrores) : null,
        totalAumCrores: row.totalAumCrores ? parseFloat(row.totalAumCrores) : null
      }));
    } catch (error) {
      console.error('Error fetching AUM data:', error);
      throw error;
    }
  }

  /**
   * Get portfolio overlaps above a threshold
   */
  async getPortfolioOverlaps(minOverlap = 50, limit = 50) {
    try {
      const data = await db
        .select({
          id: portfolioOverlap.id,
          fund1SchemeCode: portfolioOverlap.fund1SchemeCode,
          fund2SchemeCode: portfolioOverlap.fund2SchemeCode,
          fund1Name: portfolioOverlap.fund1Name,
          fund2Name: portfolioOverlap.fund2Name,
          overlapPercentage: portfolioOverlap.overlapPercentage,
          analysisDate: portfolioOverlap.analysisDate
        })
        .from(portfolioOverlap)
        .where(gte(portfolioOverlap.overlapPercentage, minOverlap.toString()))
        .orderBy(desc(portfolioOverlap.overlapPercentage))
        .limit(limit);

      return data.map(row => ({
        ...row,
        overlapPercentage: row.overlapPercentage ? parseFloat(row.overlapPercentage) : null
      }));
    } catch (error) {
      console.error('Error fetching portfolio overlaps:', error);
      throw error;
    }
  }

  /**
   * Get top fund managers by AUM
   */
  async getTopManagers(limit = 20) {
    try {
      const data = await db
        .select({
          id: managerAnalytics.id,
          managerName: managerAnalytics.managerName,
          managedFundsCount: managerAnalytics.managedFundsCount,
          totalAumManaged: managerAnalytics.totalAumManaged,
          avgPerformance1y: managerAnalytics.avgPerformance1y,
          avgPerformance3y: managerAnalytics.avgPerformance3y,
          analysisDate: managerAnalytics.analysisDate
        })
        .from(managerAnalytics)
        .orderBy(desc(managerAnalytics.totalAumManaged))
        .limit(limit);

      return data.map(row => ({
        ...row,
        totalAumManaged: row.totalAumManaged ? parseFloat(row.totalAumManaged) : null,
        avgPerformance1y: row.avgPerformance1y ? parseFloat(row.avgPerformance1y) : null,
        avgPerformance3y: row.avgPerformance3y ? parseFloat(row.avgPerformance3y) : null
      }));
    } catch (error) {
      console.error('Error fetching manager data:', error);
      throw error;
    }
  }

  /**
   * Get category performance data
   */
  async getCategoryPerformance(limit = 50) {
    try {
      const data = await db
        .select({
          id: categoryPerformance.id,
          categoryName: categoryPerformance.categoryName,
          subcategory: categoryPerformance.subcategory,
          avgReturn1y: categoryPerformance.avgReturn1y,
          avgReturn3y: categoryPerformance.avgReturn3y,
          avgReturn5y: categoryPerformance.avgReturn5y,
          fundCount: categoryPerformance.fundCount,
          analysisDate: categoryPerformance.analysisDate
        })
        .from(categoryPerformance)
        .orderBy(desc(categoryPerformance.avgReturn1y))
        .limit(limit);

      return data.map(row => ({
        ...row,
        avgReturn1y: row.avgReturn1y ? parseFloat(row.avgReturn1y) : null,
        avgReturn3y: row.avgReturn3y ? parseFloat(row.avgReturn3y) : null,
        avgReturn5y: row.avgReturn5y ? parseFloat(row.avgReturn5y) : null
      }));
    } catch (error) {
      console.error('Error fetching category performance:', error);
      throw error;
    }
  }

  /**
   * Get data freshness status
   */
  async getDataFreshnessStatus() {
    try {
      const [aumStatus, overlapStatus, managerStatus, categoryStatus] = await Promise.all([
        db.select({ 
          latest: sql<Date>`MAX(data_date)`,
          count: sql<number>`COUNT(*)`
        }).from(aumAnalytics),
        
        db.select({ 
          latest: sql<Date>`MAX(analysis_date)`,
          count: sql<number>`COUNT(*)`
        }).from(portfolioOverlap),
        
        db.select({ 
          latest: sql<Date>`MAX(analysis_date)`,
          count: sql<number>`COUNT(*)`
        }).from(managerAnalytics),
        
        db.select({ 
          latest: sql<Date>`MAX(analysis_date)`,
          count: sql<number>`COUNT(*)`
        }).from(categoryPerformance)
      ]);

      const today = new Date();
      const isStale = (date: Date | null) => {
        if (!date) return true;
        const daysDiff = Math.floor((today.getTime() - new Date(date).getTime()) / (1000 * 60 * 60 * 24));
        return daysDiff > 7; // Consider data stale if older than 7 days
      };

      return {
        aum: {
          lastUpdate: aumStatus[0]?.latest,
          recordCount: Number(aumStatus[0]?.count) || 0,
          isStale: isStale(aumStatus[0]?.latest)
        },
        portfolioOverlap: {
          lastUpdate: overlapStatus[0]?.latest,
          recordCount: Number(overlapStatus[0]?.count) || 0,
          isStale: isStale(overlapStatus[0]?.latest)
        },
        managers: {
          lastUpdate: managerStatus[0]?.latest,
          recordCount: Number(managerStatus[0]?.count) || 0,
          isStale: isStale(managerStatus[0]?.latest)
        },
        categories: {
          lastUpdate: categoryStatus[0]?.latest,
          recordCount: Number(categoryStatus[0]?.count) || 0,
          isStale: isStale(categoryStatus[0]?.latest)
        }
      };
    } catch (error) {
      console.error('Error checking data freshness:', error);
      throw error;
    }
  }

  /**
   * Get AUM by AMC
   */
  async getAumByAmc() {
    try {
      const data = await db
        .select({
          amcName: aumAnalytics.amcName,
          totalAum: sql<string>`SUM(total_aum_crores)`,
          fundCount: sql<number>`SUM(fund_count)`,
          latestDate: sql<Date>`MAX(data_date)`
        })
        .from(aumAnalytics)
        .where(sql`amc_name IS NOT NULL`)
        .groupBy(aumAnalytics.amcName)
        .orderBy(desc(sql`SUM(total_aum_crores)`));

      return data.map(row => ({
        amcName: row.amcName,
        totalAum: row.totalAum ? parseFloat(row.totalAum) : 0,
        fundCount: row.fundCount || 0,
        latestDate: row.latestDate
      }));
    } catch (error) {
      console.error('Error fetching AUM by AMC:', error);
      throw error;
    }
  }

  /**
   * Search for portfolio overlaps by fund name
   */
  async searchPortfolioOverlaps(fundName: string) {
    try {
      const searchPattern = `%${fundName}%`;
      
      const data = await db
        .select()
        .from(portfolioOverlap)
        .where(
          sql`fund1_name ILIKE ${searchPattern} OR fund2_name ILIKE ${searchPattern}`
        )
        .orderBy(desc(portfolioOverlap.overlapPercentage))
        .limit(20);

      return data.map(row => ({
        ...row,
        overlapPercentage: row.overlapPercentage ? parseFloat(row.overlapPercentage) : null
      }));
    } catch (error) {
      console.error('Error searching portfolio overlaps:', error);
      throw error;
    }
  }

  /**
   * Get category performance trends
   */
  async getCategoryTrends(categoryName: string, days = 30) {
    try {
      const startDate = subDays(new Date(), days);
      
      const data = await db
        .select()
        .from(categoryPerformance)
        .where(
          and(
            eq(categoryPerformance.categoryName, categoryName),
            gte(categoryPerformance.analysisDate, format(startDate, 'yyyy-MM-dd'))
          )
        )
        .orderBy(categoryPerformance.analysisDate);

      return data.map(row => ({
        ...row,
        avgReturn1y: row.avgReturn1y ? parseFloat(row.avgReturn1y) : null,
        avgReturn3y: row.avgReturn3y ? parseFloat(row.avgReturn3y) : null,
        avgReturn5y: row.avgReturn5y ? parseFloat(row.avgReturn5y) : null
      }));
    } catch (error) {
      console.error('Error fetching category trends:', error);
      throw error;
    }
  }
}

// Export singleton instance
export const advisorKhojService = new AdvisorKhojScraperService();