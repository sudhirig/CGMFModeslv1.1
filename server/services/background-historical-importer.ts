/**
 * Background Historical NAV Data Importer
 * Runs continuously to import authentic historical data from free APIs
 * Designed to work 24/7 in the background without user intervention
 */

import { db, executeRawQuery } from '../db.js';
import { funds, navData } from '../../shared/schema.js';
import { eq, lt, sql, and, isNull, or } from 'drizzle-orm';
import axios from 'axios';

interface HistoricalNavEntry {
  date: string;
  nav: string;
}

interface ImportProgress {
  totalFundsProcessed: number;
  totalRecordsImported: number;
  currentBatch: number;
  lastProcessedFund: string;
  isRunning: boolean;
}

class BackgroundHistoricalImporter {
  private isRunning = false;
  private currentProgress: ImportProgress = {
    totalFundsProcessed: 0,
    totalRecordsImported: 0,
    currentBatch: 0,
    lastProcessedFund: '',
    isRunning: false
  };
  private processedFundIds = new Set<number>();
  private consecutiveEmptyBatches = 0;
  
  private readonly BATCH_SIZE = 10; // Process 10 funds at a time
  private readonly DELAY_BETWEEN_BATCHES = 5000; // 5 seconds between batches
  private readonly DELAY_BETWEEN_REQUESTS = 500; // 0.5 seconds between API calls
  private readonly MAX_MONTHS_BACK = 120; // Import up to 10 years of data
  private readonly PARALLEL_REQUESTS = 10; // Run 10 parallel requests per fund
  private readonly MAX_EMPTY_BATCHES = 20; // Stop after 20 consecutive empty batches

  async start() {
    if (this.isRunning) {
      console.log('📊 Background historical importer already running');
      return;
    }

    // Reset state to start fresh and access different funds
    this.processedFundIds.clear();
    this.consecutiveEmptyBatches = 0;

    this.isRunning = true;
    this.currentProgress.isRunning = true;
    
    console.log('🚀 Starting background historical NAV data importer...');
    console.log(`  Batch size: ${this.BATCH_SIZE} funds`);
    console.log(`  Delay between batches: ${this.DELAY_BETWEEN_BATCHES / 1000}s`);
    console.log(`  Max import range: ${this.MAX_MONTHS_BACK} months`);

    try {
      await this.runContinuousImport();
    } catch (error) {
      console.error('Background importer error:', error);
      this.isRunning = false;
      this.currentProgress.isRunning = false;
    }
  }

  stop() {
    console.log('⏹️ Stopping background historical importer...');
    this.isRunning = false;
    this.currentProgress.isRunning = false;
    // Force stop by clearing any running timeouts
    if (this.batchTimeout) {
      clearTimeout(this.batchTimeout);
      this.batchTimeout = null;
    }
  }

  private batchTimeout: NodeJS.Timeout | null = null;

  getProgress(): ImportProgress {
    return { ...this.currentProgress };
  }

  private async runContinuousImport() {
    while (this.isRunning) {
      try {
        // Get next batch of funds needing historical data
        let fundsToProcess = await this.getFundsNeedingHistoricalData();
        
        if (fundsToProcess.length === 0) {
          console.log('📈 All eligible funds processed! Checking for funds needing deeper historical data...');
          
          // Try to find funds with some data but needing more historical depth
          const fundsNeedingMoreData = await this.getFundsNeedingDeeperHistory();
          fundsToProcess = fundsNeedingMoreData;
          
          if (fundsNeedingMoreData.length === 0) {
            console.log('✅ Historical import complete - all funds have adequate data coverage');
            this.stop();
            break;
          } else {
            console.log(`🔄 Found ${fundsNeedingMoreData.length} funds needing deeper historical data`);
            // Process these funds before restarting
            continue;
          }
        }

        this.currentProgress.currentBatch++;
        console.log(`\n📦 Processing batch ${this.currentProgress.currentBatch} - ${fundsToProcess.length} funds`);

        let batchRecordsImported = 0;

        // Process funds in parallel batches
        const parallelBatches = [];
        for (let i = 0; i < fundsToProcess.length; i += this.PARALLEL_REQUESTS) {
          const batch = fundsToProcess.slice(i, i + this.PARALLEL_REQUESTS);
          parallelBatches.push(batch);
        }

        for (const batch of parallelBatches) {
          if (!this.isRunning) break;

          // Process this batch in parallel
          const batchPromises = batch.map(async (fund) => {
            if (!this.isRunning) return 0;
            
            this.currentProgress.lastProcessedFund = fund.fundName;
            const importedCount = await this.importHistoricalDataForFund(fund);
            
            if (importedCount > 0) {
              this.currentProgress.totalRecordsImported += importedCount;
              batchRecordsImported += importedCount;
              console.log(`  ✅ ${fund.fundName}: +${importedCount} records`);
            }

            this.currentProgress.totalFundsProcessed++;
            return importedCount;
          });

          await Promise.all(batchPromises);
          
          // Short delay between parallel batches
          await new Promise(resolve => setTimeout(resolve, this.DELAY_BETWEEN_REQUESTS));
        }

        // Log progress
        console.log(`📊 Batch ${this.currentProgress.currentBatch} completed:`);
        console.log(`  Total funds processed: ${this.currentProgress.totalFundsProcessed}`);
        console.log(`  Total records imported: ${this.currentProgress.totalRecordsImported}`);

        // Check if this batch imported any records
        if (batchRecordsImported === 0) {
          this.consecutiveEmptyBatches++;
          console.log(`⚠️ Empty batch ${this.consecutiveEmptyBatches}/${this.MAX_EMPTY_BATCHES}`);
          
          if (this.consecutiveEmptyBatches >= this.MAX_EMPTY_BATCHES) {
            console.log('🛑 Too many consecutive empty batches. Stopping import to prevent endless cycling.');
            this.stop();
            break;
          }
        } else {
          this.consecutiveEmptyBatches = 0; // Reset counter on successful import
        }

        // Delay between batches
        if (this.isRunning) {
          console.log(`⏱️ Waiting ${this.DELAY_BETWEEN_BATCHES / 1000}s before next batch...`);
          await new Promise(resolve => setTimeout(resolve, this.DELAY_BETWEEN_BATCHES));
        }

      } catch (error) {
        console.error('Error in continuous import loop:', error);
        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, 60000)); // Wait 1 minute on error
      }
    }
  }

  private async getFundsNeedingDeeperHistory() {
    try {
      // Look for funds that have some data but need more historical depth for scoring
      const result = await db
        .select({
          id: funds.id,
          schemeCode: funds.schemeCode,
          fundName: funds.fundName,
          category: funds.category,
          status: funds.status
        })
        .from(funds)
        .leftJoin(
          sql`(SELECT fund_id, COUNT(*) as record_count FROM nav_data GROUP BY fund_id) nav_counts`,
          sql`nav_counts.fund_id = ${funds.id}`
        )
        .where(
          and(
            eq(funds.status, 'ACTIVE'),
            sql`COALESCE(nav_counts.record_count, 0) BETWEEN 100 AND 252`, // Some data but not enough for 1-year analysis
            sql`${funds.schemeCode} IS NOT NULL`,
            sql`${funds.schemeCode} ~ '^[0-9]+$'`,
            sql`${funds.category} IN ('Equity', 'Debt', 'Hybrid')` // Focus on major categories
          )
        )
        .orderBy(sql`COALESCE(nav_counts.record_count, 0) DESC`) // Prioritize funds with more existing data
        .limit(this.BATCH_SIZE);

      return result;
    } catch (error) {
      console.error('Error fetching funds needing deeper history:', error);
      return [];
    }
  }

  private async getFundsNeedingHistoricalData() {
    try {
      // Smart targeting: focus on scheme code ranges that historically work
      const result = await db
        .select({
          id: funds.id,
          schemeCode: funds.schemeCode,
          fundName: funds.fundName,
          category: funds.category,
          status: funds.status
        })
        .from(funds)
        .leftJoin(
          sql`(SELECT fund_id, COUNT(*) as record_count FROM nav_data GROUP BY fund_id) nav_counts`,
          sql`nav_counts.fund_id = ${funds.id}`
        )
        .where(
          and(
            eq(funds.status, 'ACTIVE'),
            sql`${funds.schemeCode} IS NOT NULL`,
            sql`${funds.schemeCode} ~ '^[0-9]+$'`,
            sql`COALESCE(nav_counts.record_count, 0) = 0`,
            // Target scheme codes with proven high success patterns
            or(
              sql`${funds.schemeCode} ~ '^120[0-9]{3}$'`,  // 120xxx - highest success rate
              sql`${funds.schemeCode} ~ '^119[0-9]{3}$'`,  // 119xxx - high success rate
              sql`${funds.schemeCode} ~ '^118[0-9]{3}$'`,  // 118xxx - high success rate
              sql`${funds.schemeCode} ~ '^100[0-9]{3}$'`,  // 100xxx - proven pattern
              sql`${funds.schemeCode} ~ '^101[0-9]{3}$'`,  // 101xxx - proven pattern
              sql`${funds.schemeCode} ~ '^102[0-9]{3}$'`,  // 102xxx - proven pattern
              sql`${funds.schemeCode} ~ '^147[0-9]{3}$'`,  // 147xxx - proven pattern
              sql`${funds.schemeCode} ~ '^145[0-9]{3}$'`   // 145xxx - proven pattern
            )
          )
        )
        .orderBy(
          // Prioritize by proven successful scheme code patterns
          sql`CASE 
            WHEN ${funds.schemeCode} ~ '^120[0-9]{3}$' THEN 1
            WHEN ${funds.schemeCode} ~ '^119[0-9]{3}$' THEN 2
            WHEN ${funds.schemeCode} ~ '^118[0-9]{3}$' THEN 3
            WHEN ${funds.schemeCode} ~ '^100[0-9]{3}$' THEN 4
            WHEN ${funds.schemeCode} ~ '^101[0-9]{3}$' THEN 5
            WHEN ${funds.schemeCode} ~ '^102[0-9]{3}$' THEN 6
            WHEN ${funds.schemeCode} ~ '^147[0-9]{3}$' THEN 7
            WHEN ${funds.schemeCode} ~ '^145[0-9]{3}$' THEN 8
            ELSE 9
          END`,
          sql`CASE 
            WHEN ${funds.category} = 'Equity' THEN 1
            WHEN ${funds.category} = 'Debt' THEN 2
            ELSE 3
          END`,
          funds.id
        )
        .limit(this.BATCH_SIZE);

      // If no funds in the successful range, try other ranges
      if (result.length === 0) {
        const fallbackResult = await db
          .select({
            id: funds.id,
            schemeCode: funds.schemeCode,
            fundName: funds.fundName,
            category: funds.category,
            status: funds.status
          })
          .from(funds)
          .leftJoin(
            sql`(SELECT fund_id, COUNT(*) as record_count FROM nav_data GROUP BY fund_id) nav_counts`,
            sql`nav_counts.fund_id = ${funds.id}`
          )
          .where(
            and(
              eq(funds.status, 'ACTIVE'),
              sql`COALESCE(nav_counts.record_count, 0) < 100`,
              sql`${funds.schemeCode} IS NOT NULL`,
              sql`${funds.schemeCode} ~ '^[0-9]+$'`
            )
          )
          .orderBy(sql`COALESCE(nav_counts.record_count, 0) ASC`, funds.id)
          .limit(this.BATCH_SIZE)
          .offset(Math.max(0, batchOffset - 3396)); // Adjust offset for broader range
        
        return fallbackResult;
      }

      return result;
    } catch (error) {
      console.error('Error fetching funds needing data:', error);
      return [];
    }
  }

  private async importHistoricalDataForFund(fund: any): Promise<number> {
    try {
      // First, check what's the latest date we have for this fund
      const latestNavResult = await db.execute(sql`
        SELECT MAX(nav_date) as latest_date 
        FROM nav_data 
        WHERE fund_id = ${fund.id}
      `);
      
      const latestDate = latestNavResult.rows[0]?.latest_date;
      const cutoffDate = latestDate || new Date('2020-01-01'); // If no data, start from 2020
      
      console.log(`  Latest data for ${fund.fundName}: ${latestDate || 'No data'}`);
      
      // Fetch all data from API and filter only what we need
      const allHistoricalData = await this.fetchAllHistoricalNavFromAPI(fund.schemeCode);

      if (allHistoricalData && allHistoricalData.length > 0) {
        // Filter to only get data newer than what we already have
        const newDataOnly = allHistoricalData.filter(nav => {
          const navDate = new Date(nav.date);
          return navDate > cutoffDate;
        });
        
        console.log(`  Found ${newDataOnly.length} new records (${allHistoricalData.length} total)`);
        
        if (newDataOnly.length > 0) {
          const insertedCount = await this.bulkInsertNavData(fund.id, newDataOnly);
          this.processedFundIds.add(fund.id);
          return insertedCount;
        }
      }
      
      // Mark fund as processed even if no new data
      this.processedFundIds.add(fund.id);
      return 0;
    } catch (error) {
      console.error(`Error importing data for fund ${fund.id}:`, error);
      this.processedFundIds.add(fund.id);
      return 0;
    }
  }

  private generateMonthRanges(monthsBack: number) {
    const ranges = [];
    const now = new Date();

    for (let i = 1; i <= monthsBack; i++) {
      const date = new Date(now.getFullYear(), now.getMonth() - i, 1);
      ranges.push({
        year: date.getFullYear(),
        month: date.getMonth() + 1
      });
    }

    return ranges;
  }

  private async fetchAllHistoricalNavFromAPI(schemeCode: string): Promise<any[]> {
    // Use MFAPI.in basic endpoint to get all historical data at once
    try {
      console.log(`  Fetching data from MFAPI.in for scheme ${schemeCode}`);
      const response = await axios.get(`https://api.mfapi.in/mf/${schemeCode}`, {
        timeout: 15000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; MutualFundAnalyzer/1.0)',
          'Accept': 'application/json'
        }
      });

      if (response.data && response.data.data && Array.isArray(response.data.data)) {
        console.log(`  Received ${response.data.data.length} NAV entries from MFAPI.in`);
        
        const processedData = response.data.data
          .map((entry: HistoricalNavEntry) => {
            // Convert DD-MM-YYYY to YYYY-MM-DD format
            const dateParts = entry.date.split('-');
            if (dateParts.length === 3) {
              const formattedDate = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
              return {
                nav_date: formattedDate,
                nav_value: parseFloat(entry.nav)
              };
            }
            return null;
          })
          .filter((entry: any) => 
            entry && !isNaN(entry.nav_value) && entry.nav_value > 0
          );

        console.log(`  Processed ${processedData.length} valid NAV entries`);
        return processedData;
      }

      console.log(`  No data array found in response for scheme ${schemeCode}`);
      return [];
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.log(`  Error fetching data for scheme ${schemeCode}:`, errorMessage);
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return []; // No data available for this scheme
      }
      return []; // Return empty on any error to continue processing
    }
  }

  private async fetchHistoricalNavFromAPI(schemeCode: string, year: number, month: number): Promise<any[]> {
    // Use MFAPI.in basic endpoint (same as successful earlier imports)
    try {
      const response = await axios.get(`https://api.mfapi.in/mf/${schemeCode}`, {
        timeout: 15000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; MutualFundAnalyzer/1.0)',
          'Accept': 'application/json'
        }
      });

      if (response.data && response.data.data && Array.isArray(response.data.data)) {
        // Filter data for the specific month/year requested
        const targetYear = year;
        const targetMonth = month;
        
        return response.data.data
          .map((entry: HistoricalNavEntry) => ({
            nav_date: entry.date,
            nav_value: parseFloat(entry.nav)
          }))
          .filter((entry: { nav_date: string; nav_value: number }) => {
            if (isNaN(entry.nav_value) || entry.nav_value <= 0) return false;
            
            // Parse date and check if it matches target month/year
            try {
              const entryDate = new Date(entry.nav_date);
              return entryDate.getFullYear() === targetYear && 
                     (entryDate.getMonth() + 1) === targetMonth;
            } catch {
              return false;
            }
          });
      }

      return [];
    } catch (error) {
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return []; // No data available for this scheme
      }
      return []; // Return empty on any error to continue processing
    }
  }

  private async bulkInsertNavData(fundId: number, navDataArray: any[]): Promise<number> {
    if (!navDataArray || navDataArray.length === 0) {
      console.log(`  No NAV data to insert for fund ${fundId}`);
      return 0;
    }

    try {
      const navRecords = navDataArray.map(nav => ({
        fundId: fundId,
        navDate: nav.nav_date,
        navValue: nav.nav_value.toString()
      }));

      console.log(`  Attempting to insert ${navRecords.length} NAV records for fund ${fundId}`);

      await db.insert(navData)
        .values(navRecords)
        .onConflictDoNothing({
          target: [navData.fundId, navData.navDate]
        });

      console.log(`  Successfully inserted ${navRecords.length} NAV records for fund ${fundId}`);
      return navRecords.length;
    } catch (error) {
      console.error(`Error inserting NAV data for fund ${fundId}:`, error);
      return 0;
    }
  }
}

export const backgroundHistoricalImporter = new BackgroundHistoricalImporter();

// Auto-start the background importer
setTimeout(() => {
  backgroundHistoricalImporter.start();
}, 5000); // Start after 5 seconds to allow system initialization