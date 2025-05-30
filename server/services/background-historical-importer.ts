/**
 * Background Historical NAV Data Importer
 * Runs continuously to import authentic historical data from free APIs
 * Designed to work 24/7 in the background without user intervention
 */

import { db } from '../db.js';
import { funds, navData } from '../../shared/schema.js';
import { eq, lt, sql, and, isNull } from 'drizzle-orm';
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
  private readonly DELAY_BETWEEN_BATCHES = 15000; // 15 seconds between batches
  private readonly DELAY_BETWEEN_REQUESTS = 500; // 0.5 seconds between API calls
  private readonly MAX_MONTHS_BACK = 120; // Import up to 10 years of data
  private readonly PARALLEL_REQUESTS = 3; // Run 3 parallel requests per fund
  private readonly MAX_EMPTY_BATCHES = 5; // Stop after 5 consecutive empty batches

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
        const fundsToProcess = await this.getFundsNeedingHistoricalData();
        
        if (fundsToProcess.length === 0) {
          console.log('📈 All funds processed! Restarting from beginning...');
          // Reset and start over to catch any new funds or missed data
          await new Promise(resolve => setTimeout(resolve, 300000)); // Wait 5 minutes
          continue;
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

  private async getFundsNeedingHistoricalData() {
    try {
      // Calculate a different offset based on batch number to avoid cycling
      const batchOffset = Math.floor(this.currentProgress.totalFundsProcessed / this.BATCH_SIZE) * this.BATCH_SIZE;
      
      // Use a more diverse selection strategy
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
            sql`COALESCE(nav_counts.record_count, 0) < 100`,
            sql`${funds.schemeCode} IS NOT NULL`,
            sql`${funds.schemeCode} ~ '^[0-9]+$'`,
            // Explore all funds above 100000 to access the full database
            sql`${funds.schemeCode}::INTEGER > 100000`
          )
        )
        .orderBy(sql`RANDOM()`, funds.id)  // Add randomization to explore different funds
        .limit(this.BATCH_SIZE)
        .offset(batchOffset);

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
    // Use single API call per fund (same as successful imports)
    try {
      const historicalData = await this.fetchAllHistoricalNavFromAPI(fund.schemeCode);

      if (historicalData && historicalData.length > 0) {
        const insertedCount = await this.bulkInsertNavData(fund.id, historicalData);
        this.processedFundIds.add(fund.id);
        return insertedCount;
      }
      
      // Mark fund as processed even if no data to avoid cycling
      this.processedFundIds.add(fund.id);
      return 0;
    } catch (error) {
      // Mark fund as processed even on error to avoid retry loops
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
      console.log(`  Error fetching data for scheme ${schemeCode}:`, error.message);
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