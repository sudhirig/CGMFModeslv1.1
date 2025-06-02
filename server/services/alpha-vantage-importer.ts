/**
 * Alpha Vantage NAV Data Importer
 * Imports authentic historical NAV data for Indian mutual funds using Alpha Vantage API
 */

import axios from 'axios';
import { db } from '../db.js';
import { funds, navData } from '../../shared/schema.js';
import { eq, and, sql } from 'drizzle-orm';

interface AlphaVantageTimeSeriesData {
  [date: string]: {
    '1. open': string;
    '2. high': string;
    '3. low': string;
    '4. close': string;
    '5. volume': string;
  };
}

interface AlphaVantageResponse {
  'Meta Data'?: {
    '1. Information': string;
    '2. Symbol': string;
    '3. Last Refreshed': string;
    '4. Time Zone': string;
  };
  'Time Series (Daily)'?: AlphaVantageTimeSeriesData;
  'Error Message'?: string;
  'Note'?: string;
}

class AlphaVantageImporter {
  private readonly API_KEY = process.env.ALPHA_VANTAGE_API_KEY;
  private readonly BASE_URL = 'https://www.alphavantage.co/query';
  private readonly RATE_LIMIT_DELAY = 12000; // 12 seconds between requests (free tier: 5 calls/min)

  constructor() {
    if (!this.API_KEY) {
      throw new Error('Alpha Vantage API key is required');
    }
  }

  /**
   * Import historical NAV data for funds using Alpha Vantage
   */
  async importHistoricalData(batchSize: number = 5): Promise<{
    processed: number;
    imported: number;
    errors: string[];
  }> {
    console.log('🔄 Starting Alpha Vantage historical import...');
    
    const results = {
      processed: 0,
      imported: 0,
      errors: [] as string[]
    };

    try {
      // Get funds that need historical data
      const fundsToProcess = await this.getFundsNeedingData(batchSize);
      
      if (fundsToProcess.length === 0) {
        console.log('✅ No funds need historical data import');
        return results;
      }

      console.log(`📊 Found ${fundsToProcess.length} funds to process`);

      for (const fund of fundsToProcess) {
        try {
          console.log(`📈 Processing ${fund.fundName} (${fund.schemeCode})`);
          
          const navRecords = await this.fetchFundHistoricalData(fund.schemeCode);
          
          if (navRecords.length > 0) {
            const insertedCount = await this.insertNavRecords(fund.id, navRecords);
            results.imported += insertedCount;
            console.log(`✅ Imported ${insertedCount} records for ${fund.fundName}`);
          } else {
            console.log(`⚠️  No data available for ${fund.fundName}`);
          }

          results.processed++;

          // Rate limiting to respect API limits
          if (results.processed < fundsToProcess.length) {
            console.log(`⏱️  Waiting ${this.RATE_LIMIT_DELAY/1000}s for rate limit...`);
            await this.delay(this.RATE_LIMIT_DELAY);
          }

        } catch (error: any) {
          const errorMsg = `Error processing ${fund.fundName}: ${error.message}`;
          console.error(`❌ ${errorMsg}`);
          results.errors.push(errorMsg);
          results.processed++;
        }
      }

      console.log(`📊 Alpha Vantage import completed: ${results.processed} processed, ${results.imported} records imported`);
      return results;

    } catch (error: any) {
      console.error('❌ Alpha Vantage import failed:', error.message);
      results.errors.push(`Import failed: ${error.message}`);
      return results;
    }
  }

  /**
   * Fetch historical NAV data for a specific scheme from Alpha Vantage
   */
  private async fetchFundHistoricalData(schemeCode: string): Promise<Array<{
    navDate: string;
    navValue: number;
  }>> {
    try {
      // Try different symbol formats for Indian mutual funds
      const symbolVariants = [
        `${schemeCode}.BSE`,  // BSE format
        `${schemeCode}.NSE`,  // NSE format
        `IN.${schemeCode}`,   // India prefix
        schemeCode            // Direct scheme code
      ];

      for (const symbol of symbolVariants) {
        try {
          console.log(`🔍 Trying symbol: ${symbol}`);
          
          const response = await axios.get(this.BASE_URL, {
            params: {
              function: 'TIME_SERIES_DAILY',
              symbol: symbol,
              apikey: this.API_KEY,
              outputsize: 'full'
            },
            timeout: 30000
          });

          const data: AlphaVantageResponse = response.data;

          // Check for API error messages
          if (data['Error Message'] || data['Note']) {
            console.log(`⚠️  API message for ${symbol}:`, data['Error Message'] || data['Note']);
            continue;
          }

          if (data['Time Series (Daily)']) {
            const timeSeries = data['Time Series (Daily)'];
            const navRecords: Array<{ navDate: string; navValue: number }> = [];

            for (const [date, values] of Object.entries(timeSeries)) {
              // Use closing price as NAV value
              const navValue = parseFloat(values['4. close']);
              
              if (!isNaN(navValue) && navValue > 0) {
                navRecords.push({
                  navDate: date,
                  navValue: navValue
                });
              }
            }

            if (navRecords.length > 0) {
              console.log(`✅ Found ${navRecords.length} records for symbol ${symbol}`);
              return navRecords.sort((a, b) => new Date(a.navDate).getTime() - new Date(b.navDate).getTime());
            }
          }

        } catch (symbolError: any) {
          console.log(`⚠️  Symbol ${symbol} failed:`, symbolError.message);
          continue;
        }
      }

      return [];

    } catch (error: any) {
      console.error(`❌ Error fetching data for scheme ${schemeCode}:`, error.message);
      return [];
    }
  }

  /**
   * Get funds that need historical data
   */
  private async getFundsNeedingData(limit: number) {
    try {
      const result = await db
        .select({
          id: funds.id,
          schemeCode: funds.schemeCode,
          fundName: funds.fundName,
          amcName: funds.amcName,
          category: funds.category
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
            sql`COALESCE(nav_counts.record_count, 0) < 50` // Less than 50 records
          )
        )
        .orderBy(
          sql`CASE 
            WHEN ${funds.category} = 'Equity' THEN 1
            WHEN ${funds.category} = 'Debt' THEN 2
            ELSE 3
          END`,
          funds.id
        )
        .limit(limit);

      return result;
    } catch (error: any) {
      console.error('❌ Error getting funds needing data:', error.message);
      return [];
    }
  }

  /**
   * Insert NAV records into database
   */
  private async insertNavRecords(fundId: number, navRecords: Array<{
    navDate: string;
    navValue: number;
  }>): Promise<number> {
    try {
      if (navRecords.length === 0) return 0;

      // Prepare records for insertion
      const recordsToInsert = navRecords.map(record => ({
        fundId,
        navValue: record.navValue,
        navDate: new Date(record.navDate)
      }));

      // Insert in batches to avoid memory issues
      const batchSize = 1000;
      let insertedCount = 0;

      for (let i = 0; i < recordsToInsert.length; i += batchSize) {
        const batch = recordsToInsert.slice(i, i + batchSize);
        
        try {
          await db.insert(navData)
            .values(batch)
            .onConflictDoNothing(); // Skip duplicates
          
          insertedCount += batch.length;
        } catch (batchError: any) {
          console.error(`❌ Error inserting batch:`, batchError.message);
        }
      }

      return insertedCount;
    } catch (error: any) {
      console.error(`❌ Error inserting NAV records:`, error.message);
      return 0;
    }
  }

  /**
   * Utility function for delays
   */
  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Test Alpha Vantage connectivity
   */
  async testConnection(): Promise<{ success: boolean; message: string }> {
    try {
      console.log('🔄 Testing Alpha Vantage connection...');
      
      const response = await axios.get(this.BASE_URL, {
        params: {
          function: 'TIME_SERIES_DAILY',
          symbol: 'RELIANCE.BSE',
          apikey: this.API_KEY,
          outputsize: 'compact'
        },
        timeout: 10000
      });

      if (response.data['Error Message']) {
        return {
          success: false,
          message: `API Error: ${response.data['Error Message']}`
        };
      }

      if (response.data['Note']) {
        return {
          success: false,
          message: `API Limit: ${response.data['Note']}`
        };
      }

      if (response.data['Time Series (Daily)']) {
        return {
          success: true,
          message: 'Alpha Vantage connection successful'
        };
      }

      return {
        success: false,
        message: 'Unexpected response format'
      };

    } catch (error: any) {
      return {
        success: false,
        message: `Connection failed: ${error.message}`
      };
    }
  }
}

export const alphaVantageImporter = new AlphaVantageImporter();