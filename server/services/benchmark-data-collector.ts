/**
 * Benchmark Data Collector
 * Collects historical benchmark data from various authentic sources
 */

import { pool } from '../db';
import axios from 'axios';

export class BenchmarkDataCollector {
  private static readonly ALPHA_VANTAGE_KEY = process.env.ALPHA_VANTAGE_API_KEY;
  
  /**
   * Fetch benchmark data from Alpha Vantage
   */
  static async fetchFromAlphaVantage(symbol: string, benchmarkName: string) {
    try {
      if (!this.ALPHA_VANTAGE_KEY) {
        console.error('Alpha Vantage API key not configured');
        return { success: false, error: 'API key not configured' };
      }

      // Map benchmark names to Alpha Vantage symbols
      const symbolMap: Record<string, string> = {
        'Nifty 50 TRI': 'NSEI',
        'Nifty 500 TRI': 'NIFTY500.NS',
        'Nifty 100 TRI': 'NIFTY100.NS',
        'Nifty Midcap 100 TRI': 'NIFTYMIDCAP100.NS',
        'Nifty Smallcap 100 TRI': 'NIFTYSMALLCAP100.NS'
      };

      const avSymbol = symbolMap[benchmarkName] || symbol;
      
      const url = `https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=${avSymbol}&outputsize=full&apikey=${this.ALPHA_VANTAGE_KEY}`;
      
      console.log(`Fetching ${benchmarkName} data from Alpha Vantage...`);
      const response = await axios.get(url);
      
      if (response.data['Error Message']) {
        return { success: false, error: response.data['Error Message'] };
      }
      
      if (response.data['Note']) {
        return { success: false, error: 'API limit reached' };
      }
      
      const timeSeries = response.data['Time Series (Daily)'];
      if (!timeSeries) {
        return { success: false, error: 'No data found' };
      }
      
      // Convert and insert data
      let inserted = 0;
      const entries = Object.entries(timeSeries);
      
      for (const [date, values] of entries) {
        const closeValue = parseFloat((values as any)['4. close']);
        
        await pool.query(`
          INSERT INTO market_indices (index_name, index_date, close_value)
          VALUES ($1, $2, $3)
          ON CONFLICT (index_name, index_date) 
          DO UPDATE SET close_value = EXCLUDED.close_value
        `, [benchmarkName, date, closeValue]);
        
        inserted++;
      }
      
      console.log(`Inserted ${inserted} records for ${benchmarkName}`);
      return { success: true, recordsInserted: inserted };
      
    } catch (error) {
      console.error(`Error fetching ${benchmarkName} from Alpha Vantage:`, error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Fetch CRISIL indices data
   * Note: CRISIL data requires subscription and authentication
   */
  static async fetchCRISILIndices() {
    console.log('CRISIL indices require subscription for data access');
    console.log('Available indices: CRISIL Liquid Fund Index, CRISIL Short Term Debt Index, etc.');
    
    return {
      message: 'CRISIL data requires valid subscription',
      indices: [
        'CRISIL Liquid Fund Index',
        'CRISIL Ultra Short Term Debt Index',
        'CRISIL Short Term Debt Index',
        'CRISIL Corporate Bond Composite Index',
        'CRISIL Credit Risk Debt Index',
        'CRISIL Composite Bond Fund Index',
        'CRISIL Overnight Index',
        'CRISIL Banking & PSU Debt Index',
        'CRISIL Gilt Index'
      ]
    };
  }
  
  /**
   * Fetch Gold and Silver price indices
   */
  static async fetchCommodityIndices() {
    try {
      if (!this.ALPHA_VANTAGE_KEY) {
        return { success: false, error: 'API key not configured' };
      }
      
      // Fetch Gold prices
      console.log('Fetching Gold price data...');
      let url = `https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=XAU&to_currency=INR&apikey=${this.ALPHA_VANTAGE_KEY}`;
      let response = await axios.get(url);
      
      if (response.data['Realtime Currency Exchange Rate']) {
        const goldPrice = parseFloat(response.data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        const date = new Date().toISOString().split('T')[0];
        
        await pool.query(`
          INSERT INTO market_indices (index_name, index_date, close_value)
          VALUES ($1, $2, $3)
          ON CONFLICT (index_name, index_date) 
          DO UPDATE SET close_value = EXCLUDED.close_value
        `, ['Gold Price Index', date, goldPrice]);
      }
      
      // Fetch Silver prices
      console.log('Fetching Silver price data...');
      url = `https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=XAG&to_currency=INR&apikey=${this.ALPHA_VANTAGE_KEY}`;
      response = await axios.get(url);
      
      if (response.data['Realtime Currency Exchange Rate']) {
        const silverPrice = parseFloat(response.data['Realtime Currency Exchange Rate']['5. Exchange Rate']);
        const date = new Date().toISOString().split('T')[0];
        
        await pool.query(`
          INSERT INTO market_indices (index_name, index_date, close_value)
          VALUES ($1, $2, $3)
          ON CONFLICT (index_name, index_date) 
          DO UPDATE SET close_value = EXCLUDED.close_value
        `, ['Silver Price Index', date, silverPrice]);
      }
      
      return { success: true, message: 'Commodity indices updated' };
      
    } catch (error) {
      console.error('Error fetching commodity indices:', error);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Collect all missing benchmark data
   */
  static async collectMissingBenchmarks() {
    try {
      // Get list of missing benchmarks
      const result = await pool.query(`
        SELECT DISTINCT 
          f.benchmark_name,
          COUNT(*) as fund_count
        FROM funds f
        LEFT JOIN (
          SELECT DISTINCT index_name 
          FROM market_indices
        ) mi ON f.benchmark_name = mi.index_name
        WHERE f.benchmark_name IS NOT NULL
        AND mi.index_name IS NULL
        GROUP BY f.benchmark_name
        ORDER BY fund_count DESC
        LIMIT 5
      `);
      
      const missingBenchmarks = result.rows;
      console.log(`Found ${missingBenchmarks.length} missing benchmarks to collect`);
      
      const results = [];
      
      for (const benchmark of missingBenchmarks) {
        console.log(`\nCollecting data for ${benchmark.benchmark_name} (used by ${benchmark.fund_count} funds)`);
        
        // Skip CRISIL indices as they require subscription
        if (benchmark.benchmark_name.includes('CRISIL')) {
          results.push({
            benchmark: benchmark.benchmark_name,
            status: 'skipped',
            reason: 'Requires CRISIL subscription'
          });
          continue;
        }
        
        // Try to fetch from Alpha Vantage
        const result = await this.fetchFromAlphaVantage('', benchmark.benchmark_name);
        results.push({
          benchmark: benchmark.benchmark_name,
          ...result
        });
        
        // Rate limit: 5 calls per minute
        await new Promise(resolve => setTimeout(resolve, 12000));
      }
      
      return results;
      
    } catch (error) {
      console.error('Error collecting missing benchmarks:', error);
      throw error;
    }
  }
}

export default BenchmarkDataCollector;