/**
 * Benchmark Data Importer Service
 * Imports benchmark data from legitimate sources
 */

import { pool } from '../db';
import axios from 'axios';
import * as fs from 'fs';
import * as path from 'path';
import { parseFile } from '@fast-csv/parse';

export class BenchmarkDataImporter {
  
  /**
   * Import benchmark data from NSE official website
   * NSE provides free access to index data
   */
  static async importFromNSE() {
    try {
      console.log('Importing benchmark data from NSE...');
      
      // NSE provides historical data downloads for indices
      // These are the official sources for NIFTY indices
      const nseIndices = [
        'NIFTY 50',
        'NIFTY 100', 
        'NIFTY 200',
        'NIFTY 500',
        'NIFTY MIDCAP 100',
        'NIFTY SMALLCAP 100',
        'NIFTY NEXT 50'
      ];
      
      // NSE historical data is available via their official data products
      // Users can download from: https://www.nseindia.com/market-data/live-equity-market
      
      return {
        message: 'NSE provides official data downloads. Visit NSE website for historical index data.',
        indices: nseIndices
      };
    } catch (error) {
      console.error('Error importing NSE data:', error);
      throw error;
    }
  }
  
  /**
   * Import benchmark data from BSE official website
   * BSE also provides index data downloads
   */
  static async importFromBSE() {
    try {
      console.log('Checking BSE data sources...');
      
      // BSE indices available from official sources
      const bseIndices = [
        'BSE SENSEX',
        'BSE 100',
        'BSE 200',
        'BSE 500',
        'BSE MIDCAP',
        'BSE SMALLCAP'
      ];
      
      // BSE provides data via: https://www.bseindia.com/indices/IndexArchiveData.html
      
      return {
        message: 'BSE provides official index data. Visit BSE website for downloads.',
        indices: bseIndices
      };
    } catch (error) {
      console.error('Error importing BSE data:', error);
      throw error;
    }
  }
  
  /**
   * Import benchmark data from a CSV file
   * This allows manual import of legitimately obtained data
   */
  static async importFromCSV(filePath: string, benchmarkName: string) {
    try {
      console.log(`Importing benchmark data from CSV: ${filePath}`);
      
      const records: any[] = [];
      
      return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
          .pipe(parseFile({ headers: true }))
          .on('data', (row) => {
            records.push(row);
          })
          .on('end', async () => {
            try {
              // Process and insert records
              let inserted = 0;
              
              for (const record of records) {
                // Adjust field names based on CSV format
                const indexDate = record.Date || record.date;
                const closeValue = record.Close || record.close || record.Value || record.value;
                
                if (indexDate && closeValue) {
                  await pool.query(`
                    INSERT INTO market_indices (index_name, index_date, close_value)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (index_name, index_date) 
                    DO UPDATE SET close_value = EXCLUDED.close_value
                  `, [benchmarkName, indexDate, parseFloat(closeValue)]);
                  
                  inserted++;
                }
              }
              
              console.log(`Imported ${inserted} records for ${benchmarkName}`);
              resolve({ success: true, recordsImported: inserted });
            } catch (error) {
              reject(error);
            }
          })
          .on('error', reject);
      });
    } catch (error) {
      console.error('Error importing CSV data:', error);
      throw error;
    }
  }
  
  /**
   * Import data from Excel file (manual upload)
   * Processes benchmark data from uploaded Excel files
   */
  static async importFromExcel(filePath: string) {
    try {
      console.log(`Processing Excel file: ${filePath}`);
      
      // For Excel files, we would need to use a library like xlsx
      // This is a placeholder for manual data import
      
      return {
        message: 'Excel import requires manual processing. Convert to CSV for automatic import.',
        recommendation: 'Use CSV format for easier import'
      };
    } catch (error) {
      console.error('Error importing Excel data:', error);
      throw error;
    }
  }
  
  /**
   * Get legitimate data sources for benchmarks
   */
  static async getDataSources() {
    return {
      nse: {
        name: 'National Stock Exchange',
        website: 'https://www.nseindia.com',
        dataSection: 'Market Data > Indices > Historical Data',
        indices: ['NIFTY 50', 'NIFTY 100', 'NIFTY 500', 'NIFTY MIDCAP', 'NIFTY SMALLCAP'],
        access: 'Free with registration'
      },
      bse: {
        name: 'Bombay Stock Exchange',
        website: 'https://www.bseindia.com',
        dataSection: 'Markets > Indices > Index Archive',
        indices: ['SENSEX', 'BSE 100', 'BSE 200', 'BSE 500', 'BSE MIDCAP', 'BSE SMALLCAP'],
        access: 'Free download'
      },
      amfi: {
        name: 'Association of Mutual Funds in India',
        website: 'https://www.amfiindia.com',
        dataSection: 'Research & Information',
        data: 'NAV data and fund information',
        access: 'Free'
      },
      rbi: {
        name: 'Reserve Bank of India',
        website: 'https://www.rbi.org.in',
        dataSection: 'Database on Indian Economy',
        data: 'Interest rates, inflation, economic indicators',
        access: 'Free'
      },
      crisil: {
        name: 'CRISIL',
        website: 'https://www.crisil.com',
        dataSection: 'Indices',
        indices: ['CRISIL Liquid Fund Index', 'CRISIL Debt Indices'],
        access: 'Subscription required'
      }
    };
  }
  
  /**
   * Calculate returns from benchmark data
   */
  static async calculateBenchmarkReturns(benchmarkName: string) {
    try {
      const result = await pool.query(`
        WITH ordered_data AS (
          SELECT 
            index_date,
            close_value,
            ROW_NUMBER() OVER (ORDER BY index_date DESC) as rn
          FROM market_indices
          WHERE index_name = $1
          ORDER BY index_date DESC
        ),
        returns AS (
          SELECT
            -- 1 Week Return
            (SELECT ((d1.close_value - d2.close_value) / d2.close_value * 100)
             FROM ordered_data d1, ordered_data d2
             WHERE d1.rn = 1 AND d2.rn = 6) as week_1_return,
            
            -- 1 Month Return  
            (SELECT ((d1.close_value - d2.close_value) / d2.close_value * 100)
             FROM ordered_data d1, ordered_data d2
             WHERE d1.rn = 1 AND d2.rn = 22) as month_1_return,
            
            -- 3 Months Return
            (SELECT ((d1.close_value - d2.close_value) / d2.close_value * 100)
             FROM ordered_data d1, ordered_data d2
             WHERE d1.rn = 1 AND d2.rn = 66) as month_3_return,
            
            -- 6 Months Return
            (SELECT ((d1.close_value - d2.close_value) / d2.close_value * 100)
             FROM ordered_data d1, ordered_data d2
             WHERE d1.rn = 1 AND d2.rn = 132) as month_6_return,
            
            -- 1 Year Return
            (SELECT ((d1.close_value - d2.close_value) / d2.close_value * 100)
             FROM ordered_data d1, ordered_data d2
             WHERE d1.rn = 1 AND d2.rn = 252) as year_1_return
        )
        SELECT * FROM returns
      `, [benchmarkName]);
      
      return result.rows[0];
    } catch (error) {
      console.error('Error calculating benchmark returns:', error);
      throw error;
    }
  }
}