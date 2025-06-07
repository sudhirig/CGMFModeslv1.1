/**
 * ELIVATE Data Integrity Validator
 * Ensures all ELIVATE Framework data comes from authentic sources only
 * Prevents synthetic data contamination and validates data freshness
 */

import { pool } from '../db';

interface DataIntegrityReport {
  isValid: boolean;
  missingIndices: string[];
  staleData: string[];
  totalIndices: number;
  validIndices: number;
  lastUpdate: Date | null;
  recommendations: string[];
}

export class ElivateDataIntegrityValidator {
  
  private static readonly REQUIRED_INDICES = [
    'US GDP GROWTH', 'US FED RATE', 'US DOLLAR INDEX', 'CHINA PMI',
    'INDIA GDP GROWTH', 'GST COLLECTION', 'IIP GROWTH', 'INDIA PMI',
    'CPI INFLATION', 'WPI INFLATION', 'REPO RATE', '10Y GSEC YIELD',
    'FII FLOWS', 'DII FLOWS', 'SIP INFLOWS',
    'EARNINGS GROWTH', 'STOCKS ABOVE 200DMA', 'ADVANCE DECLINE RATIO'
  ];

  private static readonly KNOWN_SYNTHETIC_VALUES = [
    '2.30', '5.25', '102.80', '50.8', '6.80', '176000.00', '4.30', '57.5',
    '4.70', '3.10', '6.50', '7.15', '20.50', '3.20', '15.30', '16500.00',
    '12800.00', '18200.00', '65.3', '14.20', '1.20'
  ];

  /**
   * Comprehensive data integrity validation
   */
  static async validateDataIntegrity(): Promise<DataIntegrityReport> {
    console.log('Starting comprehensive ELIVATE data integrity validation...');
    
    try {
      const report: DataIntegrityReport = {
        isValid: true,
        missingIndices: [],
        staleData: [],
        totalIndices: this.REQUIRED_INDICES.length,
        validIndices: 0,
        lastUpdate: null,
        recommendations: []
      };

      // Check for missing indices
      const availableIndicesResult = await pool.query(`
        SELECT DISTINCT index_name 
        FROM market_indices 
        WHERE index_name = ANY($1)
        AND index_date >= CURRENT_DATE - INTERVAL '7 days'
      `, [this.REQUIRED_INDICES]);

      const availableIndices = availableIndicesResult.rows.map(row => row.index_name);
      report.missingIndices = this.REQUIRED_INDICES.filter(
        index => !availableIndices.includes(index)
      );

      // Check for synthetic data contamination
      const syntheticDataResult = await pool.query(`
        SELECT index_name, close_value, index_date
        FROM market_indices 
        WHERE close_value = ANY($1)
        AND index_name = ANY($2)
      `, [this.KNOWN_SYNTHETIC_VALUES, this.REQUIRED_INDICES]);

      if (syntheticDataResult.rows.length > 0) {
        report.isValid = false;
        report.recommendations.push(`Found ${syntheticDataResult.rows.length} synthetic data entries that must be removed`);
        
        // Log synthetic entries for removal
        for (const row of syntheticDataResult.rows) {
          console.warn(`Synthetic data detected: ${row.index_name} = ${row.close_value} on ${row.index_date}`);
        }
      }

      // Check data freshness (within last 7 days)
      const staleDataResult = await pool.query(`
        SELECT index_name, MAX(index_date) as last_update
        FROM market_indices 
        WHERE index_name = ANY($1)
        GROUP BY index_name
        HAVING MAX(index_date) < CURRENT_DATE - INTERVAL '7 days'
      `, [this.REQUIRED_INDICES]);

      report.staleData = staleDataResult.rows.map(row => row.index_name);
      
      // Get most recent update timestamp
      const lastUpdateResult = await pool.query(`
        SELECT MAX(created_at) as last_update
        FROM market_indices 
        WHERE index_name = ANY($1)
      `, [this.REQUIRED_INDICES]);

      if (lastUpdateResult.rows.length > 0) {
        report.lastUpdate = lastUpdateResult.rows[0].last_update;
      }

      // Calculate valid indices count
      report.validIndices = availableIndices.length - report.staleData.length;

      // Determine overall validity
      if (report.missingIndices.length > 0) {
        report.isValid = false;
        report.recommendations.push(`Missing ${report.missingIndices.length} required market indices`);
      }

      if (report.staleData.length > 0) {
        report.isValid = false;
        report.recommendations.push(`${report.staleData.length} indices have stale data (older than 7 days)`);
      }

      if (report.isValid) {
        report.recommendations.push('All ELIVATE data integrity checks passed');
      } else {
        report.recommendations.push('Data collection from authorized sources required before ELIVATE calculation');
      }

      return report;

    } catch (error) {
      console.error('Data integrity validation error:', error);
      return {
        isValid: false,
        missingIndices: this.REQUIRED_INDICES,
        staleData: [],
        totalIndices: this.REQUIRED_INDICES.length,
        validIndices: 0,
        lastUpdate: null,
        recommendations: ['Failed to validate data integrity - database connection issue']
      };
    }
  }

  /**
   * Remove all synthetic data contamination
   */
  static async cleanSyntheticData(): Promise<{ removed: number; success: boolean }> {
    console.log('Removing synthetic data contamination from market indices...');
    
    try {
      const result = await pool.query(`
        DELETE FROM market_indices 
        WHERE close_value = ANY($1)
        AND index_name = ANY($2)
      `, [this.KNOWN_SYNTHETIC_VALUES, this.REQUIRED_INDICES]);

      console.log(`Removed ${result.rowCount} synthetic data entries`);
      
      return {
        removed: result.rowCount || 0,
        success: true
      };

    } catch (error) {
      console.error('Synthetic data cleanup error:', error);
      return {
        removed: 0,
        success: false
      };
    }
  }

  /**
   * Validate ELIVATE calculation prerequisites
   */
  static async validateCalculationPrerequisites(): Promise<{
    canCalculate: boolean;
    missingData: string[];
    errors: string[];
  }> {
    const report = await this.validateDataIntegrity();
    
    return {
      canCalculate: report.isValid && report.missingIndices.length === 0,
      missingData: [...report.missingIndices, ...report.staleData],
      errors: report.recommendations.filter(rec => rec.includes('required') || rec.includes('missing'))
    };
  }

  /**
   * Get detailed data source status
   */
  static async getDataSourceStatus(): Promise<{
    [key: string]: {
      available: boolean;
      lastUpdate: string | null;
      value: number | null;
      source: 'authentic' | 'missing' | 'stale';
    }
  }> {
    const status: any = {};
    
    for (const indexName of this.REQUIRED_INDICES) {
      const result = await pool.query(`
        SELECT close_value, index_date, created_at
        FROM market_indices 
        WHERE index_name = $1
        ORDER BY index_date DESC, created_at DESC
        LIMIT 1
      `, [indexName]);

      if (result.rows.length === 0) {
        status[indexName] = {
          available: false,
          lastUpdate: null,
          value: null,
          source: 'missing'
        };
      } else {
        const row = result.rows[0];
        const indexDate = new Date(row.index_date);
        const daysDiff = Math.floor((Date.now() - indexDate.getTime()) / (1000 * 60 * 60 * 24));
        
        status[indexName] = {
          available: true,
          lastUpdate: row.index_date,
          value: parseFloat(row.close_value),
          source: daysDiff > 7 ? 'stale' : 'authentic'
        };
      }
    }

    return status;
  }
}

export const elivateDataIntegrityValidator = ElivateDataIntegrityValidator;