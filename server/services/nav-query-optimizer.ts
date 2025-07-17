import { pool } from '../db';

export class NavQueryOptimizer {
  /**
   * Get NAV data for a fund using the partitioned table
   * This query will automatically use partition pruning for better performance
   */
  static async getNavDataForFund(fundId: number, startDate?: string, endDate?: string) {
    const query = `
      SELECT 
        nav_date,
        nav_value,
        nav_change,
        nav_change_pct
      FROM nav_data
      WHERE fund_id = $1
      ${startDate ? 'AND nav_date >= $2' : ''}
      ${endDate ? `AND nav_date <= $${startDate ? '3' : '2'}` : ''}
      ORDER BY nav_date ASC
    `;
    
    const params = [fundId];
    if (startDate) params.push(startDate);
    if (endDate) params.push(endDate);
    
    const result = await pool.query(query, params);
    return result.rows;
  }
  
  /**
   * Get latest NAV for multiple funds
   * Uses partition pruning to only scan recent partitions
   */
  static async getLatestNavForFunds(fundIds: number[]) {
    const query = `
      WITH latest_navs AS (
        SELECT DISTINCT ON (fund_id)
          fund_id,
          nav_date,
          nav_value,
          nav_change_pct
        FROM nav_data
        WHERE fund_id = ANY($1)
          AND nav_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY fund_id, nav_date DESC
      )
      SELECT * FROM latest_navs
    `;
    
    const result = await pool.query(query, [fundIds]);
    return result.rows;
  }
  
  /**
   * Get NAV data for a specific date range across all funds
   * Efficiently uses partition pruning
   */
  static async getNavDataByDateRange(startDate: string, endDate: string, limit = 1000) {
    const query = `
      SELECT 
        fund_id,
        nav_date,
        nav_value,
        nav_change_pct
      FROM nav_data
      WHERE nav_date BETWEEN $1 AND $2
      ORDER BY nav_date DESC, fund_id
      LIMIT $3
    `;
    
    const result = await pool.query(query, [startDate, endDate, limit]);
    return result.rows;
  }
  
  /**
   * Get performance metrics for a fund over a period
   * Leverages partitioning for efficient calculation
   */
  static async getFundPerformanceMetrics(fundId: number, periodDays: number) {
    const query = `
      WITH nav_range AS (
        SELECT 
          MIN(nav_value) FILTER (WHERE nav_date = start_date.date) as start_nav,
          MAX(nav_value) FILTER (WHERE nav_date = end_date.date) as end_nav,
          COUNT(*) as data_points,
          STDDEV(nav_change_pct) as volatility
        FROM nav_data,
          (SELECT MAX(nav_date) as date FROM nav_data WHERE fund_id = $1) as end_date,
          (SELECT MAX(nav_date) as date FROM nav_data 
           WHERE fund_id = $1 AND nav_date <= CURRENT_DATE - INTERVAL '$2 days') as start_date
        WHERE fund_id = $1
          AND nav_date BETWEEN start_date.date AND end_date.date
      )
      SELECT 
        CASE 
          WHEN start_nav > 0 
          THEN ((end_nav - start_nav) / start_nav * 100)::DECIMAL(10,2)
          ELSE 0 
        END as return_pct,
        volatility::DECIMAL(10,4) as volatility,
        data_points
      FROM nav_range
    `;
    
    const result = await pool.query(query, [fundId, periodDays]);
    return result.rows[0];
  }
  
  /**
   * Bulk insert NAV data with automatic partition creation
   */
  static async bulkInsertNavData(navRecords: any[]) {
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Use COPY for bulk insert which is much faster
      const copyQuery = `
        COPY nav_data (fund_id, nav_date, nav_value, nav_change, nav_change_pct, aum_cr)
        FROM STDIN WITH (FORMAT csv)
      `;
      
      const stream = client.query(copyQuery);
      
      for (const record of navRecords) {
        const row = [
          record.fund_id,
          record.nav_date,
          record.nav_value,
          record.nav_change || null,
          record.nav_change_pct || null,
          record.aum_cr || null
        ].join(',');
        
        stream.write(row + '\n');
      }
      
      await stream.end();
      await client.query('COMMIT');
      
      return { success: true, inserted: navRecords.length };
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}

export default NavQueryOptimizer;