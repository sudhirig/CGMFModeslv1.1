import { Router } from 'express';
import { OfficialBenchmarkCollector } from '../services/official-benchmark-collector';
import { pool } from '../db';

const router = Router();

// Collect benchmarks from official sources
router.post('/collect-official', async (req, res) => {
  try {
    console.log('Starting official benchmark collection...');
    
    // Run collection
    const results = await OfficialBenchmarkCollector.collectAllBenchmarks();
    
    // Get summary of what we have
    const summary = await pool.query(`
      SELECT 
        index_name,
        COUNT(*) as data_points,
        MIN(index_date) as earliest_date,
        MAX(index_date) as latest_date,
        MIN(close_value) as min_value,
        MAX(close_value) as max_value
      FROM market_indices
      GROUP BY index_name
      ORDER BY index_name
    `);
    
    res.json({
      success: true,
      results,
      summary: summary.rows,
      message: `Collected ${results.total} benchmark data points from official sources`
    });
  } catch (error) {
    console.error('Benchmark collection error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to collect benchmark data',
      message: error.message 
    });
  }
});

// Import TRI data
router.post('/import-tri', async (req, res) => {
  try {
    console.log('Importing TRI benchmark data...');
    
    const result = await OfficialBenchmarkCollector.importTRIData();
    
    res.json({
      success: true,
      ...result,
      message: `Imported ${result.imported} TRI data points`
    });
  } catch (error) {
    console.error('TRI import error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to import TRI data' 
    });
  }
});

// Get current benchmark status
router.get('/status', async (req, res) => {
  try {
    // Get benchmark coverage
    const coverage = await pool.query(`
      WITH fund_benchmarks AS (
        SELECT DISTINCT benchmark_name, COUNT(*) as fund_count
        FROM funds
        WHERE benchmark_name IS NOT NULL
        GROUP BY benchmark_name
      ),
      available_data AS (
        SELECT DISTINCT index_name, COUNT(*) as data_points
        FROM market_indices
        GROUP BY index_name
      )
      SELECT 
        fb.benchmark_name,
        fb.fund_count,
        COALESCE(ad.data_points, 0) as data_points,
        CASE WHEN ad.data_points > 0 THEN true ELSE false END as has_data
      FROM fund_benchmarks fb
      LEFT JOIN available_data ad ON fb.benchmark_name = ad.index_name
      ORDER BY fb.fund_count DESC
    `);
    
    const totalBenchmarks = coverage.rows.length;
    const benchmarksWithData = coverage.rows.filter(b => b.has_data).length;
    const coveragePercent = totalBenchmarks > 0 ? 
      Math.round((benchmarksWithData / totalBenchmarks) * 100) : 0;
    
    res.json({
      success: true,
      totalBenchmarks,
      benchmarksWithData,
      coveragePercent,
      benchmarks: coverage.rows
    });
  } catch (error) {
    console.error('Status check error:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to get benchmark status' 
    });
  }
});

export default router;