import { Router } from 'express';
import { BenchmarkDataCollector } from '../services/benchmark-data-collector';
import { pool } from '../db';

const router = Router();

// Get benchmark collection status
router.get('/status', async (req, res) => {
  try {
    // Get total benchmarks used by funds
    const totalResult = await pool.query(`
      SELECT COUNT(DISTINCT benchmark_name) as total
      FROM funds
      WHERE benchmark_name IS NOT NULL
    `);
    
    // Get benchmarks with data
    const withDataResult = await pool.query(`
      SELECT COUNT(DISTINCT f.benchmark_name) as with_data
      FROM funds f
      INNER JOIN (
        SELECT DISTINCT index_name 
        FROM market_indices
      ) mi ON f.benchmark_name = mi.index_name
      WHERE f.benchmark_name IS NOT NULL
    `);
    
    // Get top missing benchmarks
    const missingResult = await pool.query(`
      SELECT 
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
    
    res.json({
      success: true,
      total: totalResult.rows[0].total,
      withData: withDataResult.rows[0].with_data,
      missing: totalResult.rows[0].total - withDataResult.rows[0].with_data,
      topMissing: missingResult.rows
    });
  } catch (error) {
    console.error('Error fetching collection status:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch status' });
  }
});

// Collect data for a specific benchmark
router.post('/collect/:benchmarkName', async (req, res) => {
  try {
    const { benchmarkName } = req.params;
    const result = await BenchmarkDataCollector.fetchFromAlphaVantage('', benchmarkName);
    res.json(result);
  } catch (error) {
    console.error('Error collecting benchmark data:', error);
    res.status(500).json({ success: false, error: 'Failed to collect data' });
  }
});

// Collect commodity indices (Gold, Silver)
router.post('/collect-commodities', async (req, res) => {
  try {
    const result = await BenchmarkDataCollector.fetchCommodityIndices();
    res.json(result);
  } catch (error) {
    console.error('Error collecting commodity data:', error);
    res.status(500).json({ success: false, error: 'Failed to collect commodity data' });
  }
});

// Auto-collect missing benchmarks
router.post('/auto-collect', async (req, res) => {
  try {
    const results = await BenchmarkDataCollector.collectMissingBenchmarks();
    res.json({ success: true, results });
  } catch (error) {
    console.error('Error auto-collecting benchmarks:', error);
    res.status(500).json({ success: false, error: 'Failed to auto-collect' });
  }
});

// Get CRISIL indices info
router.get('/crisil-info', async (req, res) => {
  try {
    const info = await BenchmarkDataCollector.fetchCRISILIndices();
    res.json({ success: true, ...info });
  } catch (error) {
    console.error('Error fetching CRISIL info:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch CRISIL info' });
  }
});

export default router;