import { Router } from 'express';
import { BenchmarkDataPopulator } from '../services/benchmark-data-populator';
import { pool } from '../db';

const router = Router();

// Fill missing dates for a benchmark
router.post('/fill-missing/:benchmarkName', async (req, res) => {
  try {
    const { benchmarkName } = req.params;
    const result = await BenchmarkDataPopulator.fillMissingDates(benchmarkName);
    res.json(result);
  } catch (error) {
    console.error('Error filling missing dates:', error);
    res.status(500).json({ success: false, error: 'Failed to fill missing dates' });
  }
});

// Extend historical data for a benchmark
router.post('/extend/:benchmarkName', async (req, res) => {
  try {
    const { benchmarkName } = req.params;
    const { years = 5 } = req.body;
    const result = await BenchmarkDataPopulator.extendHistoricalData(benchmarkName, years);
    res.json(result);
  } catch (error) {
    console.error('Error extending data:', error);
    res.status(500).json({ success: false, error: 'Failed to extend data' });
  }
});

// Create correlated benchmark
router.post('/create-correlated', async (req, res) => {
  try {
    const { sourceBenchmark, targetBenchmark, correlation = 0.85 } = req.body;
    const result = await BenchmarkDataPopulator.createCorrelatedBenchmark(
      sourceBenchmark, 
      targetBenchmark, 
      correlation
    );
    res.json(result);
  } catch (error) {
    console.error('Error creating correlated benchmark:', error);
    res.status(500).json({ success: false, error: 'Failed to create correlated benchmark' });
  }
});

// Populate all missing benchmarks
router.post('/populate-all', async (req, res) => {
  try {
    const result = await BenchmarkDataPopulator.populateAllMissingBenchmarks();
    res.json(result);
  } catch (error) {
    console.error('Error populating all benchmarks:', error);
    res.status(500).json({ success: false, error: 'Failed to populate benchmarks' });
  }
});

// Get population status
router.get('/status', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        index_name,
        COUNT(*) as records,
        MIN(index_date) as start_date,
        MAX(index_date) as end_date,
        AVG(close_value) as avg_value
      FROM market_indices
      WHERE index_name LIKE '%TRI%'
      GROUP BY index_name
      ORDER BY records DESC
    `);
    
    res.json({
      success: true,
      benchmarks: result.rows,
      total: result.rowCount
    });
  } catch (error) {
    console.error('Error fetching status:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch status' });
  }
});

export default router;