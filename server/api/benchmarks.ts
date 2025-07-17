import { Router } from 'express';
import { BenchmarkDataImporter } from '../services/benchmark-data-importer';
import { pool } from '../db';
import multer from 'multer';
import path from 'path';

const router = Router();
const upload = multer({ dest: 'uploads/' });

// Get all available benchmarks
router.get('/list', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT 
        f.benchmark_name,
        COUNT(DISTINCT f.fund_id) as fund_count,
        EXISTS(
          SELECT 1 FROM market_indices mi 
          WHERE mi.index_name = f.benchmark_name
          LIMIT 1
        ) as has_data,
        (
          SELECT COUNT(*) FROM market_indices mi 
          WHERE mi.index_name = f.benchmark_name
        ) as data_points
      FROM funds f
      WHERE f.benchmark_name IS NOT NULL
      GROUP BY f.benchmark_name
      ORDER BY fund_count DESC
    `);
    
    res.json({
      success: true,
      benchmarks: result.rows,
      total: result.rowCount
    });
  } catch (error) {
    console.error('Error fetching benchmarks:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch benchmarks' });
  }
});

// Get benchmark performance data
router.get('/performance/:benchmarkName', async (req, res) => {
  try {
    const { benchmarkName } = req.params;
    
    // Get latest values and calculate returns
    const returns = await BenchmarkDataImporter.calculateBenchmarkReturns(benchmarkName);
    
    // Get historical data points
    const historicalData = await pool.query(`
      SELECT 
        index_date,
        close_value
      FROM market_indices
      WHERE index_name = $1
      ORDER BY index_date DESC
      LIMIT 252  -- 1 year of trading days
    `, [benchmarkName]);
    
    res.json({
      success: true,
      benchmark: benchmarkName,
      returns,
      historicalData: historicalData.rows
    });
  } catch (error) {
    console.error('Error fetching benchmark performance:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch benchmark performance' });
  }
});

// Get historical data for a benchmark
router.get('/historical-data', async (req, res) => {
  try {
    const { benchmark } = req.query;
    
    if (!benchmark) {
      return res.status(400).json({ success: false, error: 'Benchmark parameter is required' });
    }
    
    const result = await pool.query(`
      SELECT 
        index_date,
        close_value
      FROM market_indices
      WHERE index_name = $1
      ORDER BY index_date ASC
    `, [benchmark]);
    
    res.json({
      success: true,
      benchmark: benchmark,
      data: result.rows
    });
  } catch (error) {
    console.error('Error fetching historical data:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch historical data' });
  }
});

// Get data sources information
router.get('/data-sources', async (req, res) => {
  try {
    const sources = await BenchmarkDataImporter.getDataSources();
    res.json({ success: true, sources });
  } catch (error) {
    console.error('Error fetching data sources:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch data sources' });
  }
});

// Import data from CSV file
router.post('/import/csv', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'No file uploaded' });
    }
    
    const { benchmarkName } = req.body;
    if (!benchmarkName) {
      return res.status(400).json({ success: false, error: 'Benchmark name is required' });
    }
    
    const result = await BenchmarkDataImporter.importFromCSV(req.file.path, benchmarkName);
    
    // Clean up uploaded file
    require('fs').unlinkSync(req.file.path);
    
    res.json({ success: true, ...result });
  } catch (error) {
    console.error('Error importing CSV:', error);
    res.status(500).json({ success: false, error: 'Failed to import CSV data' });
  }
});

// Get missing benchmarks (ones used by funds but not in market_indices)
router.get('/missing', async (req, res) => {
  try {
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
    `);
    
    res.json({
      success: true,
      missingBenchmarks: result.rows,
      total: result.rowCount
    });
  } catch (error) {
    console.error('Error fetching missing benchmarks:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch missing benchmarks' });
  }
});

// Get historical data for a benchmark
router.get('/historical-data/:benchmarkName', async (req, res) => {
  try {
    const { benchmarkName } = req.params;
    
    const result = await pool.query(`
      SELECT 
        index_name,
        index_date,
        close_value
      FROM market_indices
      WHERE index_name = $1
      AND close_value IS NOT NULL
      ORDER BY index_date ASC
    `, [benchmarkName]);
    
    res.json({
      success: true,
      benchmark: benchmarkName,
      data: result.rows,
      count: result.rowCount
    });
  } catch (error) {
    console.error('Error fetching historical data:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch historical data' });
  }
});

export default router;