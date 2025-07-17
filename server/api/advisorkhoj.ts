/**
 * AdvisorKhoj API Routes
 * Endpoints for accessing AdvisorKhoj scraped data
 */

import { Router } from 'express';
import { advisorKhojService } from '../services/advisorkhoj-scraper-service';
// Authentication middleware - will be added when auth system is implemented
const requireAuth = (req: any, res: any, next: any) => next();
const requireAdmin = (req: any, res: any, next: any) => next();

const router = Router();

/**
 * Get latest AUM analytics data
 */
router.get('/aum', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 100;
    const data = await advisorKhojService.getLatestAumData(limit);
    
    res.json({
      success: true,
      data,
      count: data.length
    });
  } catch (error) {
    console.error('Error fetching AUM data:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch AUM data' 
    });
  }
});

/**
 * Get AUM grouped by AMC
 */
router.get('/aum/by-amc', async (req, res) => {
  try {
    const data = await advisorKhojService.getAumByAmc();
    
    res.json({
      success: true,
      data,
      count: data.length
    });
  } catch (error) {
    console.error('Error fetching AUM by AMC:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch AUM by AMC' 
    });
  }
});

/**
 * Get portfolio overlap data
 */
router.get('/portfolio-overlap', async (req, res) => {
  try {
    const minOverlap = parseInt(req.query.minOverlap as string) || 50;
    const limit = parseInt(req.query.limit as string) || 50;
    
    const data = await advisorKhojService.getPortfolioOverlaps(minOverlap, limit);
    
    res.json({
      success: true,
      data,
      count: data.length,
      filters: { minOverlap }
    });
  } catch (error) {
    console.error('Error fetching portfolio overlap:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch portfolio overlap data' 
    });
  }
});

/**
 * Search portfolio overlaps by fund name
 */
router.get('/portfolio-overlap/search', async (req, res) => {
  try {
    const fundName = req.query.fundName as string;
    
    if (!fundName) {
      return res.status(400).json({
        success: false,
        error: 'Fund name is required'
      });
    }
    
    const data = await advisorKhojService.searchPortfolioOverlaps(fundName);
    
    res.json({
      success: true,
      data,
      count: data.length,
      searchTerm: fundName
    });
  } catch (error) {
    console.error('Error searching portfolio overlaps:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to search portfolio overlaps' 
    });
  }
});

/**
 * Get top fund managers analytics
 */
router.get('/managers', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 20;
    const data = await advisorKhojService.getTopManagers(limit);
    
    res.json({
      success: true,
      data,
      count: data.length
    });
  } catch (error) {
    console.error('Error fetching manager data:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch manager analytics' 
    });
  }
});

/**
 * Get category performance data
 */
router.get('/category-performance', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50;
    const data = await advisorKhojService.getCategoryPerformance(limit);
    
    res.json({
      success: true,
      data,
      count: data.length
    });
  } catch (error) {
    console.error('Error fetching category performance:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch category performance' 
    });
  }
});

/**
 * Get category performance trends
 */
router.get('/category-performance/trends', async (req, res) => {
  try {
    const categoryName = req.query.category as string;
    const days = parseInt(req.query.days as string) || 30;
    
    if (!categoryName) {
      return res.status(400).json({
        success: false,
        error: 'Category name is required'
      });
    }
    
    const data = await advisorKhojService.getCategoryTrends(categoryName, days);
    
    res.json({
      success: true,
      data,
      count: data.length,
      category: categoryName,
      days
    });
  } catch (error) {
    console.error('Error fetching category trends:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch category trends' 
    });
  }
});

/**
 * Get data freshness status
 */
router.get('/data-status', async (req, res) => {
  try {
    const status = await advisorKhojService.getDataFreshnessStatus();
    
    res.json({
      success: true,
      status,
      lastChecked: new Date()
    });
  } catch (error) {
    console.error('Error checking data status:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to check data status' 
    });
  }
});

/**
 * Trigger manual data scraping (Admin only)
 */
router.post('/scrape', requireAuth, requireAdmin, async (req, res) => {
  try {
    const { categories, skipExisting, dateRange } = req.body;
    
    // Start scraping in background
    advisorKhojService.runScraper({
      categories,
      skipExisting,
      dateRange
    }).then(result => {
      console.log('Scraping completed:', result);
    }).catch(error => {
      console.error('Scraping failed:', error);
    });
    
    // Return immediately
    res.json({
      success: true,
      message: 'Scraping started in background',
      timestamp: new Date()
    });
  } catch (error) {
    console.error('Error starting scraper:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to start scraping' 
    });
  }
});

/**
 * Get scraping history (Admin only)
 */
router.get('/scrape/history', requireAuth, requireAdmin, async (req, res) => {
  try {
    // This would query ETL pipeline runs table
    // For now, return data freshness status
    const status = await advisorKhojService.getDataFreshnessStatus();
    
    res.json({
      success: true,
      history: Object.entries(status).map(([type, info]) => ({
        type,
        lastRun: info.lastUpdate,
        recordsCollected: info.recordCount,
        status: info.isStale ? 'stale' : 'fresh'
      }))
    });
  } catch (error) {
    console.error('Error fetching scrape history:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to fetch scraping history' 
    });
  }
});

export default router;