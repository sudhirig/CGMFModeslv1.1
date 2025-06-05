import express from 'express';
import axios from 'axios';

const router = express.Router();

/**
 * Test endpoint for MFTool functionality
 * This simulates what a MFTool Python library might return
 */
router.post('/test', async (req, res) => {
  try {
    const { schemeCode, startDate, endDate } = req.body;
    
    if (!schemeCode) {
      return res.status(400).json({
        success: false,
        message: 'Scheme code is required'
      });
    }
    
    // For now, we'll simulate the MFTool response structure
    // In a real implementation, this would call a Python script or use a Python library bridge
    const mockResponse = {
      success: true,
      message: 'MFTool test completed successfully (simulated)',
      data: {
        meta: {
          scheme_name: `Test Scheme ${schemeCode}`,
          scheme_code: schemeCode,
          fund_house: 'Sample Fund House',
          scheme_type: 'Equity',
          scheme_category: 'Large Cap'
        },
        data: generateMockNavData(startDate, endDate)
      },
      statistics: {
        totalRecords: 100,
        latestNAV: '45.67',
        dateRange: {
          start: startDate || '01-01-2024',
          end: endDate || '31-12-2024'
        }
      },
      note: 'This is a simulated response. Real implementation would require MFTool Python library integration.'
    };
    
    res.json(mockResponse);
    
  } catch (error: any) {
    console.error('Error in MFTool test:', error);
    
    res.status(500).json({
      success: false,
      message: 'MFTool test failed: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

/**
 * Generate mock NAV data for testing purposes
 */
function generateMockNavData(startDate?: string, endDate?: string) {
  const mockData = [];
  const start = startDate ? parseDate(startDate) : new Date('2024-01-01');
  const end = endDate ? parseDate(endDate) : new Date('2024-12-31');
  
  let currentDate = new Date(start);
  let currentNav = 45.0;
  
  while (currentDate <= end && mockData.length < 10) {
    // Generate realistic NAV fluctuation
    const change = (Math.random() - 0.5) * 2; // Random change between -1 and +1
    currentNav = Math.max(10, currentNav + change);
    
    mockData.push({
      date: formatDate(currentDate),
      nav: currentNav.toFixed(2)
    });
    
    // Move to next business day (skip weekends)
    do {
      currentDate.setDate(currentDate.getDate() + 1);
    } while (currentDate.getDay() === 0 || currentDate.getDay() === 6);
  }
  
  return mockData.reverse(); // Latest first
}

/**
 * Parse date from DD-MM-YYYY format
 */
function parseDate(dateStr: string): Date {
  const [day, month, year] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day);
}

/**
 * Format date to DD-MM-YYYY
 */
function formatDate(date: Date): string {
  const day = date.getDate().toString().padStart(2, '0');
  const month = (date.getMonth() + 1).toString().padStart(2, '0');
  const year = date.getFullYear();
  return `${day}-${month}-${year}`;
}

export default router;