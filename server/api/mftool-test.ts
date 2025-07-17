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
 * SYNTHETIC NAV DATA GENERATION DISABLED
 * Real NAV data must come from authorized MFAPI or AMFI sources only
 */
function generateMockNavData(startDate?: string, endDate?: string) {
  console.error('Mock NAV data generation is disabled to maintain data integrity');
  console.error('Please use authentic NAV data from MFAPI.in or AMFI sources');
  
  // Return empty array instead of synthetic data
  return [];
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