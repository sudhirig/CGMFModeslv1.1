/**
 * Simple test script to check if AMFI historical data API is accessible
 */
import axios from 'axios';
import * as cheerio from 'cheerio';

// Test with a known scheme code (SBI NIFTY INDEX FUND)
const TEST_SCHEME_CODE = '119827';

async function testAmfiHistoricalAccess() {
  try {
    console.log('Testing AMFI historical data API access...');
    
    // Format dates for the previous month
    const now = new Date();
    const currentMonth = now.getMonth();
    const currentYear = now.getFullYear();
    
    // Calculate previous month
    let prevMonth = currentMonth;
    let prevYear = currentYear;
    if (prevMonth === 0) {
      prevMonth = 12;
      prevYear -= 1;
    }
    
    // Format dates for API request
    const formatDate = (date) => {
      const day = date.getDate().toString().padStart(2, '0');
      const month = (date.getMonth() + 1).toString().padStart(2, '0');
      const year = date.getFullYear();
      return `${day}-${month}-${year}`;
    };
    
    // First day of previous month
    const fromDate = new Date(prevYear, prevMonth - 1, 1);
    // Last day of previous month
    const toDate = new Date(prevYear, prevMonth, 0);
    
    const fromDateStr = formatDate(fromDate);
    const toDateStr = formatDate(toDate);
    
    console.log(`Testing date range: ${fromDateStr} to ${toDateStr}`);
    console.log(`URL: https://www.amfiindia.com/spages/NAVHistoryReport.aspx?sch=${TEST_SCHEME_CODE}&from_date=${fromDateStr}&to_date=${toDateStr}`);
    
    // Make the request with detailed logging
    console.log('Sending request...');
    const response = await axios.get('https://www.amfiindia.com/spages/NAVHistoryReport.aspx', {
      params: {
        sch: TEST_SCHEME_CODE,
        from_date: fromDateStr,
        to_date: toDateStr
      },
      timeout: 30000,
      validateStatus: status => true // Accept any status code to see what's happening
    });
    
    console.log(`Response received: Status ${response.status}`);
    console.log(`Content type: ${response.headers['content-type']}`);
    console.log(`Content length: ${response.data.length} bytes`);
    
    // Analyze the response to see if it contains NAV data
    const $ = cheerio.load(response.data);
    
    // Log the entire HTML for inspection
    console.log('Response HTML (first 1000 characters):');
    console.log(response.data.substring(0, 1000));
    
    // Check for tables in the response
    const tables = $('table');
    console.log(`Found ${tables.length} tables in the response`);
    
    // Try to find NAV data in any table
    let foundNavData = false;
    let navCount = 0;
    
    tables.each((i, table) => {
      console.log(`\nAnalyzing table ${i+1}:`);
      
      // Get headers
      const headers = $(table).find('tr:first-child th, tr:first-child td').map((_, el) => $(el).text().trim()).get();
      console.log(`Headers: ${headers.join(' | ')}`);
      
      // Check if this could be the NAV data table
      if (headers.some(header => header.includes('Date') || header.includes('NAV'))) {
        console.log('This looks like a NAV data table');
        
        // Count the rows
        const rows = $(table).find('tr:not(:first-child)');
        console.log(`Found ${rows.length} data rows`);
        
        // Sample the first few rows
        rows.slice(0, 3).each((rowIndex, row) => {
          const cells = $(row).find('td').map((_, cell) => $(cell).text().trim()).get();
          console.log(`Row ${rowIndex+1}: ${cells.join(' | ')}`);
          
          if (cells.length >= 2) {
            foundNavData = true;
            navCount++;
          }
        });
      }
    });
    
    if (foundNavData) {
      console.log(`\nSuccess! Found NAV data with approximately ${navCount} entries`);
    } else {
      console.log('\nNo NAV data found in the response');
    }
    
  } catch (error) {
    console.error('Error testing AMFI API:', error.message);
    if (error.response) {
      console.log('Response status:', error.response.status);
      console.log('Response headers:', error.response.headers);
    }
  }
}

// Run the test
testAmfiHistoricalAccess().catch(error => {
  console.error('Unhandled error in test:', error);
});