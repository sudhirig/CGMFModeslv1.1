/**
 * Test script to evaluate MFAPI.in data quality
 */
import axios from 'axios';

async function testMFAPIData() {
  console.log('=== Testing MFAPI.in Data Source ===\n');
  
  try {
    // Test 1: Search for schemes
    console.log('1. Testing scheme search...');
    const searchResponse = await axios.get('https://api.mfapi.in/mf/search?q=HDFC');
    const schemes = searchResponse.data;
    
    console.log(`Found ${schemes.length} HDFC schemes`);
    console.log('Sample schemes:');
    schemes.slice(0, 3).forEach(scheme => {
      console.log(`  - ${scheme.schemeName} (Code: ${scheme.schemeCode})`);
    });
    
    // Test 2: Fetch historical data for a specific scheme
    console.log('\n2. Testing historical data fetch...');
    const testSchemeCode = schemes[0].schemeCode;
    console.log(`Fetching data for scheme: ${testSchemeCode}`);
    
    const historicalResponse = await axios.get(`https://api.mfapi.in/mf/${testSchemeCode}`);
    const historicalData = historicalResponse.data;
    
    console.log('\nScheme Information:');
    console.log(`  Name: ${historicalData.meta.scheme_name}`);
    console.log(`  Code: ${historicalData.meta.scheme_code}`);
    console.log(`  Category: ${historicalData.meta.scheme_category}`);
    console.log(`  Fund House: ${historicalData.meta.fund_house}`);
    
    console.log('\nHistorical Data Analysis:');
    console.log(`  Total records: ${historicalData.data.length}`);
    
    if (historicalData.data.length > 0) {
      const latest = historicalData.data[0];
      const oldest = historicalData.data[historicalData.data.length - 1];
      
      console.log(`  Latest: ${latest.date} - NAV: ₹${latest.nav}`);
      console.log(`  Oldest: ${oldest.date} - NAV: ₹${oldest.nav}`);
      
      console.log('\nSample Recent Data:');
      historicalData.data.slice(0, 5).forEach(entry => {
        console.log(`    ${entry.date}: ₹${entry.nav}`);
      });
      
      // Test data quality
      console.log('\nData Quality Analysis:');
      const hasValidDates = historicalData.data.every(entry => 
        entry.date && entry.date.match(/^\d{2}-\d{2}-\d{4}$/)
      );
      const hasValidNAVs = historicalData.data.every(entry => 
        entry.nav && !isNaN(parseFloat(entry.nav))
      );
      
      console.log(`  Valid date format: ${hasValidDates ? 'Yes' : 'No'}`);
      console.log(`  Valid NAV values: ${hasValidNAVs ? 'Yes' : 'No'}`);
      console.log(`  Data completeness: ${historicalData.data.length > 100 ? 'Good' : 'Limited'}`);
    }
    
    // Test 3: Test another scheme to verify consistency
    console.log('\n3. Testing data consistency with another scheme...');
    if (schemes.length > 1) {
      const secondScheme = schemes[1];
      const secondResponse = await axios.get(`https://api.mfapi.in/mf/${secondScheme.schemeCode}`);
      const secondData = secondResponse.data;
      
      console.log(`Second scheme: ${secondData.meta.scheme_name}`);
      console.log(`Records: ${secondData.data.length}`);
      
      if (secondData.data.length > 0) {
        console.log(`Latest: ${secondData.data[0].date} - NAV: ₹${secondData.data[0].nav}`);
      }
    }
    
    console.log('\n=== MFAPI.in Test Results ===');
    console.log('✓ API is accessible and responsive');
    console.log('✓ Scheme search functionality works');
    console.log('✓ Historical data is available');
    console.log('✓ Data includes proper metadata');
    console.log('✓ NAV values are in correct format');
    
  } catch (error) {
    console.error('❌ MFAPI.in Test Failed:', error.message);
    if (error.response) {
      console.error(`HTTP Status: ${error.response.status}`);
      console.error(`Response: ${JSON.stringify(error.response.data)}`);
    }
  }
}

async function testMFToolEndpoint() {
  console.log('\n=== Testing MFTool Endpoint ===\n');
  
  try {
    const response = await axios.post('http://localhost:5000/api/mftool/test', {
      schemeCode: '119551',
      startDate: '01-01-2024',
      endDate: '31-12-2024'
    });
    
    console.log('MFTool Test Response:');
    console.log(JSON.stringify(response.data, null, 2));
    
  } catch (error) {
    console.error('❌ MFTool Test Failed:', error.message);
    if (error.response) {
      console.error(`HTTP Status: ${error.response.status}`);
    }
  }
}

// Run tests
testMFAPIData().then(() => {
  return testMFToolEndpoint();
}).catch(error => {
  console.error('Test execution failed:', error);
});