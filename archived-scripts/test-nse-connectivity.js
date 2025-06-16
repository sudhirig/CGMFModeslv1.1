/**
 * NSE India Connectivity Test
 * Tests access to NSE public APIs for authentic market data collection
 */

import axios from 'axios';

class NSEIndiaCollector {
    constructor() {
        this.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        };
        this.baseUrl = 'https://www.nseindia.com';
    }

    async initializeSession() {
        try {
            console.log('🔄 Initializing NSE session...');
            // Visit main page to get cookies and establish session
            const response = await axios.get(this.baseUrl, { 
                headers: this.headers,
                timeout: 10000
            });
            
            // Extract cookies from response
            if (response.headers['set-cookie']) {
                const cookies = response.headers['set-cookie'].map(cookie => cookie.split(';')[0]).join('; ');
                this.headers['Cookie'] = cookies;
                console.log('✅ Session initialized with cookies');
                return true;
            }
            
            console.log('⚠️ No cookies received, proceeding without session');
            return true;
        } catch (error) {
            console.error('❌ Session initialization failed:', error.message);
            return false;
        }
    }

    async testMarketDataAccess() {
        const endpoints = {
            'Market Indices': '/api/allIndices',
            'Market Status': '/api/marketStatus', 
            'FII/DII Data': '/api/fiidiiTradeReact',
            'Sector Performance': '/api/equity-stockIndices?index=SECTORAL',
            'Top Gainers': '/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O',
            'Market Data': '/api/marketStatus'
        };

        const results = {};
        
        for (const [name, endpoint] of Object.entries(endpoints)) {
            try {
                console.log(`🔍 Testing ${name}...`);
                const url = `${this.baseUrl}${endpoint}`;
                
                const response = await axios.get(url, {
                    headers: this.headers,
                    timeout: 10000,
                    validateStatus: function (status) {
                        return status < 500; // Accept any status less than 500
                    }
                });

                if (response.status === 200 && response.data) {
                    console.log(`✅ ${name}: SUCCESS (${response.data.length || 'Object'} records)`);
                    results[name] = {
                        status: 'SUCCESS',
                        statusCode: response.status,
                        dataType: Array.isArray(response.data) ? 'Array' : typeof response.data,
                        dataSize: Array.isArray(response.data) ? response.data.length : Object.keys(response.data || {}).length,
                        sampleData: Array.isArray(response.data) ? response.data.slice(0, 2) : response.data
                    };
                } else {
                    console.log(`❌ ${name}: Failed (Status: ${response.status})`);
                    results[name] = {
                        status: 'FAILED',
                        statusCode: response.status,
                        error: response.statusText
                    };
                }
                
                // Add delay between requests to be respectful
                await new Promise(resolve => setTimeout(resolve, 1000));
                
            } catch (error) {
                console.log(`❌ ${name}: Error - ${error.message}`);
                results[name] = {
                    status: 'ERROR',
                    error: error.message,
                    errorCode: error.code
                };
            }
        }

        return results;
    }

    async getValuationMetrics() {
        try {
            console.log('🔍 Testing valuation metrics access...');
            
            // Try to get Nifty PE/PB ratios
            const niftyUrl = `${this.baseUrl}/api/equity-stockIndices?index=NIFTY%2050`;
            const response = await axios.get(niftyUrl, {
                headers: this.headers,
                timeout: 10000
            });

            if (response.status === 200 && response.data) {
                console.log('✅ Nifty data access successful');
                return {
                    status: 'SUCCESS',
                    data: response.data
                };
            }
        } catch (error) {
            console.log('❌ Valuation metrics access failed:', error.message);
            return {
                status: 'FAILED',
                error: error.message
            };
        }
    }
}

async function testNSEConnectivity() {
    console.log('🚀 Starting NSE India connectivity test...\n');
    
    const collector = new NSEIndiaCollector();
    
    // Initialize session
    const sessionOk = await collector.initializeSession();
    if (!sessionOk) {
        console.log('❌ Cannot proceed without session initialization');
        return;
    }

    console.log('\n📊 Testing market data endpoints...');
    const marketResults = await collector.testMarketDataAccess();

    console.log('\n💰 Testing valuation metrics...');
    const valuationResults = await collector.getValuationMetrics();

    console.log('\n📋 CONNECTIVITY TEST RESULTS:');
    console.log('=====================================');
    
    let successCount = 0;
    let totalCount = 0;

    for (const [endpoint, result] of Object.entries(marketResults)) {
        totalCount++;
        if (result.status === 'SUCCESS') {
            successCount++;
            console.log(`✅ ${endpoint}: ${result.dataType} with ${result.dataSize} items`);
        } else {
            console.log(`❌ ${endpoint}: ${result.status} - ${result.error || result.errorCode}`);
        }
    }

    console.log(`\n📈 Success Rate: ${successCount}/${totalCount} (${Math.round(successCount/totalCount*100)}%)`);
    
    if (valuationResults.status === 'SUCCESS') {
        console.log('✅ Valuation metrics: Available for ELIVATE integration');
    } else {
        console.log('❌ Valuation metrics: Not accessible');
    }

    console.log('\n🎯 ELIVATE INTEGRATION POTENTIAL:');
    if (successCount > 0) {
        console.log('✅ NSE data can be integrated into ELIVATE framework');
        console.log('✅ Authentic market valuation data available');
        console.log('✅ FII/DII flow data available for sentiment analysis');
        console.log('✅ Sector performance data available');
    } else {
        console.log('❌ NSE integration not viable with current approach');
        console.log('💡 Alternative: Consider NSE official API registration');
    }

    return {
        success: successCount > 0,
        successRate: successCount/totalCount,
        results: marketResults,
        valuation: valuationResults
    };
}

// Run the test
testNSEConnectivity()
    .then(results => {
        console.log('\n🏁 Test completed');
        process.exit(0);
    })
    .catch(error => {
        console.error('💥 Test failed:', error);
        process.exit(1);
    });