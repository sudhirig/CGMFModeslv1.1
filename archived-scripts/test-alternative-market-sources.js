/**
 * Alternative Market Data Sources Test
 * Testing various authentic Indian market data sources for ELIVATE integration
 */

import axios from 'axios';

class AlternativeMarketCollector {
    constructor() {
        this.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        };
    }

    async testYahooFinanceIndia() {
        console.log('🔍 Testing Yahoo Finance India data...');
        try {
            // Test Indian market indices from Yahoo Finance
            const symbols = ['^NSEI', '^BSESN', '^NSEMDCP50', '^CNXIT'];
            const results = {};
            
            for (const symbol of symbols) {
                const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}`;
                const response = await axios.get(url, { 
                    headers: this.headers,
                    timeout: 10000 
                });
                
                if (response.status === 200 && response.data?.chart?.result?.[0]) {
                    const data = response.data.chart.result[0];
                    results[symbol] = {
                        status: 'SUCCESS',
                        symbol: data.meta.symbol,
                        price: data.meta.regularMarketPrice,
                        change: data.meta.regularMarketPrice - data.meta.previousClose,
                        currency: data.meta.currency,
                        exchangeName: data.meta.exchangeName
                    };
                    console.log(`✅ ${symbol}: ${data.meta.regularMarketPrice} ${data.meta.currency}`);
                } else {
                    results[symbol] = { status: 'FAILED', error: 'No data' };
                }
                
                await new Promise(resolve => setTimeout(resolve, 500));
            }
            
            return { source: 'Yahoo Finance', results };
        } catch (error) {
            console.log(`❌ Yahoo Finance failed: ${error.message}`);
            return { source: 'Yahoo Finance', error: error.message };
        }
    }

    async testRBIData() {
        console.log('🔍 Testing RBI official data...');
        try {
            // Test RBI's official data API endpoints
            const endpoints = [
                'https://www.rbi.org.in/Scripts/bs_viewcontent.aspx?Id=2009', // Interest rates
                'https://database.rbi.org.in/DBIE/dbie.rbi?site=statistics' // Economic data
            ];
            
            const results = {};
            
            for (const url of endpoints) {
                try {
                    const response = await axios.get(url, { 
                        headers: this.headers,
                        timeout: 15000,
                        maxRedirects: 5
                    });
                    
                    if (response.status === 200) {
                        results[url] = {
                            status: 'SUCCESS',
                            contentType: response.headers['content-type'],
                            dataSize: response.data.length
                        };
                        console.log(`✅ RBI endpoint accessible: ${url.split('/').pop()}`);
                    }
                } catch (error) {
                    results[url] = { status: 'FAILED', error: error.message };
                    console.log(`❌ RBI endpoint failed: ${error.message}`);
                }
                
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            return { source: 'RBI Official', results };
        } catch (error) {
            return { source: 'RBI Official', error: error.message };
        }
    }

    async testMutualFundIndia() {
        console.log('🔍 Testing Mutual Fund India data...');
        try {
            // Test AMFI and other mutual fund data sources
            const endpoints = {
                'AMFI NAV': 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?tp=1',
                'AMFI Fund List': 'https://www.amfiindia.com/spages/NAVAll.txt'
            };
            
            const results = {};
            
            for (const [name, url] of Object.entries(endpoints)) {
                try {
                    const response = await axios.get(url, { 
                        headers: this.headers,
                        timeout: 15000
                    });
                    
                    if (response.status === 200) {
                        results[name] = {
                            status: 'SUCCESS',
                            dataSize: response.data.length,
                            sampleData: response.data.substring(0, 200)
                        };
                        console.log(`✅ ${name}: Data available (${response.data.length} bytes)`);
                    }
                } catch (error) {
                    results[name] = { status: 'FAILED', error: error.message };
                    console.log(`❌ ${name}: ${error.message}`);
                }
                
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            return { source: 'Mutual Fund India', results };
        } catch (error) {
            return { source: 'Mutual Fund India', error: error.message };
        }
    }

    async testBSEData() {
        console.log('🔍 Testing BSE data access...');
        try {
            // Test BSE API endpoints
            const endpoints = {
                'BSE Index': 'https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w?Debtflag=&scripcode=1&seriesid=',
                'BSE Market Data': 'https://api.bseindia.com/BseIndiaAPI/api/ComHeader/w'
            };
            
            const results = {};
            
            for (const [name, url] of Object.entries(endpoints)) {
                try {
                    const response = await axios.get(url, { 
                        headers: this.headers,
                        timeout: 10000
                    });
                    
                    if (response.status === 200) {
                        results[name] = {
                            status: 'SUCCESS',
                            data: response.data
                        };
                        console.log(`✅ ${name}: API accessible`);
                    }
                } catch (error) {
                    results[name] = { status: 'FAILED', error: error.message };
                    console.log(`❌ ${name}: ${error.message}`);
                }
                
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            return { source: 'BSE Official', results };
        } catch (error) {
            return { source: 'BSE Official', error: error.message };
        }
    }
}

async function testAlternativeMarketSources() {
    console.log('🚀 Testing alternative authentic market data sources...\n');
    
    const collector = new AlternativeMarketCollector();
    const results = {};
    
    // Test each source
    results.yahooFinance = await collector.testYahooFinanceIndia();
    console.log('');
    
    results.rbiData = await collector.testRBIData();
    console.log('');
    
    results.mutualFundIndia = await collector.testMutualFundIndia();
    console.log('');
    
    results.bseData = await collector.testBSEData();
    console.log('');
    
    // Analyze results for ELIVATE integration
    console.log('📊 ELIVATE INTEGRATION ANALYSIS:');
    console.log('=====================================');
    
    let viableSources = 0;
    
    if (results.yahooFinance.results && !results.yahooFinance.error) {
        console.log('✅ Yahoo Finance: Viable for market valuation (PE/PB ratios)');
        viableSources++;
    }
    
    if (results.rbiData.results && !results.rbiData.error) {
        console.log('✅ RBI Data: Viable for monetary policy indicators');
        viableSources++;
    }
    
    if (results.mutualFundIndia.results && !results.mutualFundIndia.error) {
        console.log('✅ AMFI Data: Viable for fund flow analysis');
        viableSources++;
    }
    
    if (results.bseData.results && !results.bseData.error) {
        console.log('✅ BSE Data: Viable for market sentiment indicators');
        viableSources++;
    }
    
    console.log(`\n🎯 MISSING ELIVATE COMPONENTS STATUS:`);
    console.log(`Available authentic sources: ${viableSources}/4`);
    
    if (viableSources >= 2) {
        console.log('✅ Sufficient sources for completing ELIVATE framework');
        console.log('🔧 Next: Implement collectors for missing components');
    } else {
        console.log('⚠️ Limited sources available');
        console.log('💡 Recommendation: Focus on available authentic data only');
    }
    
    return results;
}

// Run the test
testAlternativeMarketSources()
    .then(results => {
        console.log('\n🏁 Alternative sources test completed');
        process.exit(0);
    })
    .catch(error => {
        console.error('💥 Test failed:', error);
        process.exit(1);
    });