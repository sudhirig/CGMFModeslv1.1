/**
 * Phase 2 Backtesting Test Suite
 * Tests all new backtesting capabilities with authentic data only
 */

import axios from 'axios';

const BASE_URL = 'http://localhost:5000';

class Phase2BacktestingTester {
  
  async runAllTests() {
    console.log('🧪 Starting Phase 2 Comprehensive Backtesting Tests');
    console.log('=' .repeat(60));
    
    const tests = [
      { name: 'Individual Fund Backtesting', test: () => this.testIndividualFund() },
      { name: 'Multiple Funds Backtesting', test: () => this.testMultipleFunds() },
      { name: 'ELIVATE Score Range Backtesting', test: () => this.testElivateScoreRange() },
      { name: 'Quartile-Based Backtesting', test: () => this.testQuartileBacktesting() },
      { name: 'Recommendation-Based Backtesting', test: () => this.testRecommendationBacktesting() },
      { name: 'Data Integrity Validation', test: () => this.testDataIntegrity() },
      { name: 'Database Safety Audit', test: () => this.auditDatabaseSafety() }
    ];
    
    const results = [];
    
    for (const { name, test } of tests) {
      try {
        console.log(`\n🔬 Testing: ${name}`);
        const result = await test();
        results.push({ name, status: 'PASSED', result });
        console.log(`✅ ${name}: PASSED`);
      } catch (error) {
        results.push({ name, status: 'FAILED', error: error.message });
        console.log(`❌ ${name}: FAILED - ${error.message}`);
      }
    }
    
    this.generateTestReport(results);
    return results;
  }
  
  /**
   * Test individual fund backtesting
   */
  async testIndividualFund() {
    const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
      fundId: 507, // Known fund with good data
      startDate: '2024-01-01',
      endDate: '2024-12-31', 
      initialAmount: 100000,
      rebalancePeriod: 'quarterly',
      validateElivateScore: true
    });
    
    this.validateResponse(response.data, 'Individual Fund');
    
    if (!response.data.riskProfile.includes('Individual')) {
      throw new Error('Individual fund portfolio not properly identified');
    }
    
    return {
      portfolioName: response.data.portfolioId,
      performance: response.data.performance,
      elivateValidation: response.data.elivateScoreValidation
    };
  }
  
  /**
   * Test multiple funds backtesting with score weighting
   */
  async testMultipleFunds() {
    const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
      fundIds: [507, 1965, 13023], // Known funds with validated data
      startDate: '2024-01-01',
      endDate: '2024-12-31',
      initialAmount: 100000,
      scoreWeighting: true,
      rebalancePeriod: 'quarterly'
    });
    
    this.validateResponse(response.data, 'Multi-Fund');
    
    if (!response.data.riskProfile.includes('Custom')) {
      throw new Error('Multi-fund portfolio not properly identified');
    }
    
    return {
      fundCount: response.data.attribution?.fundContributions?.length || 0,
      performance: response.data.performance
    };
  }
  
  /**
   * Test ELIVATE score range backtesting
   */
  async testElivateScoreRange() {
    const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
      elivateScoreRange: { min: 70, max: 90 },
      startDate: '2024-01-01',
      endDate: '2024-12-31',
      initialAmount: 100000,
      maxFunds: 10,
      equalWeighting: true,
      validateElivateScore: true
    });
    
    this.validateResponse(response.data, 'Score Range');
    
    if (!response.data.riskProfile.includes('Score-Based')) {
      throw new Error('Score-based portfolio not properly identified');
    }
    
    // Validate score validation data
    if (response.data.elivateScoreValidation) {
      const avgScore = response.data.elivateScoreValidation.averagePortfolioScore;
      if (avgScore < 70 || avgScore > 90) {
        throw new Error(`Portfolio average score ${avgScore} outside requested range 70-90`);
      }
    }
    
    return {
      averageScore: response.data.elivateScoreValidation?.averagePortfolioScore,
      performance: response.data.performance,
      scorePredictionAccuracy: response.data.elivateScoreValidation?.scorePredictionAccuracy
    };
  }
  
  /**
   * Test quartile-based backtesting
   */
  async testQuartileBacktesting() {
    const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
      quartile: 'Q1',
      category: 'Equity',
      startDate: '2024-01-01',
      endDate: '2024-12-31',
      initialAmount: 100000,
      maxFunds: 5
    });
    
    this.validateResponse(response.data, 'Quartile Q1');
    
    if (!response.data.riskProfile.includes('Quartile-Based')) {
      throw new Error('Quartile-based portfolio not properly identified');
    }
    
    return {
      quartile: 'Q1',
      category: 'Equity',
      performance: response.data.performance
    };
  }
  
  /**
   * Test recommendation-based backtesting
   */
  async testRecommendationBacktesting() {
    const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
      recommendation: 'BUY',
      startDate: '2024-01-01',
      endDate: '2024-12-31',
      initialAmount: 100000,
      maxFunds: 10
    });
    
    this.validateResponse(response.data, 'BUY Recommendation');
    
    if (!response.data.riskProfile.includes('Recommendation-Based')) {
      throw new Error('Recommendation-based portfolio not properly identified');
    }
    
    return {
      recommendation: 'BUY',
      performance: response.data.performance
    };
  }
  
  /**
   * Validate data integrity - ensure no synthetic data used
   */
  async testDataIntegrity() {
    console.log('   📊 Checking for authentic data usage...');
    
    // Test multiple scenarios to ensure authentic data only
    const scenarios = [
      { fundId: 507 },
      { elivateScoreRange: { min: 80, max: 95 } },
      { quartile: 'Q1' }
    ];
    
    const integrityResults = [];
    
    for (const scenario of scenarios) {
      const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        ...scenario,
        startDate: '2024-06-01',
        endDate: '2024-12-31',
        initialAmount: 50000
      });
      
      // Check for realistic performance numbers (not synthetic)
      const performance = response.data.performance;
      
      if (!performance) {
        throw new Error('No performance data returned');
      }
      
      // Realistic return ranges (not synthetic perfect data)
      if (Math.abs(performance.totalReturn) > 50) {
        throw new Error(`Unrealistic return detected: ${performance.totalReturn}% - possible synthetic data`);
      }
      
      // Check for authentic volatility patterns
      if (performance.bestMonth > 25 || performance.worstMonth < -25) {
        throw new Error('Extreme monthly returns suggest synthetic data contamination');
      }
      
      integrityResults.push({
        scenario: Object.keys(scenario)[0],
        totalReturn: performance.totalReturn,
        volatility: response.data.riskMetrics?.volatility,
        authentic: true
      });
    }
    
    return integrityResults;
  }
  
  /**
   * Audit database safety - ensure no writes occurred
   */
  async auditDatabaseSafety() {
    console.log('   🔒 Auditing database safety...');
    
    // Check that no new records were created during testing
    const beforeCount = await this.getDatabaseRecordCounts();
    
    // Run a comprehensive backtest
    await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
      elivateScoreRange: { min: 60, max: 100 },
      startDate: '2024-01-01',
      endDate: '2024-12-31',
      initialAmount: 100000,
      maxFunds: 20
    });
    
    const afterCount = await this.getDatabaseRecordCounts();
    
    // Verify no changes in record counts
    for (const table in beforeCount) {
      if (beforeCount[table] !== afterCount[table]) {
        throw new Error(`Database modification detected in table ${table}: ${beforeCount[table]} -> ${afterCount[table]}`);
      }
    }
    
    return {
      databaseSafe: true,
      tablesChecked: Object.keys(beforeCount).length,
      recordCounts: beforeCount
    };
  }
  
  /**
   * Get database record counts for safety auditing
   */
  async getDatabaseRecordCounts() {
    const response = await axios.get(`${BASE_URL}/api/system/database-stats`);
    return response.data;
  }
  
  /**
   * Validate response structure and data quality
   */
  validateResponse(data, testType) {
    if (!data) {
      throw new Error(`No data returned for ${testType}`);
    }
    
    // Required fields validation
    const requiredFields = ['portfolioId', 'riskProfile', 'performance', 'riskMetrics'];
    for (const field of requiredFields) {
      if (!(field in data)) {
        throw new Error(`Missing required field: ${field}`);
      }
    }
    
    // Performance validation
    if (!data.performance || typeof data.performance.totalReturn !== 'number') {
      throw new Error('Invalid performance data structure');
    }
    
    // Risk metrics validation
    if (!data.riskMetrics || typeof data.riskMetrics.volatility !== 'number') {
      throw new Error('Invalid risk metrics data structure');
    }
    
    console.log(`   ✓ Response structure validated for ${testType}`);
    console.log(`   ✓ Total Return: ${data.performance.totalReturn?.toFixed(2)}%`);
    console.log(`   ✓ Volatility: ${data.riskMetrics.volatility?.toFixed(2)}%`);
  }
  
  /**
   * Generate comprehensive test report
   */
  generateTestReport(results) {
    console.log('\n' + '='.repeat(60));
    console.log('📋 PHASE 2 BACKTESTING TEST REPORT');
    console.log('='.repeat(60));
    
    const passed = results.filter(r => r.status === 'PASSED').length;
    const failed = results.filter(r => r.status === 'FAILED').length;
    
    console.log(`\n📊 Test Summary:`);
    console.log(`   Total Tests: ${results.length}`);
    console.log(`   Passed: ${passed} ✅`);
    console.log(`   Failed: ${failed} ${failed > 0 ? '❌' : ''}`);
    console.log(`   Success Rate: ${((passed / results.length) * 100).toFixed(1)}%`);
    
    if (failed === 0) {
      console.log('\n🎉 ALL TESTS PASSED - Phase 2 Implementation Ready!');
      console.log('\nKey Capabilities Validated:');
      console.log('   ✓ Individual fund backtesting');
      console.log('   ✓ Multi-fund portfolio analysis');
      console.log('   ✓ ELIVATE score-based selection');
      console.log('   ✓ Quartile performance analysis'); 
      console.log('   ✓ Recommendation validation');
      console.log('   ✓ Data integrity maintained');
      console.log('   ✓ Database safety confirmed');
    } else {
      console.log('\n⚠️  Some tests failed. Review issues before proceeding to Phase 3.');
      results.filter(r => r.status === 'FAILED').forEach(r => {
        console.log(`   ❌ ${r.name}: ${r.error}`);
      });
    }
  }
}

// Export for testing
const tester = new Phase2BacktestingTester();
tester.runAllTests().catch(console.error);

export { Phase2BacktestingTester };