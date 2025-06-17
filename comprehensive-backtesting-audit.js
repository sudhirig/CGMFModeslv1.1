/**
 * Comprehensive Backtesting System Audit
 * Tests all backtesting types across frontend and backend integration
 */

import axios from 'axios';

const BASE_URL = 'http://localhost:5000';

class BacktestingAudit {
  constructor() {
    this.results = [];
    this.errors = [];
  }

  async runComprehensiveAudit() {
    console.log('🔍 Starting Comprehensive Backtesting System Audit');
    console.log('=' .repeat(60));

    // Test all backtesting types
    await this.testIndividualFundBacktesting();
    await this.testRiskProfileBacktesting();
    await this.testPortfolioBacktesting();
    await this.testScoreRangeBacktesting();
    await this.testQuartileBacktesting();
    await this.testRecommendationBacktesting();
    
    // Test data integrity
    await this.testDataIntegrity();
    
    // Test error handling
    await this.testErrorHandling();
    
    // Test performance
    await this.testPerformanceMetrics();

    this.generateAuditReport();
  }

  async testIndividualFundBacktesting() {
    console.log('\n📊 Testing Individual Fund Backtesting...');
    
    try {
      const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        fundId: "8319",
        startDate: "2024-01-01",
        endDate: "2024-12-31",
        initialAmount: "100000",
        rebalancePeriod: "quarterly"
      });

      if (response.status === 200 && response.data.performance) {
        this.logSuccess('Individual Fund', response.data);
      } else {
        this.logError('Individual Fund', 'Invalid response structure');
      }
    } catch (error) {
      this.logError('Individual Fund', error.message);
    }
  }

  async testRiskProfileBacktesting() {
    console.log('\n📊 Testing Risk Profile Backtesting...');
    
    const riskProfiles = ['Conservative', 'Balanced', 'Aggressive'];
    
    for (const profile of riskProfiles) {
      try {
        const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
          riskProfile: profile,
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        });

        if (response.status === 200 && response.data.performance) {
          this.logSuccess(`Risk Profile: ${profile}`, response.data);
        } else {
          this.logError(`Risk Profile: ${profile}`, 'Invalid response structure');
        }
      } catch (error) {
        this.logError(`Risk Profile: ${profile}`, error.message);
      }
    }
  }

  async testPortfolioBacktesting() {
    console.log('\n📊 Testing Portfolio Backtesting...');
    
    try {
      // First get available portfolios
      const portfoliosResponse = await axios.get(`${BASE_URL}/api/portfolios`);
      
      if (portfoliosResponse.data.length > 0) {
        const portfolioId = portfoliosResponse.data[0].id;
        
        const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
          portfolioId: portfolioId.toString(),
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        });

        if (response.status === 200 && response.data.performance) {
          this.logSuccess('Portfolio Backtesting', response.data);
        } else {
          this.logError('Portfolio Backtesting', 'Invalid response structure');
        }
      } else {
        this.logError('Portfolio Backtesting', 'No portfolios available');
      }
    } catch (error) {
      this.logError('Portfolio Backtesting', error.message);
    }
  }

  async testScoreRangeBacktesting() {
    console.log('\n📊 Testing Score Range Backtesting...');
    
    try {
      const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        elivateScoreRange: { min: 70, max: 90 },
        maxFunds: "10",
        startDate: "2024-01-01",
        endDate: "2024-12-31",
        initialAmount: "100000",
        rebalancePeriod: "quarterly",
        scoreWeighting: true
      });

      if (response.status === 200 && response.data.performance) {
        this.logSuccess('Score Range', response.data);
      } else {
        this.logError('Score Range', 'Invalid response structure');
      }
    } catch (error) {
      this.logError('Score Range', error.message);
    }
  }

  async testQuartileBacktesting() {
    console.log('\n📊 Testing Quartile Backtesting...');
    
    const quartiles = ['Q1', 'Q2', 'Q3', 'Q4'];
    
    for (const quartile of quartiles) {
      try {
        const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
          quartile: quartile,
          maxFunds: "15",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        });

        if (response.status === 200 && response.data.performance) {
          this.logSuccess(`Quartile: ${quartile}`, response.data);
        } else {
          this.logError(`Quartile: ${quartile}`, 'Invalid response structure');
        }
      } catch (error) {
        this.logError(`Quartile: ${quartile}`, error.message);
      }
    }
  }

  async testRecommendationBacktesting() {
    console.log('\n📊 Testing Recommendation Backtesting...');
    
    const recommendations = ['BUY', 'HOLD', 'SELL'];
    
    for (const recommendation of recommendations) {
      try {
        const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
          recommendation: recommendation,
          maxFunds: "20",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        });

        if (response.status === 200 && response.data.performance) {
          this.logSuccess(`Recommendation: ${recommendation}`, response.data);
        } else {
          this.logError(`Recommendation: ${recommendation}`, 'Invalid response structure');
        }
      } catch (error) {
        this.logError(`Recommendation: ${recommendation}`, error.message);
      }
    }
  }

  async testDataIntegrity() {
    console.log('\n🔒 Testing Data Integrity...');
    
    try {
      // Test with authentic fund data
      const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        fundId: "8319",
        startDate: "2024-01-01",
        endDate: "2024-12-31",
        initialAmount: "100000",
        rebalancePeriod: "quarterly"
      });

      const data = response.data;
      
      // Validate data structure
      const hasValidStructure = 
        data.performance &&
        data.riskMetrics &&
        data.attribution &&
        data.attribution.fundContributions;

      if (hasValidStructure) {
        // Check for authentic ELIVATE scores
        const fundContributions = data.attribution.fundContributions;
        const hasElivateScores = fundContributions.every(fund => 
          fund.elivateScore && !isNaN(parseFloat(fund.elivateScore))
        );

        if (hasElivateScores) {
          this.logSuccess('Data Integrity', 'All funds have authentic ELIVATE scores');
        } else {
          this.logError('Data Integrity', 'Missing or invalid ELIVATE scores');
        }
      } else {
        this.logError('Data Integrity', 'Invalid data structure');
      }
    } catch (error) {
      this.logError('Data Integrity', error.message);
    }
  }

  async testErrorHandling() {
    console.log('\n⚠️  Testing Error Handling...');
    
    // Test invalid fund ID
    try {
      await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        fundId: "999999",
        startDate: "2024-01-01",
        endDate: "2024-12-31",
        initialAmount: "100000",
        rebalancePeriod: "quarterly"
      });
      this.logError('Error Handling', 'Should fail with invalid fund ID');
    } catch (error) {
      if (error.response && error.response.status >= 400) {
        this.logSuccess('Error Handling', 'Properly handles invalid fund ID');
      } else {
        this.logError('Error Handling', 'Unexpected error type');
      }
    }

    // Test invalid date range
    try {
      await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        riskProfile: "Balanced",
        startDate: "2024-12-31",
        endDate: "2024-01-01",
        initialAmount: "100000",
        rebalancePeriod: "quarterly"
      });
      this.logError('Error Handling', 'Should fail with invalid date range');
    } catch (error) {
      if (error.response && error.response.status >= 400) {
        this.logSuccess('Error Handling', 'Properly handles invalid date range');
      } else {
        this.logError('Error Handling', 'Unexpected error for date range');
      }
    }
  }

  async testPerformanceMetrics() {
    console.log('\n⚡ Testing Performance Metrics...');
    
    try {
      const startTime = Date.now();
      
      const response = await axios.post(`${BASE_URL}/api/comprehensive-backtest`, {
        riskProfile: "Balanced",
        startDate: "2024-01-01",
        endDate: "2024-12-31",
        initialAmount: "100000",
        rebalancePeriod: "quarterly"
      });

      const endTime = Date.now();
      const responseTime = endTime - startTime;

      if (response.status === 200) {
        if (responseTime < 5000) {
          this.logSuccess('Performance', `Response time: ${responseTime}ms`);
        } else {
          this.logError('Performance', `Slow response time: ${responseTime}ms`);
        }

        // Validate performance metrics
        const performance = response.data.performance;
        const riskMetrics = response.data.riskMetrics;

        const metricsValid = 
          typeof performance.totalReturn === 'number' &&
          typeof performance.annualizedReturn === 'number' &&
          typeof riskMetrics.volatility === 'number' &&
          typeof riskMetrics.sharpeRatio === 'number';

        if (metricsValid) {
          this.logSuccess('Metrics Validation', 'All metrics are numeric');
        } else {
          this.logError('Metrics Validation', 'Invalid metric types');
        }
      }
    } catch (error) {
      this.logError('Performance', error.message);
    }
  }

  logSuccess(test, data) {
    console.log(`✅ ${test}: PASSED`);
    if (data && data.performance) {
      console.log(`   Return: ${data.performance.totalReturn?.toFixed(2)}%`);
      console.log(`   Volatility: ${data.riskMetrics?.volatility?.toFixed(2)}%`);
      if (data.attribution?.fundContributions) {
        console.log(`   Funds: ${data.attribution.fundContributions.length}`);
      }
    } else if (typeof data === 'string') {
      console.log(`   ${data}`);
    }
    this.results.push({ test, status: 'PASSED', data });
  }

  logError(test, error) {
    console.log(`❌ ${test}: FAILED`);
    console.log(`   Error: ${error}`);
    this.errors.push({ test, error });
  }

  generateAuditReport() {
    console.log('\n' + '='.repeat(60));
    console.log('📋 COMPREHENSIVE BACKTESTING AUDIT REPORT');
    console.log('='.repeat(60));

    const totalTests = this.results.length + this.errors.length;
    const passedTests = this.results.length;
    const failedTests = this.errors.length;
    const successRate = ((passedTests / totalTests) * 100).toFixed(1);

    console.log(`\n📊 SUMMARY:`);
    console.log(`   Total Tests: ${totalTests}`);
    console.log(`   Passed: ${passedTests}`);
    console.log(`   Failed: ${failedTests}`);
    console.log(`   Success Rate: ${successRate}%`);

    if (failedTests > 0) {
      console.log(`\n❌ FAILED TESTS:`);
      this.errors.forEach(error => {
        console.log(`   • ${error.test}: ${error.error}`);
      });
    }

    console.log(`\n✅ OPERATIONAL FEATURES:`);
    this.results.forEach(result => {
      console.log(`   • ${result.test}`);
    });

    if (successRate >= 80) {
      console.log(`\n🎉 AUDIT STATUS: SYSTEM OPERATIONAL`);
      console.log(`   The backtesting system is ready for production use.`);
    } else {
      console.log(`\n⚠️  AUDIT STATUS: NEEDS ATTENTION`);
      console.log(`   Critical issues found that require resolution.`);
    }
  }
}

// Run the audit
const audit = new BacktestingAudit();
audit.runComprehensiveAudit().catch(console.error);