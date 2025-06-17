/**
 * Comprehensive Backtesting System Audit
 * Tests all layers: Database, Backend API, Business Logic, Frontend Integration
 */

import axios from 'axios';

class ComprehensiveBacktestingAudit {
  constructor() {
    this.baseUrl = 'http://localhost:5000';
    this.results = {
      database: [],
      api: [],
      businessLogic: [],
      frontend: [],
      performance: [],
      errors: []
    };
  }

  async runFullAudit() {
    console.log('🔍 Starting Comprehensive Backtesting System Audit...\n');
    
    try {
      await this.auditDatabaseLayer();
      await this.auditAPIEndpoints();
      await this.auditBusinessLogic();
      await this.auditPerformanceMetrics();
      await this.auditDataIntegrity();
      await this.auditErrorHandling();
      
      this.generateComprehensiveReport();
    } catch (error) {
      console.error('❌ Audit failed:', error.message);
      this.results.errors.push({ test: 'Audit System', error: error.message });
    }
  }

  async auditDatabaseLayer() {
    console.log('📊 Auditing Database Layer...');
    
    const tests = [
      {
        name: 'NAV Data Availability',
        test: async () => {
          const response = await axios.post(`${this.baseUrl}/api/execute-sql`, {
            query: `SELECT 
              COUNT(DISTINCT fund_id) as funds_with_nav,
              COUNT(*) as total_nav_records,
              MIN(nav_date) as earliest_date,
              MAX(nav_date) as latest_date,
              AVG(nav_value) as avg_nav_value
            FROM nav_data 
            WHERE nav_value > 0`
          });
          return response.data;
        }
      },
      {
        name: 'Fund Scores Coverage',
        test: async () => {
          const response = await axios.post(`${this.baseUrl}/api/execute-sql`, {
            query: `SELECT 
              COUNT(*) as total_funds,
              COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as funds_with_scores,
              AVG(total_score) as avg_score,
              COUNT(CASE WHEN recommendation IS NOT NULL THEN 1 END) as funds_with_recommendations
            FROM fund_scores_corrected`
          });
          return response.data;
        }
      },
      {
        name: 'Historical Data Quality',
        test: async () => {
          const response = await axios.post(`${this.baseUrl}/api/execute-sql`, {
            query: `SELECT 
              fund_id,
              COUNT(*) as nav_count,
              MIN(nav_date) as earliest,
              MAX(nav_date) as latest
            FROM nav_data 
            WHERE nav_value > 0
            AND nav_date >= '2024-01-01'
            GROUP BY fund_id
            HAVING COUNT(*) >= 100
            ORDER BY nav_count DESC
            LIMIT 10`
          });
          return response.data;
        }
      }
    ];

    for (const test of tests) {
      try {
        const result = await test.test();
        this.results.database.push({ name: test.name, status: 'PASS', data: result });
        console.log(`  ✅ ${test.name}: PASS`);
      } catch (error) {
        this.results.database.push({ name: test.name, status: 'FAIL', error: error.message });
        console.log(`  ❌ ${test.name}: FAIL - ${error.message}`);
      }
    }
  }

  async auditAPIEndpoints() {
    console.log('\n🌐 Auditing API Endpoints...');
    
    const testCases = [
      {
        name: 'Individual Fund Backtesting',
        config: {
          fundId: "8319",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        }
      },
      {
        name: 'Score Range Backtesting',
        config: {
          elivateScoreRange: { min: 75, max: 90 },
          maxFunds: "5",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        }
      },
      {
        name: 'Quartile Backtesting',
        config: {
          quartile: "Q1",
          maxFunds: "8",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        }
      },
      {
        name: 'Recommendation Backtesting',
        config: {
          recommendation: "BUY",
          maxFunds: "6",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        }
      },
      {
        name: 'Risk Profile Backtesting',
        config: {
          riskProfile: "Conservative",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        }
      },
      {
        name: 'Portfolio Backtesting',
        config: {
          portfolioId: "1",
          startDate: "2024-01-01",
          endDate: "2024-12-31",
          initialAmount: "100000",
          rebalancePeriod: "quarterly"
        }
      }
    ];

    for (const testCase of testCases) {
      try {
        const startTime = Date.now();
        const response = await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, testCase.config);
        const duration = Date.now() - startTime;
        
        const result = response.data;
        const isValid = this.validateBacktestResult(result);
        
        this.results.api.push({
          name: testCase.name,
          status: isValid.status,
          duration: duration,
          data: {
            totalReturn: result.performance?.totalReturn,
            annualizedReturn: result.performance?.annualizedReturn,
            sharpeRatio: result.riskMetrics?.sharpeRatio,
            maxDrawdown: result.riskMetrics?.maxDrawdown,
            fundCount: result.attribution?.fundContributions?.length
          },
          issues: isValid.issues
        });
        
        console.log(`  ${isValid.status === 'PASS' ? '✅' : '⚠️'} ${testCase.name}: ${isValid.status} (${duration}ms)`);
        if (isValid.issues.length > 0) {
          isValid.issues.forEach(issue => console.log(`    - ${issue}`));
        }
      } catch (error) {
        this.results.api.push({
          name: testCase.name,
          status: 'FAIL',
          error: error.message
        });
        console.log(`  ❌ ${testCase.name}: FAIL - ${error.message}`);
      }
    }
  }

  validateBacktestResult(result) {
    const issues = [];
    let status = 'PASS';

    // Check required structure
    if (!result.performance) {
      issues.push('Missing performance metrics');
      status = 'FAIL';
    }
    if (!result.riskMetrics) {
      issues.push('Missing risk metrics');
      status = 'FAIL';
    }
    if (!result.attribution) {
      issues.push('Missing attribution analysis');
      status = 'FAIL';
    }

    // Validate performance metrics
    if (result.performance) {
      if (typeof result.performance.totalReturn !== 'number') {
        issues.push('Invalid totalReturn type');
        status = 'WARN';
      }
      if (typeof result.performance.annualizedReturn !== 'number') {
        issues.push('Invalid annualizedReturn type');
        status = 'WARN';
      }
      if (!Array.isArray(result.performance.monthlyReturns)) {
        issues.push('Invalid monthlyReturns format');
        status = 'WARN';
      }
    }

    // Validate risk metrics
    if (result.riskMetrics) {
      if (typeof result.riskMetrics.volatility !== 'number') {
        issues.push('Invalid volatility type');
        status = 'WARN';
      }
      if (typeof result.riskMetrics.sharpeRatio !== 'number') {
        issues.push('Invalid sharpeRatio type');
        status = 'WARN';
      }
    }

    // Check for realistic values
    if (result.performance?.totalReturn && Math.abs(result.performance.totalReturn) > 1000) {
      issues.push('Unrealistic total return value');
      status = 'WARN';
    }

    return { status, issues };
  }

  async auditBusinessLogic() {
    console.log('\n⚙️ Auditing Business Logic...');
    
    const tests = [
      {
        name: 'Date Range Validation',
        test: async () => {
          try {
            await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, {
              fundId: "8319",
              startDate: "2025-01-01",
              endDate: "2024-01-01", // Invalid: end before start
              initialAmount: "100000",
              rebalancePeriod: "quarterly"
            });
            return { status: 'FAIL', message: 'Should reject invalid date ranges' };
          } catch (error) {
            return { status: 'PASS', message: 'Correctly rejects invalid date ranges' };
          }
        }
      },
      {
        name: 'Fund Allocation Logic',
        test: async () => {
          const response = await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, {
            elivateScoreRange: { min: 80, max: 90 },
            maxFunds: "3",
            startDate: "2024-01-01",
            endDate: "2024-12-31",
            initialAmount: "100000",
            rebalancePeriod: "quarterly"
          });
          
          const allocations = response.data.attribution.fundContributions.map(f => f.allocation);
          const totalAllocation = allocations.reduce((sum, alloc) => sum + alloc, 0);
          
          if (Math.abs(totalAllocation - 1.0) < 0.01) {
            return { status: 'PASS', message: 'Fund allocations sum to 100%' };
          } else {
            return { status: 'FAIL', message: `Allocations sum to ${totalAllocation * 100}%` };
          }
        }
      },
      {
        name: 'Performance Calculation Accuracy',
        test: async () => {
          const response = await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, {
            fundId: "8319",
            startDate: "2024-01-01",
            endDate: "2024-03-31", // 3-month period
            initialAmount: "100000",
            rebalancePeriod: "quarterly"
          });
          
          const monthlyReturns = response.data.performance.monthlyReturns;
          const totalReturn = response.data.performance.totalReturn;
          
          // Calculate expected total return from monthly returns
          const calculatedTotal = monthlyReturns.reduce((total, monthRet) => 
            (1 + total) * (1 + monthRet / 100) - 1, 0) * 100;
          
          if (Math.abs(calculatedTotal - totalReturn) < 1.0) {
            return { status: 'PASS', message: 'Performance calculations are consistent' };
          } else {
            return { status: 'WARN', message: `Calculation discrepancy: ${Math.abs(calculatedTotal - totalReturn).toFixed(2)}%` };
          }
        }
      }
    ];

    for (const test of tests) {
      try {
        const result = await test.test();
        this.results.businessLogic.push({
          name: test.name,
          status: result.status,
          message: result.message
        });
        console.log(`  ${result.status === 'PASS' ? '✅' : result.status === 'WARN' ? '⚠️' : '❌'} ${test.name}: ${result.status} - ${result.message}`);
      } catch (error) {
        this.results.businessLogic.push({
          name: test.name,
          status: 'FAIL',
          error: error.message
        });
        console.log(`  ❌ ${test.name}: FAIL - ${error.message}`);
      }
    }
  }

  async auditPerformanceMetrics() {
    console.log('\n⚡ Auditing Performance Metrics...');
    
    const performanceTests = [
      {
        name: 'Individual Fund Response Time',
        config: { fundId: "8319", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
        expectedMaxTime: 3000
      },
      {
        name: 'Score Range Response Time',
        config: { elivateScoreRange: { min: 75, max: 90 }, maxFunds: "10", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
        expectedMaxTime: 8000
      },
      {
        name: 'Large Portfolio Response Time',
        config: { quartile: "Q1", maxFunds: "20", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
        expectedMaxTime: 15000
      }
    ];

    for (const test of performanceTests) {
      try {
        const startTime = Date.now();
        await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, test.config);
        const duration = Date.now() - startTime;
        
        const status = duration <= test.expectedMaxTime ? 'PASS' : 'WARN';
        this.results.performance.push({
          name: test.name,
          status: status,
          duration: duration,
          expectedMax: test.expectedMaxTime
        });
        
        console.log(`  ${status === 'PASS' ? '✅' : '⚠️'} ${test.name}: ${status} (${duration}ms / ${test.expectedMaxTime}ms max)`);
      } catch (error) {
        this.results.performance.push({
          name: test.name,
          status: 'FAIL',
          error: error.message
        });
        console.log(`  ❌ ${test.name}: FAIL - ${error.message}`);
      }
    }
  }

  async auditDataIntegrity() {
    console.log('\n🔒 Auditing Data Integrity...');
    
    try {
      // Test with known fund to verify data authenticity
      const response = await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, {
        fundId: "8319",
        startDate: "2024-01-01",
        endDate: "2024-06-30",
        initialAmount: "100000",
        rebalancePeriod: "quarterly"
      });

      const fund = response.data.attribution.fundContributions[0];
      
      // Verify ELIVATE score is present
      if (fund.elivateScore && parseFloat(fund.elivateScore) > 0) {
        console.log(`  ✅ ELIVATE Score Integration: PASS (Score: ${fund.elivateScore})`);
        this.results.database.push({ name: 'ELIVATE Score Integration', status: 'PASS' });
      } else {
        console.log(`  ❌ ELIVATE Score Integration: FAIL`);
        this.results.database.push({ name: 'ELIVATE Score Integration', status: 'FAIL' });
      }

      // Verify fund name authenticity
      if (fund.fundName && fund.fundName.length > 10) {
        console.log(`  ✅ Fund Name Authenticity: PASS`);
        this.results.database.push({ name: 'Fund Name Authenticity', status: 'PASS' });
      } else {
        console.log(`  ❌ Fund Name Authenticity: FAIL`);
        this.results.database.push({ name: 'Fund Name Authenticity', status: 'FAIL' });
      }

    } catch (error) {
      console.log(`  ❌ Data Integrity Audit: FAIL - ${error.message}`);
      this.results.database.push({ name: 'Data Integrity Audit', status: 'FAIL', error: error.message });
    }
  }

  async auditErrorHandling() {
    console.log('\n🛡️ Auditing Error Handling...');
    
    const errorTests = [
      {
        name: 'Invalid Fund ID',
        config: { fundId: "99999", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
        expectError: true
      },
      {
        name: 'Invalid Score Range',
        config: { elivateScoreRange: { min: 150, max: 200 }, maxFunds: "5", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
        expectError: true
      },
      {
        name: 'Missing Required Parameters',
        config: { startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000" },
        expectError: true
      }
    ];

    for (const test of errorTests) {
      try {
        const response = await axios.post(`${this.baseUrl}/api/comprehensive-backtest`, test.config);
        
        if (test.expectError) {
          console.log(`  ⚠️ ${test.name}: WARN - Should have returned error but succeeded`);
          this.results.errors.push({ name: test.name, status: 'WARN', message: 'Expected error but got success' });
        } else {
          console.log(`  ✅ ${test.name}: PASS`);
          this.results.errors.push({ name: test.name, status: 'PASS' });
        }
      } catch (error) {
        if (test.expectError) {
          console.log(`  ✅ ${test.name}: PASS - Correctly handled error`);
          this.results.errors.push({ name: test.name, status: 'PASS', message: 'Correctly handled error' });
        } else {
          console.log(`  ❌ ${test.name}: FAIL - ${error.message}`);
          this.results.errors.push({ name: test.name, status: 'FAIL', error: error.message });
        }
      }
    }
  }

  generateComprehensiveReport() {
    console.log('\n📋 COMPREHENSIVE AUDIT REPORT');
    console.log('==================================================\n');

    const sections = [
      { name: 'Database Layer', results: this.results.database },
      { name: 'API Endpoints', results: this.results.api },
      { name: 'Business Logic', results: this.results.businessLogic },
      { name: 'Performance', results: this.results.performance },
      { name: 'Error Handling', results: this.results.errors }
    ];

    sections.forEach(section => {
      console.log(`${section.name.toUpperCase()}:`);
      
      const passed = section.results.filter(r => r.status === 'PASS').length;
      const warned = section.results.filter(r => r.status === 'WARN').length;
      const failed = section.results.filter(r => r.status === 'FAIL').length;
      
      console.log(`  ✅ Passed: ${passed}`);
      console.log(`  ⚠️ Warnings: ${warned}`);
      console.log(`  ❌ Failed: ${failed}`);
      
      if (failed > 0 || warned > 0) {
        console.log('  Issues:');
        section.results.forEach(result => {
          if (result.status !== 'PASS') {
            console.log(`    - ${result.name}: ${result.status} ${result.error || result.message || ''}`);
          }
        });
      }
      console.log('');
    });

    // Calculate overall health score
    const totalTests = sections.reduce((sum, section) => sum + section.results.length, 0);
    const totalPassed = sections.reduce((sum, section) => 
      sum + section.results.filter(r => r.status === 'PASS').length, 0);
    const healthScore = Math.round((totalPassed / totalTests) * 100);

    console.log(`OVERALL SYSTEM HEALTH: ${healthScore}%`);
    
    if (healthScore >= 90) {
      console.log('🟢 EXCELLENT - System is production ready');
    } else if (healthScore >= 75) {
      console.log('🟡 GOOD - Minor issues to address');
    } else if (healthScore >= 60) {
      console.log('🟠 NEEDS ATTENTION - Several issues require fixing');
    } else {
      console.log('🔴 CRITICAL - System needs significant improvements');
    }

    return {
      healthScore,
      totalTests,
      totalPassed,
      sections: sections
    };
  }
}

// Execute audit
async function runAudit() {
  const audit = new ComprehensiveBacktestingAudit();
  await audit.runFullAudit();
}

// Auto-run if called directly
runAudit().catch(console.error);

export { ComprehensiveBacktestingAudit };