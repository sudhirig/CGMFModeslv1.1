/**
 * Complete Synthetic Data Audit
 * Final comprehensive scan for any remaining synthetic data contamination
 */

import axios from 'axios';

class CompleteSyntheticDataAudit {
  constructor() {
    this.violations = [];
    this.suspiciousPatterns = [];
    this.authenticSources = [];
  }

  async runCompleteAudit() {
    console.log('🔍 COMPLETE SYNTHETIC DATA AUDIT');
    console.log('=================================\n');

    await this.auditDatabaseTables();
    await this.auditBackendAPIs();
    await this.auditFrontendIntegration();
    await this.auditCalculationEngine();
    await this.generateFinalReport();
  }

  async auditDatabaseTables() {
    console.log('📊 DATABASE TABLES AUDIT\n');

    const tableAudits = [
      {
        name: 'Funds Table',
        query: `
          SELECT 
            COUNT(*) as total_funds,
            COUNT(CASE WHEN LOWER(fund_name) LIKE '%test%' OR LOWER(fund_name) LIKE '%sample%' OR LOWER(fund_name) LIKE '%mock%' THEN 1 END) as synthetic_names,
            COUNT(CASE WHEN scheme_code IS NULL OR scheme_code = '' THEN 1 END) as missing_scheme_codes,
            COUNT(CASE WHEN category = 'Unknown' THEN 1 END) as unknown_categories
          FROM funds
        `
      },
      {
        name: 'NAV Data Integrity',
        query: `
          SELECT 
            COUNT(*) as total_nav_records,
            COUNT(CASE WHEN nav_value = 10.0 THEN 1 END) as exact_ten_values,
            COUNT(CASE WHEN nav_value = 100.0 THEN 1 END) as exact_hundred_values,
            COUNT(CASE WHEN nav_value > 10000 THEN 1 END) as unrealistic_high_values,
            COUNT(CASE WHEN nav_value < 0.1 THEN 1 END) as unrealistic_low_values,
            MIN(nav_value) as min_nav,
            MAX(nav_value) as max_nav,
            AVG(nav_value) as avg_nav
          FROM nav_data
          WHERE nav_date >= '2024-01-01'
        `
      },
      {
        name: 'ELIVATE Scores Validation',
        query: `
          SELECT 
            COUNT(*) as total_scores,
            COUNT(CASE WHEN total_score = 50.0 THEN 1 END) as default_fifty_scores,
            COUNT(CASE WHEN total_score = 0.0 THEN 1 END) as zero_scores,
            COUNT(CASE WHEN total_score = 100.0 THEN 1 END) as perfect_scores,
            COUNT(CASE WHEN MOD(total_score::numeric, 10) = 0 THEN 1 END) as round_number_scores,
            AVG(total_score) as avg_score,
            STDDEV(total_score) as score_deviation,
            MIN(total_score) as min_score,
            MAX(total_score) as max_score
          FROM fund_scores_corrected
          WHERE total_score IS NOT NULL
        `
      },
      {
        name: 'Market Indices Data',
        query: `
          SELECT 
            COUNT(*) as total_market_records,
            COUNT(DISTINCT index_name) as unique_indices,
            COUNT(CASE WHEN close_value = open_value THEN 1 END) as no_movement_days,
            COUNT(CASE WHEN close_value = 10000 THEN 1 END) as round_ten_thousand,
            MIN(close_value) as min_close,
            MAX(close_value) as max_close,
            AVG(close_value) as avg_close
          FROM market_indices
          WHERE index_date >= '2024-01-01'
        `
      }
    ];

    for (const audit of tableAudits) {
      try {
        const response = await axios.post('http://localhost:5000/api/execute-sql', {
          query: audit.query
        });

        console.log(`${audit.name}:`);
        if (response.data && response.data.length > 0) {
          const result = response.data[0];
          Object.entries(result).forEach(([key, value]) => {
            console.log(`  ${key}: ${value}`);
          });
          
          // Check for synthetic patterns
          this.analyzeTableData(audit.name, result);
        }
        console.log('');

      } catch (error) {
        this.violations.push({
          type: 'Database Query Error',
          table: audit.name,
          error: error.message
        });
        console.log(`  Error: ${error.message}\n`);
      }
    }
  }

  analyzeTableData(tableName, data) {
    switch (tableName) {
      case 'Funds Table':
        if (data.synthetic_names > 0) {
          this.violations.push({
            type: 'Synthetic Fund Names',
            count: data.synthetic_names,
            severity: 'CRITICAL'
          });
        }
        if (data.missing_scheme_codes > data.total_funds * 0.1) {
          this.suspiciousPatterns.push({
            type: 'Missing Scheme Codes',
            percentage: (data.missing_scheme_codes / data.total_funds * 100).toFixed(2)
          });
        }
        break;

      case 'NAV Data Integrity':
        if (data.exact_ten_values > data.total_nav_records * 0.01) {
          this.violations.push({
            type: 'Suspicious NAV Values (10.0)',
            count: data.exact_ten_values,
            severity: 'HIGH'
          });
        }
        if (data.unrealistic_high_values > 0) {
          this.suspiciousPatterns.push({
            type: 'Unrealistic High NAV Values',
            count: data.unrealistic_high_values
          });
        }
        break;

      case 'ELIVATE Scores Validation':
        if (data.default_fifty_scores > data.total_scores * 0.02) {
          this.violations.push({
            type: 'Default ELIVATE Scores (50.0)',
            count: data.default_fifty_scores,
            severity: 'MEDIUM'
          });
        }
        if (data.round_number_scores > data.total_scores * 0.3) {
          this.suspiciousPatterns.push({
            type: 'Excessive Round Number Scores',
            percentage: (data.round_number_scores / data.total_scores * 100).toFixed(2)
          });
        }
        break;
    }
  }

  async auditBackendAPIs() {
    console.log('⚙️ BACKEND APIs AUDIT\n');

    const apiTests = [
      { name: 'Individual Fund Backtest', endpoint: '/api/comprehensive-backtest', 
        payload: { fundId: "8319", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
      },
      { name: 'Score Range Backtest', endpoint: '/api/comprehensive-backtest',
        payload: { elivateScoreRange: { min: 80, max: 90 }, maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
      },
      { name: 'Top Rated Funds', endpoint: '/api/funds/top-rated', payload: null },
      { name: 'ELIVATE Score', endpoint: '/api/elivate/score', payload: null }
    ];

    for (const test of apiTests) {
      try {
        console.log(`Testing ${test.name}:`);
        
        const response = test.payload 
          ? await axios.post(`http://localhost:5000${test.endpoint}`, test.payload)
          : await axios.get(`http://localhost:5000${test.endpoint}`);

        const data = response.data;
        
        // Check for synthetic patterns in API responses
        if (test.name.includes('Backtest')) {
          this.validateBacktestResponse(data, test.name);
        } else if (test.name === 'Top Rated Funds') {
          this.validateTopFundsResponse(data);
        } else if (test.name === 'ELIVATE Score') {
          this.validateElivateResponse(data);
        }

        console.log(`  Status: ✅ API responding with data`);
        
      } catch (error) {
        this.violations.push({
          type: 'API Error',
          endpoint: test.endpoint,
          error: error.message
        });
        console.log(`  Status: ❌ Error - ${error.message}`);
      }
      console.log('');
    }
  }

  validateBacktestResponse(data, testName) {
    if (data.benchmark) {
      // Check for hardcoded benchmark values
      if (data.benchmark.benchmarkReturn === 10) {
        this.violations.push({
          type: 'Hardcoded Benchmark Return',
          test: testName,
          value: 10,
          severity: 'CRITICAL'
        });
      }
      if (data.benchmark.alpha === 2) {
        this.violations.push({
          type: 'Hardcoded Alpha Value',
          test: testName,
          value: 2,
          severity: 'CRITICAL'
        });
      }
    }

    if (data.attribution && data.attribution.fundContributions) {
      data.attribution.fundContributions.forEach(fund => {
        if (fund.fundName.toLowerCase().includes('test') || 
            fund.fundName.toLowerCase().includes('sample') ||
            fund.fundName.toLowerCase().includes('mock')) {
          this.violations.push({
            type: 'Synthetic Fund in Results',
            fund: fund.fundName,
            severity: 'HIGH'
          });
        }
      });
    }
  }

  validateTopFundsResponse(data) {
    if (Array.isArray(data)) {
      data.forEach(item => {
        if (item.fund && item.fund.fundName) {
          const name = item.fund.fundName.toLowerCase();
          if (name.includes('test') || name.includes('sample') || name.includes('mock')) {
            this.violations.push({
              type: 'Synthetic Fund in Top Rated',
              fund: item.fund.fundName,
              severity: 'HIGH'
            });
          }
        }
      });
    }
  }

  validateElivateResponse(data) {
    if (data.score === 50 || data.score === 0 || data.score === 100) {
      this.suspiciousPatterns.push({
        type: 'Suspicious Market ELIVATE Score',
        score: data.score,
        interpretation: data.interpretation
      });
    }
  }

  async auditFrontendIntegration() {
    console.log('🖥️ FRONTEND INTEGRATION AUDIT\n');

    try {
      // Test portfolio data
      const portfoliosResponse = await axios.get('http://localhost:5000/api/portfolios');
      console.log('Portfolio Data:');
      if (portfoliosResponse.data && portfoliosResponse.data.length > 0) {
        portfoliosResponse.data.forEach(portfolio => {
          console.log(`  ${portfolio.name}: ${portfolio.riskProfile}`);
        });
        console.log('  Status: ✅ Authentic portfolio data loaded');
      } else {
        console.log('  Status: ⚠️ No portfolio data available');
      }

      // Test market indices
      const indicesResponse = await axios.get('http://localhost:5000/api/market/indices');
      console.log('\nMarket Indices:');
      if (indicesResponse.data && indicesResponse.data.length > 0) {
        indicesResponse.data.slice(0, 3).forEach(index => {
          console.log(`  ${index.indexName}: ${index.indexValue || index.closeValue || 'N/A'}`);
        });
        console.log('  Status: ✅ Market data integration working');
      } else {
        console.log('  Status: ⚠️ No market indices data');
      }

    } catch (error) {
      this.violations.push({
        type: 'Frontend Integration Error',
        error: error.message
      });
      console.log(`  Error: ${error.message}`);
    }
    console.log('');
  }

  async auditCalculationEngine() {
    console.log('🧮 CALCULATION ENGINE AUDIT\n');

    // Test different backtesting scenarios for consistency
    const testScenarios = [
      { name: 'Q1 vs Q4 Comparison', tests: [
        { quartile: 'Q1', maxFunds: '3' },
        { quartile: 'Q4', maxFunds: '3' }
      ]},
      { name: 'Score Range Validation', tests: [
        { elivateScoreRange: { min: 90, max: 100 }, maxFunds: '3' },
        { elivateScoreRange: { min: 30, max: 40 }, maxFunds: '3' }
      ]}
    ];

    for (const scenario of testScenarios) {
      console.log(`${scenario.name}:`);
      const results = [];

      for (const test of scenario.tests) {
        try {
          const config = {
            ...test,
            startDate: "2024-01-01",
            endDate: "2024-12-31",
            initialAmount: "100000",
            rebalancePeriod: "quarterly"
          };

          const response = await axios.post('http://localhost:5000/api/comprehensive-backtest', config);
          const data = response.data;

          const label = test.quartile || `Score ${test.elivateScoreRange.min}-${test.elivateScoreRange.max}`;
          const totalReturn = data.performance?.totalReturn || 0;
          const benchmarkReturn = data.benchmark?.benchmarkReturn || 0;

          console.log(`  ${label}: ${totalReturn.toFixed(2)}% return, ${benchmarkReturn.toFixed(2)}% benchmark`);
          results.push({ label, totalReturn, benchmarkReturn });

        } catch (error) {
          console.log(`  Error: ${error.message}`);
        }
      }

      // Analyze results for synthetic patterns
      if (results.length === 2) {
        const returnDiff = Math.abs(results[0].totalReturn - results[1].totalReturn);
        const benchmarkDiff = Math.abs(results[0].benchmarkReturn - results[1].benchmarkReturn);

        if (returnDiff < 1 && scenario.name.includes('Q1 vs Q4')) {
          this.suspiciousPatterns.push({
            type: 'Insufficient Performance Differentiation',
            scenario: scenario.name,
            difference: returnDiff.toFixed(2)
          });
        }

        if (benchmarkDiff < 0.1) {
          this.authenticSources.push({
            type: 'Consistent Benchmark Calculation',
            scenario: scenario.name
          });
        }
      }
      console.log('');
    }
  }

  async generateFinalReport() {
    console.log('📋 FINAL SYNTHETIC DATA AUDIT REPORT');
    console.log('====================================\n');

    console.log('🚨 CRITICAL VIOLATIONS:');
    const criticalViolations = this.violations.filter(v => v.severity === 'CRITICAL');
    if (criticalViolations.length === 0) {
      console.log('  ✅ No critical synthetic data violations detected');
    } else {
      criticalViolations.forEach(v => {
        console.log(`  ❌ ${v.type}: ${v.count || v.value || v.error}`);
      });
    }

    console.log('\n⚠️ SUSPICIOUS PATTERNS:');
    if (this.suspiciousPatterns.length === 0) {
      console.log('  ✅ No suspicious patterns detected');
    } else {
      this.suspiciousPatterns.forEach(p => {
        console.log(`  ⚠️ ${p.type}: ${p.count || p.percentage || p.difference || JSON.stringify(p)}`);
      });
    }

    console.log('\n✅ AUTHENTIC DATA SOURCES:');
    if (this.authenticSources.length === 0) {
      console.log('  ⚠️ Limited authentic source verification');
    } else {
      this.authenticSources.forEach(s => {
        console.log(`  ✅ ${s.type}: ${s.scenario || s.description || 'Verified'}`);
      });
    }

    // Calculate final authenticity score
    const totalIssues = this.violations.length + this.suspiciousPatterns.length;
    const criticalIssues = criticalViolations.length;
    
    let authenticityScore = 100;
    authenticityScore -= (criticalIssues * 10); // Critical issues heavily penalized
    authenticityScore -= (this.suspiciousPatterns.length * 2); // Suspicious patterns lightly penalized
    authenticityScore -= (this.violations.length * 5); // Other violations moderately penalized
    authenticityScore = Math.max(0, authenticityScore);

    console.log('\n🎯 FINAL ASSESSMENT:');
    console.log(`   Data Authenticity Score: ${authenticityScore}%`);
    console.log(`   Critical Issues: ${criticalIssues}`);
    console.log(`   Total Issues: ${totalIssues}`);

    if (authenticityScore >= 98) {
      console.log('   Status: ✅ EXCELLENT - System is virtually free of synthetic data');
    } else if (authenticityScore >= 95) {
      console.log('   Status: ✅ VERY GOOD - Minimal synthetic data contamination');
    } else if (authenticityScore >= 90) {
      console.log('   Status: ⚠️ GOOD - Some areas need attention');
    } else if (authenticityScore >= 80) {
      console.log('   Status: 🟡 MODERATE - Several issues detected');
    } else {
      console.log('   Status: 🔴 POOR - Significant synthetic data contamination');
    }

    console.log('\n🔧 RECOMMENDED ACTIONS:');
    if (criticalIssues > 0) {
      console.log('  1. URGENT: Fix all critical synthetic data violations immediately');
    }
    if (this.suspiciousPatterns.length > 5) {
      console.log('  2. Investigate and resolve suspicious data patterns');
    }
    if (this.authenticSources.length === 0) {
      console.log('  3. Verify authentic data source connections');
    }
    if (totalIssues === 0) {
      console.log('  ✅ System passes synthetic data contamination audit');
    }

    return {
      authenticityScore,
      criticalIssues,
      totalIssues,
      violations: this.violations,
      suspiciousPatterns: this.suspiciousPatterns,
      authenticSources: this.authenticSources
    };
  }
}

// Execute complete audit
async function runCompleteSyntheticDataAudit() {
  const audit = new CompleteSyntheticDataAudit();
  const results = await audit.runCompleteAudit();
  return results;
}

runCompleteSyntheticDataAudit().catch(console.error);