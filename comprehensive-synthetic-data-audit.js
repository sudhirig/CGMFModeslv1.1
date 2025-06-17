/**
 * Comprehensive Synthetic Data Audit
 * Zero tolerance scan for synthetic, mock, placeholder, or fallback data
 */

import axios from 'axios';

class SyntheticDataAudit {
  constructor() {
    this.violations = [];
    this.suspiciousPatterns = [];
    this.authenticSources = [];
  }

  async runCompleteAudit() {
    console.log('🔍 COMPREHENSIVE SYNTHETIC DATA AUDIT');
    console.log('=====================================\n');

    await this.auditDatabaseIntegrity();
    await this.auditBackendCalculations();
    await this.auditFrontendDataFlow();
    await this.auditAPIResponses();
    await this.auditDataSources();
    
    this.generateComprehensiveReport();
  }

  async auditDatabaseIntegrity() {
    console.log('📊 AUDITING DATABASE FOR SYNTHETIC DATA...\n');

    const queries = [
      {
        name: 'NAV Data Authenticity',
        query: `
          SELECT 
            COUNT(*) as total_nav_records,
            COUNT(CASE WHEN nav_value = 10.0 THEN 1 END) as suspicious_round_values,
            COUNT(CASE WHEN nav_value BETWEEN 9.99 AND 10.01 THEN 1 END) as potential_defaults,
            MIN(nav_value) as min_nav,
            MAX(nav_value) as max_nav,
            COUNT(DISTINCT fund_id) as funds_with_nav
          FROM nav_data 
          WHERE nav_date >= '2024-01-01'
        `
      },
      {
        name: 'ELIVATE Score Patterns',
        query: `
          SELECT 
            COUNT(*) as total_scores,
            COUNT(CASE WHEN total_score = 50.0 THEN 1 END) as default_fifty_scores,
            COUNT(CASE WHEN MOD(total_score::numeric, 10) = 0 THEN 1 END) as round_number_scores,
            COUNT(CASE WHEN total_score BETWEEN 49.5 AND 50.5 THEN 1 END) as near_default_scores,
            AVG(total_score) as avg_score,
            STDDEV(total_score) as score_deviation
          FROM fund_scores_corrected
          WHERE total_score IS NOT NULL
        `
      },
      {
        name: 'Fund Data Completeness',
        query: `
          SELECT 
            COUNT(*) as total_funds,
            COUNT(CASE WHEN fund_name LIKE '%Test%' OR fund_name LIKE '%Sample%' OR fund_name LIKE '%Mock%' THEN 1 END) as test_funds,
            COUNT(CASE WHEN scheme_code IS NULL OR scheme_code = '' THEN 1 END) as missing_scheme_codes,
            COUNT(CASE WHEN category = 'Unknown' OR category IS NULL THEN 1 END) as unknown_categories,
            COUNT(CASE WHEN fund_name IS NOT NULL AND LENGTH(fund_name) > 10 THEN 1 END) as complete_names
          FROM funds
        `
      },
      {
        name: 'Recommendation Distribution',
        query: `
          SELECT 
            recommendation,
            COUNT(*) as count,
            ROUND(AVG(total_score), 2) as avg_score,
            MIN(total_score) as min_score,
            MAX(total_score) as max_score
          FROM fund_scores_corrected 
          WHERE recommendation IS NOT NULL
          GROUP BY recommendation
          ORDER BY avg_score DESC
        `
      }
    ];

    for (const test of queries) {
      try {
        const response = await axios.post('http://localhost:5000/api/execute-sql', {
          query: test.query
        });

        console.log(`${test.name}:`);
        if (Array.isArray(response.data)) {
          response.data.forEach(row => {
            console.log(`  ${JSON.stringify(row)}`);
          });
        }
        console.log('');

        // Analyze for synthetic data patterns
        this.analyzeDatabaseResults(test.name, response.data);

      } catch (error) {
        this.violations.push({
          type: 'Database Error',
          test: test.name,
          error: error.message
        });
      }
    }
  }

  analyzeDatabaseResults(testName, data) {
    if (!Array.isArray(data) || data.length === 0) return;

    const row = data[0];

    switch (testName) {
      case 'NAV Data Authenticity':
        if (row.suspicious_round_values > row.total_nav_records * 0.1) {
          this.violations.push({
            type: 'Synthetic NAV Data',
            issue: `${row.suspicious_round_values} suspicious round NAV values (10.0) detected`,
            severity: 'HIGH'
          });
        }
        if (row.potential_defaults > row.total_nav_records * 0.05) {
          this.suspiciousPatterns.push({
            type: 'Default NAV Values',
            count: row.potential_defaults,
            percentage: (row.potential_defaults / row.total_nav_records * 100).toFixed(2)
          });
        }
        break;

      case 'ELIVATE Score Patterns':
        if (row.default_fifty_scores > row.total_scores * 0.02) {
          this.violations.push({
            type: 'Default ELIVATE Scores',
            issue: `${row.default_fifty_scores} funds with exactly 50.0 score (potential defaults)`,
            severity: 'MEDIUM'
          });
        }
        if (row.round_number_scores > row.total_scores * 0.3) {
          this.suspiciousPatterns.push({
            type: 'Round Number Bias',
            count: row.round_number_scores,
            note: 'High percentage of round number scores'
          });
        }
        break;

      case 'Fund Data Completeness':
        if (row.test_funds > 0) {
          this.violations.push({
            type: 'Test Fund Data',
            issue: `${row.test_funds} funds with test/sample/mock names detected`,
            severity: 'CRITICAL'
          });
        }
        if (row.missing_scheme_codes > row.total_funds * 0.1) {
          this.violations.push({
            type: 'Missing Authentic Identifiers',
            issue: `${row.missing_scheme_codes} funds missing scheme codes`,
            severity: 'MEDIUM'
          });
        }
        break;
    }
  }

  async auditBackendCalculations() {
    console.log('⚙️ AUDITING BACKEND CALCULATIONS...\n');

    const testCases = [
      {
        name: 'Individual Fund Calculation',
        config: { fundId: "8319", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
      },
      {
        name: 'Score Range Calculation',
        config: { elivateScoreRange: { min: 80, max: 90 }, maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
      }
    ];

    for (const test of testCases) {
      try {
        const response = await axios.post('http://localhost:5000/api/comprehensive-backtest', test.config);
        const data = response.data;

        console.log(`${test.name}:`);
        console.log(`  Total Return: ${data.performance?.totalReturn?.toFixed(4)}%`);
        console.log(`  Risk Metrics: Volatility ${data.riskMetrics?.volatility?.toFixed(4)}%`);
        
        if (data.attribution?.fundContributions) {
          data.attribution.fundContributions.forEach((fund, idx) => {
            console.log(`  Fund ${idx + 1}: ${fund.fundName.substring(0, 30)}... (Score: ${fund.elivateScore})`);
            
            // Check for synthetic patterns
            this.validateFundData(fund);
          });
        }
        console.log('');

      } catch (error) {
        this.violations.push({
          type: 'Backend Calculation Error',
          test: test.name,
          error: error.message
        });
      }
    }
  }

  validateFundData(fund) {
    // Check for placeholder fund names
    if (fund.fundName.includes('Sample') || fund.fundName.includes('Test') || fund.fundName.includes('Mock')) {
      this.violations.push({
        type: 'Synthetic Fund Name',
        fund: fund.fundName,
        severity: 'CRITICAL'
      });
    }

    // Check for default ELIVATE scores
    const score = parseFloat(fund.elivateScore);
    if (score === 50.0 || score === 0.0 || score === 100.0) {
      this.suspiciousPatterns.push({
        type: 'Suspicious ELIVATE Score',
        fund: fund.fundName,
        score: score
      });
    }

    // Check for synthetic returns
    if (fund.absoluteReturn === 0.01 || fund.absoluteReturn === 0.1 || fund.absoluteReturn === -0.01) {
      this.suspiciousPatterns.push({
        type: 'Potential Synthetic Return',
        fund: fund.fundName,
        return: fund.absoluteReturn
      });
    }
  }

  async auditFrontendDataFlow() {
    console.log('🖥️ AUDITING FRONTEND DATA FLOW...\n');

    try {
      // Test top-rated funds endpoint
      const topFundsResponse = await axios.get('http://localhost:5000/api/funds/top-rated');
      const topFunds = topFundsResponse.data;

      console.log('Top-Rated Funds Audit:');
      if (Array.isArray(topFunds) && topFunds.length > 0) {
        topFunds.slice(0, 3).forEach((item, idx) => {
          const fund = item.fund;
          console.log(`  ${idx + 1}. ${fund.fundName} (Score: ${fund.elivateScore || 'N/A'})`);
          
          if (fund.fundName.includes('Sample') || fund.fundName.includes('Test')) {
            this.violations.push({
              type: 'Frontend Synthetic Data',
              source: 'Top Rated Funds',
              fund: fund.fundName,
              severity: 'HIGH'
            });
          }
        });
      }

      // Test ELIVATE score endpoint
      const elivateResponse = await axios.get('http://localhost:5000/api/elivate/score');
      const elivateData = elivateResponse.data;

      console.log(`\nELIVATE Score: ${elivateData.score} (${elivateData.interpretation})`);
      
      if (elivateData.score === 50 || elivateData.score === 0 || elivateData.score === 100) {
        this.suspiciousPatterns.push({
          type: 'Suspicious Market ELIVATE Score',
          score: elivateData.score,
          interpretation: elivateData.interpretation
        });
      }

    } catch (error) {
      this.violations.push({
        type: 'Frontend API Error',
        error: error.message
      });
    }
  }

  async auditAPIResponses() {
    console.log('\n🌐 AUDITING API RESPONSE AUTHENTICITY...\n');

    try {
      // Audit market indices
      const indicesResponse = await axios.get('http://localhost:5000/api/market/indices');
      const indices = indicesResponse.data;

      console.log('Market Indices Audit:');
      if (Array.isArray(indices)) {
        indices.slice(0, 3).forEach(index => {
          console.log(`  ${index.indexName}: ${index.indexValue} (${index.changePercent}%)`);
          
          // Check for synthetic patterns
          if (index.indexValue === 10000 || index.changePercent === 1.0 || index.changePercent === 0.0) {
            this.suspiciousPatterns.push({
              type: 'Potential Synthetic Index Data',
              index: index.indexName,
              value: index.indexValue,
              change: index.changePercent
            });
          }
        });
      }

    } catch (error) {
      console.log(`  Error fetching market data: ${error.message}`);
    }
  }

  async auditDataSources() {
    console.log('\n📡 AUDITING DATA SOURCE AUTHENTICITY...\n');

    // Check for authentic data markers
    try {
      const authCheckQuery = `
        SELECT 
          'NAV Source Check' as audit_type,
          COUNT(DISTINCT fund_id) as funds_with_data,
          COUNT(*) as total_records,
          MIN(nav_date) as earliest_date,
          MAX(nav_date) as latest_date
        FROM nav_data 
        WHERE nav_date >= '2024-01-01'
        AND nav_value > 0
      `;

      const response = await axios.post('http://localhost:5000/api/execute-sql', {
        query: authCheckQuery
      });

      if (response.data && response.data.length > 0) {
        const result = response.data[0];
        console.log('NAV Data Source Verification:');
        console.log(`  Funds with data: ${result.funds_with_data}`);
        console.log(`  Total records: ${result.total_records}`);
        console.log(`  Date range: ${result.earliest_date} to ${result.latest_date}`);
        
        if (result.total_records > 100000) {
          this.authenticSources.push({
            type: 'NAV Data',
            recordCount: result.total_records,
            fundCount: result.funds_with_data,
            dateRange: `${result.earliest_date} to ${result.latest_date}`,
            status: 'AUTHENTIC'
          });
        }
      }

    } catch (error) {
      this.violations.push({
        type: 'Data Source Verification Error',
        error: error.message
      });
    }
  }

  generateComprehensiveReport() {
    console.log('\n📋 SYNTHETIC DATA AUDIT REPORT');
    console.log('===============================\n');

    console.log('🚨 CRITICAL VIOLATIONS:');
    const criticalViolations = this.violations.filter(v => v.severity === 'CRITICAL');
    if (criticalViolations.length === 0) {
      console.log('  ✅ None detected');
    } else {
      criticalViolations.forEach(v => {
        console.log(`  ❌ ${v.type}: ${v.issue || v.error}`);
      });
    }

    console.log('\n⚠️ SUSPICIOUS PATTERNS:');
    if (this.suspiciousPatterns.length === 0) {
      console.log('  ✅ No suspicious patterns detected');
    } else {
      this.suspiciousPatterns.forEach(p => {
        console.log(`  ⚠️ ${p.type}: ${p.count || p.note || JSON.stringify(p)}`);
      });
    }

    console.log('\n✅ AUTHENTIC DATA SOURCES:');
    if (this.authenticSources.length === 0) {
      console.log('  ⚠️ Limited authentic source verification');
    } else {
      this.authenticSources.forEach(s => {
        console.log(`  ✅ ${s.type}: ${s.recordCount} records, ${s.fundCount} funds (${s.status})`);
      });
    }

    // Calculate overall integrity score
    const totalIssues = this.violations.length + this.suspiciousPatterns.length;
    const authenticityScore = Math.max(0, 100 - (totalIssues * 5));

    console.log('\n🎯 OVERALL ASSESSMENT:');
    console.log(`   Data Authenticity Score: ${authenticityScore}%`);
    
    if (authenticityScore >= 95) {
      console.log('   Status: ✅ EXCELLENT - Minimal synthetic data contamination');
    } else if (authenticityScore >= 85) {
      console.log('   Status: ⚠️ GOOD - Some areas need attention');
    } else if (authenticityScore >= 70) {
      console.log('   Status: 🟡 MODERATE - Several synthetic data issues detected');
    } else {
      console.log('   Status: 🔴 POOR - Significant synthetic data contamination');
    }

    console.log('\n🔧 RECOMMENDED ACTIONS:');
    if (criticalViolations.length > 0) {
      console.log('  1. Immediately remove all test/mock/sample data');
    }
    if (this.suspiciousPatterns.length > 0) {
      console.log('  2. Investigate suspicious patterns for potential synthetic data');
    }
    if (this.authenticSources.length === 0) {
      console.log('  3. Verify all data sources are connected to authentic feeds');
    }
    if (totalIssues === 0) {
      console.log('  ✅ System appears clean of synthetic data contamination');
    }
  }
}

// Execute comprehensive audit
async function runSyntheticDataAudit() {
  const audit = new SyntheticDataAudit();
  await audit.runCompleteAudit();
}

runSyntheticDataAudit().catch(console.error);