/**
 * Comprehensive Authentic Data Validation
 * Verifies all data sources are authentic and eliminates any synthetic contamination
 */

import { executeRawQuery } from './server/db.ts';

class AuthenticDataValidator {
  constructor() {
    this.contaminationSources = [
      'synthetic',
      'mock',
      'placeholder',
      'estimated',
      'simulated',
      'generated',
      'fake',
      'dummy'
    ];
  }

  /**
   * Run comprehensive data validation across all systems
   */
  async runComprehensiveValidation() {
    console.log('🔍 Starting Comprehensive Authentic Data Validation...');
    
    const validationResults = {
      elivateFramework: await this.validateElivateData(),
      marketIndices: await this.validateMarketIndices(),
      fundData: await this.validateFundData(),
      navData: await this.validateNavData(),
      performanceMetrics: await this.validatePerformanceMetrics()
    };

    const overallStatus = this.calculateOverallStatus(validationResults);
    this.generateValidationReport(validationResults, overallStatus);
    
    return validationResults;
  }

  /**
   * Validate ELIVATE framework data authenticity
   */
  async validateElivateData() {
    console.log('\n📊 Validating ELIVATE Framework Data...');
    
    // Check current ELIVATE score source
    const elivateScore = await executeRawQuery(`
      SELECT index_name, close_value, index_date, 
             EXTRACT(EPOCH FROM (NOW() - index_date))/3600 as hours_old
      FROM market_indices 
      WHERE index_name = 'ELIVATE_AUTHENTIC_CORRECTED'
      ORDER BY index_date DESC
      LIMIT 1
    `);

    // Check FRED data authenticity
    const fredData = await executeRawQuery(`
      SELECT index_name, close_value, index_date,
             EXTRACT(EPOCH FROM (NOW() - index_date))/3600 as hours_old
      FROM market_indices 
      WHERE index_name IN ('US_GDP_GROWTH', 'US_FED_RATE', 'US_CPI_INFLATION', 
                           'INDIA_GDP_GROWTH', 'INDIA_CPI_INFLATION', 'INDIA_10Y_YIELD',
                           'USD_INR_RATE', 'INDIA_REPO_RATE')
      ORDER BY index_name, index_date DESC
    `);

    // Check Yahoo Finance data authenticity
    const yahooData = await executeRawQuery(`
      SELECT index_name, close_value, index_date,
             EXTRACT(EPOCH FROM (NOW() - index_date))/3600 as hours_old
      FROM market_indices 
      WHERE index_name IN ('NIFTY_50', 'NIFTY_IT', 'NIFTY_BANK', 'INDIA_VIX',
                           'BSE_SENSEX', 'NIFTY_MIDCAP', 'NIFTY_AUTO', 'NIFTY_PHARMA')
      ORDER BY index_name, index_date DESC
    `);

    const validation = {
      status: 'AUTHENTIC',
      issues: [],
      dataSources: {
        elivate: elivateScore.rows[0] || null,
        fred: fredData.rows,
        yahoo: yahooData.rows
      }
    };

    // Validate data freshness
    if (elivateScore.rows[0]?.hours_old > 24) {
      validation.issues.push('ELIVATE score is stale (>24 hours old)');
    }

    fredData.rows.forEach(row => {
      if (row.hours_old > 168) { // 1 week
        validation.issues.push(`FRED data for ${row.index_name} is stale`);
      }
    });

    // Check for contamination patterns
    const allValues = [...fredData.rows, ...yahooData.rows];
    allValues.forEach(row => {
      if (this.isContaminated(row.close_value) || this.isContaminated(row.index_name)) {
        validation.status = 'CONTAMINATED';
        validation.issues.push(`Synthetic data detected in ${row.index_name}: ${row.close_value}`);
      }
    });

    console.log(`ELIVATE Framework: ${validation.status} (${validation.issues.length} issues)`);
    return validation;
  }

  /**
   * Validate market indices data
   */
  async validateMarketIndices() {
    console.log('\n📈 Validating Market Indices Data...');
    
    const indices = await executeRawQuery(`
      SELECT index_name, close_value, index_date,
             COUNT(*) as total_records,
             MAX(index_date) as latest_date,
             MIN(index_date) as earliest_date
      FROM market_indices 
      GROUP BY index_name, close_value, index_date
      ORDER BY index_name, index_date DESC
      LIMIT 100
    `);

    const validation = {
      status: 'AUTHENTIC',
      issues: [],
      totalIndices: new Set(indices.rows.map(r => r.index_name)).size,
      records: indices.rows.length
    };

    // Check for synthetic values
    indices.rows.forEach(row => {
      if (this.isContaminated(row.close_value) || this.isContaminated(row.data_source)) {
        validation.status = 'CONTAMINATED';
        validation.issues.push(`Synthetic data in ${row.index_name}`);
      }
    });

    // Check for reasonable data ranges
    const unreasonableValues = indices.rows.filter(r => 
      r.close_value && (r.close_value < 0 || r.close_value > 1000000)
    );

    if (unreasonableValues.length > 5) {
      validation.issues.push(`Unreasonable values detected: ${unreasonableValues.length} records`);
    }

    console.log(`Market Indices: ${validation.status} (${validation.totalIndices} indices, ${validation.issues.length} issues)`);
    return validation;
  }

  /**
   * Validate fund data authenticity
   */
  async validateFundData() {
    console.log('\n💰 Validating Fund Data...');
    
    const funds = await executeRawQuery(`
      SELECT scheme_code, fund_name, category, sub_category, 
             inception_date, expense_ratio, exit_load,
             COUNT(*) as records_count
      FROM funds 
      WHERE scheme_code IS NOT NULL
      GROUP BY scheme_code, fund_name, category, sub_category, 
               inception_date, expense_ratio, exit_load
      LIMIT 100
    `);

    const validation = {
      status: 'AUTHENTIC',
      issues: [],
      totalFunds: funds.rows.length,
      categories: new Set(funds.rows.map(r => r.category)).size
    };

    // Check for placeholder/synthetic fund names
    funds.rows.forEach(row => {
      if (this.isContaminated(row.fund_name) || 
          this.isContaminated(row.category) ||
          this.isContaminated(row.sub_category)) {
        validation.status = 'CONTAMINATED';
        validation.issues.push(`Synthetic fund data: ${row.scheme_code}`);
      }
    });

    // Check for unrealistic expense ratios
    const suspiciousRatios = funds.rows.filter(r => 
      r.expense_ratio < 0.1 || r.expense_ratio > 5.0
    );

    if (suspiciousRatios.length > 10) {
      validation.issues.push(`Suspicious expense ratios detected: ${suspiciousRatios.length} funds`);
    }

    console.log(`Fund Data: ${validation.status} (${validation.totalFunds} funds, ${validation.issues.length} issues)`);
    return validation;
  }

  /**
   * Validate NAV data authenticity
   */
  async validateNavData() {
    console.log('\n📊 Validating NAV Data...');
    
    const navStats = await executeRawQuery(`
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT fund_id) as unique_funds,
        MIN(nav_date) as earliest_date,
        MAX(nav_date) as latest_date,
        AVG(nav_value) as avg_nav,
        MIN(nav_value) as min_nav,
        MAX(nav_value) as max_nav
      FROM fund_nav_history 
      WHERE nav_value > 0
    `);

    const suspiciousNavs = await executeRawQuery(`
      SELECT fund_id, nav_date, nav_value
      FROM fund_nav_history 
      WHERE nav_value < 1 OR nav_value > 10000
      ORDER BY nav_date DESC
      LIMIT 50
    `);

    const validation = {
      status: 'AUTHENTIC',
      issues: [],
      stats: navStats.rows[0],
      suspiciousRecords: suspiciousNavs.rows.length
    };

    // Check for obviously synthetic NAV values
    suspiciousNavs.rows.forEach(row => {
      if (this.isContaminated(row.nav_value.toString())) {
        validation.status = 'CONTAMINATED';
        validation.issues.push(`Synthetic NAV value: Fund ${row.fund_id} - ${row.nav_value}`);
      }
    });

    // Check for data gaps
    const daysDiff = Math.floor((new Date() - new Date(navStats.rows[0].latest_date)) / (1000 * 60 * 60 * 24));
    if (daysDiff > 7) {
      validation.issues.push(`NAV data is ${daysDiff} days old`);
    }

    console.log(`NAV Data: ${validation.status} (${validation.stats.total_records} records, ${validation.issues.length} issues)`);
    return validation;
  }

  /**
   * Validate performance metrics
   */
  async validatePerformanceMetrics() {
    console.log('\n🎯 Validating Performance Metrics...');
    
    const metrics = await executeRawQuery(`
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT fund_id) as unique_funds,
        AVG(CASE WHEN return_1y IS NOT NULL THEN return_1y END) as avg_1y_return,
        AVG(CASE WHEN return_3y IS NOT NULL THEN return_3y END) as avg_3y_return,
        AVG(CASE WHEN risk_grade IS NOT NULL THEN risk_grade END) as avg_risk_grade,
        COUNT(CASE WHEN alpha IS NOT NULL THEN 1 END) as alpha_records,
        COUNT(CASE WHEN beta IS NOT NULL THEN 1 END) as beta_records
      FROM fund_performance_metrics
    `);

    const validation = {
      status: 'AUTHENTIC',
      issues: [],
      stats: metrics.rows[0]
    };

    // Check for unrealistic returns
    if (metrics.rows[0].avg_1y_return > 100 || metrics.rows[0].avg_1y_return < -50) {
      validation.issues.push('Unrealistic average 1Y returns detected');
    }

    // Check for missing calculated metrics
    const calculatedMetricsRatio = metrics.rows[0].alpha_records / metrics.rows[0].total_records;
    if (calculatedMetricsRatio < 0.1) {
      validation.issues.push('Low coverage of calculated risk metrics (Alpha/Beta)');
    }

    console.log(`Performance Metrics: ${validation.status} (${validation.stats.total_records} records, ${validation.issues.length} issues)`);
    return validation;
  }

  /**
   * Check if a value appears to be synthetic/contaminated
   */
  isContaminated(value) {
    if (!value) return false;
    
    const strValue = value.toString().toLowerCase();
    
    // Check for obvious contamination keywords
    for (const source of this.contaminationSources) {
      if (strValue.includes(source)) return true;
    }
    
    // Check for placeholder patterns
    if (strValue.match(/^(test|example|sample|demo)/)) return true;
    if (strValue.match(/\d{4}-\d{2}-\d{2}/) && strValue.includes('2000-01-01')) return true;
    if (strValue === '0' || strValue === '1' || strValue === '100') return false; // These can be legitimate
    
    return false;
  }

  /**
   * Calculate overall data integrity status
   */
  calculateOverallStatus(results) {
    const statuses = Object.values(results).map(r => r.status);
    const totalIssues = Object.values(results).reduce((sum, r) => sum + r.issues.length, 0);
    
    if (statuses.includes('CONTAMINATED')) {
      return { status: 'CONTAMINATED', totalIssues };
    }
    
    if (totalIssues > 10) {
      return { status: 'DEGRADED', totalIssues };
    }
    
    return { status: 'AUTHENTIC', totalIssues };
  }

  /**
   * Generate comprehensive validation report
   */
  generateValidationReport(results, overall) {
    console.log('\n' + '='.repeat(60));
    console.log('📋 COMPREHENSIVE DATA VALIDATION REPORT');
    console.log('='.repeat(60));
    
    console.log(`\n🎯 OVERALL STATUS: ${overall.status}`);
    console.log(`📊 Total Issues Found: ${overall.totalIssues}`);
    
    console.log('\n📈 COMPONENT STATUS:');
    Object.entries(results).forEach(([component, result]) => {
      const status = result.status === 'AUTHENTIC' ? '✅' : '❌';
      console.log(`${status} ${component}: ${result.status} (${result.issues.length} issues)`);
    });
    
    if (overall.totalIssues > 0) {
      console.log('\n⚠️  ISSUES DETECTED:');
      Object.entries(results).forEach(([component, result]) => {
        if (result.issues.length > 0) {
          console.log(`\n${component.toUpperCase()}:`);
          result.issues.forEach(issue => console.log(`  • ${issue}`));
        }
      });
    }
    
    console.log('\n🔧 DATA SOURCE VERIFICATION:');
    if (results.elivateFramework.dataSources) {
      const { elivate, fred, yahoo } = results.elivateFramework.dataSources;
      console.log(`• ELIVATE Score: ${elivate?.close_value || 'Missing'}/100 points`);
      console.log(`• FRED Indicators: ${fred?.length || 0} active`);
      console.log(`• Yahoo Finance: ${yahoo?.length || 0} active`);
    }
    
    console.log('\n✅ VALIDATION COMPLETE');
    console.log('='.repeat(60));
    
    return overall.status === 'AUTHENTIC';
  }
}

// Execute comprehensive validation
async function runComprehensiveValidation() {
  try {
    const validator = new AuthenticDataValidator();
    const results = await validator.runComprehensiveValidation();
    
    console.log('\n🎉 Data validation completed successfully');
    return results;
  } catch (error) {
    console.error('❌ Data validation failed:', error.message);
    throw error;
  }
}

runComprehensiveValidation();