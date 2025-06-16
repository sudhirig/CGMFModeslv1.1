/**
 * Validate Corrected Recommendation System
 * Comprehensive testing of the fixed recommendation logic
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

class RecommendationSystemValidator {

  static async validateCompleteSystem() {
    console.log('RECOMMENDATION SYSTEM VALIDATION');
    console.log('='.repeat(50));

    try {
      // Test 1: Verify distribution alignment
      await this.validateDistributionAlignment();
      
      // Test 2: Check threshold compliance
      await this.validateThresholdCompliance();
      
      // Test 3: Sample fund verification
      await this.validateSampleFunds();
      
      // Test 4: Edge case testing
      await this.validateEdgeCases();

      console.log('\n' + '='.repeat(50));
      console.log('✓ ALL VALIDATION TESTS PASSED');
      console.log('✓ Recommendation system is authentic and accurate');
      console.log('✓ Ready for production deployment');
      console.log('='.repeat(50));

    } catch (error) {
      console.error('Validation failed:', error);
      throw error;
    }
  }

  static async validateDistributionAlignment() {
    console.log('\n1. Distribution Alignment Test');
    console.log('-'.repeat(30));

    const distribution = await pool.query(`
      SELECT 
        recommendation,
        COUNT(*) as count,
        ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) as percentage,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05'
      GROUP BY recommendation
      ORDER BY MIN(total_score) DESC
    `);

    console.log('Current Distribution:');
    distribution.rows.forEach(row => {
      console.log(`  ${row.recommendation}: ${row.count} funds (${row.percentage}%) | Scores: ${row.min_score}-${row.max_score}`);
    });

    // Validate against expected conservative distribution
    const strongBuyPercent = parseFloat(distribution.rows.find(r => r.recommendation === 'STRONG_BUY')?.percentage || 0);
    const buyPercent = parseFloat(distribution.rows.find(r => r.recommendation === 'BUY')?.percentage || 0);
    
    if (strongBuyPercent > 2.0) {
      throw new Error(`STRONG_BUY percentage too high: ${strongBuyPercent}% (should be <2%)`);
    }
    if (buyPercent > 5.0) {
      throw new Error(`BUY percentage too high: ${buyPercent}% (should be <5%)`);
    }

    console.log('✓ Distribution follows conservative authentic pattern');
  }

  static async validateThresholdCompliance() {
    console.log('\n2. Threshold Compliance Test');
    console.log('-'.repeat(30));

    // Test for violations of documentation thresholds
    const violations = await pool.query(`
      SELECT 
        'STRONG_BUY_VIOLATION' as violation_type,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
        AND recommendation = 'STRONG_BUY' 
        AND total_score < 70
        AND NOT (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25)
      
      UNION ALL
      
      SELECT 
        'BUY_VIOLATION' as violation_type,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
        AND recommendation = 'BUY' 
        AND total_score < 60
        AND NOT (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20)
      
      UNION ALL
      
      SELECT 
        'HOLD_VIOLATION' as violation_type,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
        AND recommendation = 'HOLD' 
        AND total_score < 50
        AND NOT (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20)
      
      UNION ALL
      
      SELECT 
        'SELL_VIOLATION' as violation_type,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
        AND recommendation = 'SELL' 
        AND total_score < 35
        AND NOT (total_score >= 30 AND risk_grade_total >= 15)
    `);

    console.log('Threshold Compliance Check:');
    violations.rows.forEach(row => {
      console.log(`  ${row.violation_type}: ${row.count} violations`);
      if (parseInt(row.count) > 0) {
        throw new Error(`Documentation threshold violated: ${row.violation_type}`);
      }
    });

    console.log('✓ All recommendations comply with documentation thresholds');
  }

  static async validateSampleFunds() {
    console.log('\n3. Sample Fund Verification');
    console.log('-'.repeat(30));

    // Test specific score ranges
    const samples = await pool.query(`
      SELECT 
        fund_id,
        total_score,
        quartile,
        risk_grade_total,
        fundamentals_total,
        recommendation
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
        AND (
          (total_score >= 70) OR 
          (total_score >= 60 AND total_score < 70) OR
          (total_score >= 50 AND total_score < 60) OR
          (total_score >= 35 AND total_score < 50) OR
          (total_score < 35)
        )
      ORDER BY total_score DESC
      LIMIT 20
    `);

    console.log('Sample Fund Verification:');
    samples.rows.forEach(fund => {
      const expectedRec = this.calculateExpectedRecommendation(
        fund.total_score, 
        fund.quartile, 
        fund.risk_grade_total, 
        fund.fundamentals_total
      );
      
      const isCorrect = fund.recommendation === expectedRec;
      console.log(`  Fund ${fund.fund_id}: Score ${fund.total_score} → ${fund.recommendation} ${isCorrect ? '✓' : '✗'}`);
      
      if (!isCorrect) {
        throw new Error(`Sample fund ${fund.fund_id} has incorrect recommendation: ${fund.recommendation} (expected: ${expectedRec})`);
      }
    });

    console.log('✓ All sampled funds have correct recommendations');
  }

  static async validateEdgeCases() {
    console.log('\n4. Edge Case Testing');
    console.log('-'.repeat(30));

    // Test boundary conditions
    const edgeCases = await pool.query(`
      SELECT 
        CASE 
          WHEN total_score = 70 THEN 'Score_70_Boundary'
          WHEN total_score = 60 THEN 'Score_60_Boundary'
          WHEN total_score = 50 THEN 'Score_50_Boundary'
          WHEN total_score = 35 THEN 'Score_35_Boundary'
          ELSE 'Other'
        END as edge_case,
        total_score,
        recommendation,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
        AND total_score IN (70, 60, 50, 35)
      GROUP BY total_score, recommendation
      ORDER BY total_score DESC
    `);

    console.log('Edge Case Validation:');
    edgeCases.rows.forEach(row => {
      console.log(`  ${row.edge_case}: ${row.count} funds with ${row.recommendation}`);
      
      // Validate boundary logic
      if (row.total_score === 70 && row.recommendation !== 'STRONG_BUY') {
        throw new Error(`Score 70 should be STRONG_BUY, got ${row.recommendation}`);
      }
      if (row.total_score === 60 && !['STRONG_BUY', 'BUY'].includes(row.recommendation)) {
        throw new Error(`Score 60 should be BUY+, got ${row.recommendation}`);
      }
    });

    console.log('✓ All edge cases handled correctly');
  }

  // Helper function to calculate expected recommendation
  static calculateExpectedRecommendation(totalScore, quartile, riskGradeTotal, fundamentalsTotal) {
    if (totalScore >= 70 || (totalScore >= 65 && quartile === 1 && riskGradeTotal >= 25)) {
      return 'STRONG_BUY';
    }
    if (totalScore >= 60 || (totalScore >= 55 && [1, 2].includes(quartile) && fundamentalsTotal >= 20)) {
      return 'BUY';
    }
    if (totalScore >= 50 || (totalScore >= 45 && [1, 2, 3].includes(quartile) && riskGradeTotal >= 20)) {
      return 'HOLD';
    }
    if (totalScore >= 35 || (totalScore >= 30 && riskGradeTotal >= 15)) {
      return 'SELL';
    }
    return 'STRONG_SELL';
  }

  static async generateSystemReport() {
    console.log('\n5. System Status Report');
    console.log('-'.repeat(30));

    const systemStats = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN total_score >= 70 THEN 1 END) as excellent_performers,
        COUNT(CASE WHEN total_score >= 60 THEN 1 END) as good_performers,
        COUNT(CASE WHEN total_score >= 50 THEN 1 END) as average_performers,
        AVG(total_score)::numeric(5,2) as avg_score,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05'
    `);

    const stats = systemStats.rows[0];
    console.log('System Performance Summary:');
    console.log(`  Total Funds: ${stats.total_funds}`);
    console.log(`  Excellent (70+): ${stats.excellent_performers} funds (${((stats.excellent_performers/stats.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Good (60+): ${stats.good_performers} funds (${((stats.good_performers/stats.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Average (50+): ${stats.average_performers} funds (${((stats.average_performers/stats.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Score Range: ${stats.min_score} - ${stats.max_score} (avg: ${stats.avg_score})`);

    return stats;
  }
}

// Execute validation
if (import.meta.url === `file://${process.argv[1]}`) {
  RecommendationSystemValidator.validateCompleteSystem()
    .then(async () => {
      await RecommendationSystemValidator.generateSystemReport();
      console.log('\n✓ Validation completed successfully!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('\n✗ Validation failed:', error.message);
      process.exit(1);
    })
    .finally(async () => {
      await pool.end();
    });
}

export default RecommendationSystemValidator;