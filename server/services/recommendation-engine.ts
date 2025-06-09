/**
 * Recommendation Engine
 * Centralized authentic recommendation logic based on original documentation
 * Ensures all scoring systems use consistent 70+/60+/50+/35+ thresholds
 */

export class RecommendationEngine {
  
  /**
   * Calculate recommendation using original documentation logic
   * This is the single source of truth for all recommendation calculations
   */
  static calculateRecommendation(
    totalScore: number, 
    quartile: number = 3, 
    riskGradeTotal: number = 0, 
    fundamentalsTotal: number = 0
  ): string {
    // Original documentation logic - exact implementation
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

  /**
   * Validate that a score meets documentation constraints
   */
  static validateScoreConstraints(totalScore: number): boolean {
    return totalScore >= 0 && totalScore <= 100;
  }

  /**
   * Get expected distribution for validation
   * Based on authentic 100-point scoring methodology
   */
  static getExpectedDistribution() {
    return {
      'STRONG_BUY': { min: 0.1, max: 1.0, threshold: 70 },
      'BUY': { min: 0.5, max: 2.0, threshold: 60 },
      'HOLD': { min: 45.0, max: 55.0, threshold: 50 },
      'SELL': { min: 40.0, max: 50.0, threshold: 35 },
      'STRONG_SELL': { min: 0.5, max: 2.0, threshold: 0 }
    };
  }

  /**
   * Validate recommendation distribution against expected ranges
   */
  static validateDistribution(distribution: Record<string, number>, totalFunds: number): {
    isValid: boolean;
    issues: string[];
  } {
    const expected = this.getExpectedDistribution();
    const issues: string[] = [];
    
    Object.entries(distribution).forEach(([recommendation, count]) => {
      const percentage = (count / totalFunds) * 100;
      const expectedRange = expected[recommendation as keyof typeof expected];
      
      if (expectedRange && (percentage < expectedRange.min || percentage > expectedRange.max)) {
        issues.push(`${recommendation}: ${percentage.toFixed(1)}% (expected ${expectedRange.min}-${expectedRange.max}%)`);
      }
    });

    return {
      isValid: issues.length === 0,
      issues
    };
  }

  /**
   * Generate comprehensive recommendation report
   */
  static generateRecommendationReport(
    totalScore: number,
    quartile: number,
    riskGradeTotal: number,
    fundamentalsTotal: number
  ) {
    const recommendation = this.calculateRecommendation(totalScore, quartile, riskGradeTotal, fundamentalsTotal);
    
    return {
      recommendation,
      reasoning: this.getRecommendationReasoning(totalScore, quartile, riskGradeTotal, fundamentalsTotal, recommendation),
      scoreBreakdown: {
        totalScore,
        quartile,
        riskGradeTotal,
        fundamentalsTotal
      },
      meetsThreshold: this.checkThresholdCompliance(totalScore, recommendation)
    };
  }

  private static getRecommendationReasoning(
    totalScore: number,
    quartile: number,
    riskGradeTotal: number,
    fundamentalsTotal: number,
    recommendation: string
  ): string {
    switch (recommendation) {
      case 'STRONG_BUY':
        if (totalScore >= 70) return `Strong performance with score ${totalScore}/100 (≥70 threshold)`;
        return `Exceptional Q1 fund with score ${totalScore}/100 and strong risk management (${riskGradeTotal})`;
      
      case 'BUY':
        if (totalScore >= 60) return `Good performance with score ${totalScore}/100 (≥60 threshold)`;
        return `Top quartile fund with score ${totalScore}/100 and solid fundamentals (${fundamentalsTotal})`;
      
      case 'HOLD':
        if (totalScore >= 50) return `Average performance with score ${totalScore}/100 (≥50 threshold)`;
        return `Upper quartile fund with score ${totalScore}/100 and adequate risk profile`;
      
      case 'SELL':
        if (totalScore >= 35) return `Below average performance with score ${totalScore}/100 (≥35 threshold)`;
        return `Lower performing fund with score ${totalScore}/100 requiring consideration`;
      
      default:
        return `Poor performance with score ${totalScore}/100 (<35 threshold)`;
    }
  }

  private static checkThresholdCompliance(totalScore: number, recommendation: string): boolean {
    switch (recommendation) {
      case 'STRONG_BUY': return totalScore >= 70;
      case 'BUY': return totalScore >= 60;
      case 'HOLD': return totalScore >= 50;
      case 'SELL': return totalScore >= 35;
      case 'STRONG_SELL': return totalScore < 35;
      default: return false;
    }
  }
}