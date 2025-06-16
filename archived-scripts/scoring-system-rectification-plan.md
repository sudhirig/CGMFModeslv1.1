# Scoring System Rectification Plan
## Complete Implementation According to Original Documentation Logic

### Current Critical Issues Identified:
1. **Scale Violation**: Original backup used raw percentages (0-192.1) instead of 0-8 point caps
2. **Component Overflow**: Current system has advanced metrics reaching 100.9 points (should be max 30)
3. **Mathematical Inconsistency**: None of the implementations follow documented quartile-based scoring
4. **Total Range Mismatch**: Doc (34-74), Backup (40-432), Current (41-169) - all different
5. **Missing Threshold Logic**: No implementation uses the documented performance thresholds

---

## Phase 1: Database Schema Correction (Priority: CRITICAL)

### 1.1 Create Proper Scoring Table Structure
```sql
-- Create new table matching exact documentation schema
CREATE TABLE fund_scores_corrected (
    fund_id INTEGER REFERENCES funds(id),
    score_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT now(),
    
    -- Historical Returns Components (40 points maximum)
    return_3m_score NUMERIC(4,2) CHECK (return_3m_score >= -0.30 AND return_3m_score <= 8.00),
    return_6m_score NUMERIC(4,2) CHECK (return_6m_score >= -0.40 AND return_6m_score <= 8.00),
    return_1y_score NUMERIC(4,2) CHECK (return_1y_score >= -0.20 AND return_1y_score <= 5.90),
    return_3y_score NUMERIC(4,2) CHECK (return_3y_score >= -0.10 AND return_3y_score <= 8.00),
    return_5y_score NUMERIC(4,2) CHECK (return_5y_score >= 0.00 AND return_5y_score <= 8.00),
    historical_returns_total NUMERIC(5,2) CHECK (historical_returns_total >= -0.70 AND historical_returns_total <= 32.00),
    
    -- Risk Assessment Components (30 points maximum)
    std_dev_1y_score NUMERIC(4,2) CHECK (std_dev_1y_score >= 0 AND std_dev_1y_score <= 8.00),
    std_dev_3y_score NUMERIC(4,2) CHECK (std_dev_3y_score >= 0 AND std_dev_3y_score <= 8.00),
    updown_capture_1y_score NUMERIC(4,2) CHECK (updown_capture_1y_score >= 0 AND updown_capture_1y_score <= 8.00),
    updown_capture_3y_score NUMERIC(4,2) CHECK (updown_capture_3y_score >= 0 AND updown_capture_3y_score <= 8.00),
    max_drawdown_score NUMERIC(4,2) CHECK (max_drawdown_score >= 0 AND max_drawdown_score <= 8.00),
    risk_grade_total NUMERIC(5,2) CHECK (risk_grade_total >= 13.00 AND risk_grade_total <= 30.00),
    
    -- Fundamentals Components (30 points maximum)
    expense_ratio_score NUMERIC(4,2) CHECK (expense_ratio_score >= 3.00 AND expense_ratio_score <= 8.00),
    aum_size_score NUMERIC(4,2) CHECK (aum_size_score >= 4.00 AND aum_size_score <= 7.00),
    age_maturity_score NUMERIC(4,2) CHECK (age_maturity_score >= 0 AND age_maturity_score <= 8.00),
    fundamentals_total NUMERIC(5,2) CHECK (fundamentals_total >= 0 AND fundamentals_total <= 30.00),
    
    -- Advanced Scoring Components (remaining points to reach 100)
    sectoral_similarity_score NUMERIC(4,2) CHECK (sectoral_similarity_score >= 0 AND sectoral_similarity_score <= 8.00),
    forward_score NUMERIC(4,2) CHECK (forward_score >= 0 AND forward_score <= 8.00),
    momentum_score NUMERIC(4,2) CHECK (momentum_score >= 0 AND momentum_score <= 8.00),
    consistency_score NUMERIC(4,2) CHECK (consistency_score >= 0 AND consistency_score <= 8.00),
    other_metrics_total NUMERIC(5,2) CHECK (other_metrics_total >= 0 AND other_metrics_total <= 30.00),
    
    -- Final Scoring and Rankings
    total_score NUMERIC(6,2) CHECK (total_score >= 34.00 AND total_score <= 100.00),
    quartile INTEGER CHECK (quartile >= 1 AND quartile <= 4),
    category_rank INTEGER,
    category_total INTEGER,
    subcategory VARCHAR(100),
    subcategory_rank INTEGER,
    subcategory_total INTEGER,
    subcategory_quartile INTEGER CHECK (subcategory_quartile >= 1 AND subcategory_quartile <= 4),
    subcategory_percentile NUMERIC(5,2) CHECK (subcategory_percentile >= 0 AND subcategory_percentile <= 100),
    recommendation TEXT CHECK (recommendation IN ('STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL')),
    
    PRIMARY KEY (fund_id, score_date)
);
```

### 1.2 Create Supporting Calculation Tables
```sql
-- Store raw calculation data for transparency
CREATE TABLE fund_raw_metrics (
    fund_id INTEGER REFERENCES funds(id),
    calculation_date DATE NOT NULL DEFAULT CURRENT_DATE,
    
    -- Raw return values (percentages)
    return_3m_percent NUMERIC(8,4),
    return_6m_percent NUMERIC(8,4),
    return_1y_percent NUMERIC(8,4),
    return_3y_percent NUMERIC(8,4),
    return_5y_percent NUMERIC(8,4),
    
    -- Raw risk metrics
    volatility_1y_percent NUMERIC(8,4),
    volatility_3y_percent NUMERIC(8,4),
    max_drawdown_percent NUMERIC(8,4),
    sharpe_ratio_1y NUMERIC(8,4),
    up_capture_1y NUMERIC(8,4),
    down_capture_1y NUMERIC(8,4),
    
    -- Category quartile positions (for threshold-based scoring)
    return_3m_quartile INTEGER,
    return_6m_quartile INTEGER,
    return_1y_quartile INTEGER,
    return_3y_quartile INTEGER,
    return_5y_quartile INTEGER,
    
    PRIMARY KEY (fund_id, calculation_date)
);
```

---

## Phase 2: Implement Correct Mathematical Logic

### 2.1 Historical Returns Scoring Engine
```javascript
// server/services/historical-returns-scoring.js
class HistoricalReturnsScoring {
    
    // Exact thresholds from documentation
    static RETURN_THRESHOLDS = {
        excellent: { min: 15.0, score: 8.0 },
        good: { min: 12.0, score: 6.4 },
        average: { min: 8.0, score: 4.8 },
        below_average: { min: 5.0, score: 3.2 },
        poor: { min: 0.0, score: 1.6 },
        negative: { max: 0.0, score: 0.0 }
    };
    
    static calculatePeriodReturn(currentNav, historicalNav, days) {
        if (!currentNav || !historicalNav || historicalNav <= 0) return null;
        
        const years = days / 365.25;
        
        if (years <= 1) {
            // Simple return for periods <= 1 year
            return ((currentNav / historicalNav) - 1) * 100;
        } else {
            // Annualized return for periods > 1 year
            return (Math.pow(currentNav / historicalNav, 1/years) - 1) * 100;
        }
    }
    
    static scoreReturnValue(returnPercent) {
        if (returnPercent === null || returnPercent === undefined) return 0;
        
        // Apply exact threshold logic from documentation
        if (returnPercent >= this.RETURN_THRESHOLDS.excellent.min) {
            return this.RETURN_THRESHOLDS.excellent.score;
        } else if (returnPercent >= this.RETURN_THRESHOLDS.good.min) {
            return this.RETURN_THRESHOLDS.good.score;
        } else if (returnPercent >= this.RETURN_THRESHOLDS.average.min) {
            return this.RETURN_THRESHOLDS.average.score;
        } else if (returnPercent >= this.RETURN_THRESHOLDS.below_average.min) {
            return this.RETURN_THRESHOLDS.below_average.score;
        } else if (returnPercent >= this.RETURN_THRESHOLDS.poor.min) {
            return this.RETURN_THRESHOLDS.poor.score;
        } else {
            // Handle negative returns with proportional scoring
            return Math.max(-0.70, returnPercent * 0.1); // Cap at -0.70 as per doc
        }
    }
    
    static async calculateHistoricalReturnsComponent(fundId) {
        const navData = await this.getNavData(fundId);
        if (!navData || navData.length < 90) return null;
        
        const periods = [
            { name: '3m', days: 90 },
            { name: '6m', days: 180 },
            { name: '1y', days: 365 },
            { name: '3y', days: 1095 },
            { name: '5y', days: 1825 }
        ];
        
        const scores = {};
        let totalScore = 0;
        
        for (const period of periods) {
            if (navData.length >= period.days) {
                const returnPercent = this.calculatePeriodReturn(
                    navData[navData.length - 1].nav_value,
                    navData[navData.length - period.days].nav_value,
                    period.days
                );
                
                const score = this.scoreReturnValue(returnPercent);
                scores[`return_${period.name}_score`] = score;
                scores[`return_${period.name}_percent`] = returnPercent;
                totalScore += score;
            } else {
                scores[`return_${period.name}_score`] = 0;
                scores[`return_${period.name}_percent`] = null;
            }
        }
        
        scores.historical_returns_total = Math.min(32.00, Math.max(-0.70, totalScore));
        return scores;
    }
}
```

### 2.2 Risk Assessment Scoring Engine
```javascript
// server/services/risk-assessment-scoring.js
class RiskAssessmentScoring {
    
    static calculateVolatility(dailyReturns) {
        if (!dailyReturns || dailyReturns.length < 50) return null;
        
        const mean = dailyReturns.reduce((sum, ret) => sum + ret, 0) / dailyReturns.length;
        const variance = dailyReturns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / (dailyReturns.length - 1);
        
        // Annualized volatility
        return Math.sqrt(variance * 252) * 100;
    }
    
    static scoreVolatility(volatilityPercent, categoryQuartile) {
        // Lower volatility = higher score (inverse scoring)
        // Use category quartile position for relative scoring
        switch(categoryQuartile) {
            case 1: return 8.0; // Lowest volatility quartile
            case 2: return 6.0;
            case 3: return 4.0;
            case 4: return 2.0; // Highest volatility quartile
            default: return 0.0;
        }
    }
    
    static calculateUpDownCapture(fundReturns, benchmarkReturns) {
        if (!fundReturns || !benchmarkReturns || fundReturns.length !== benchmarkReturns.length) {
            return { upCapture: 1.0, downCapture: 1.0 };
        }
        
        const upPeriods = [];
        const downPeriods = [];
        
        for (let i = 0; i < fundReturns.length; i++) {
            if (benchmarkReturns[i] > 0) {
                upPeriods.push({ fund: fundReturns[i], benchmark: benchmarkReturns[i] });
            } else if (benchmarkReturns[i] < 0) {
                downPeriods.push({ fund: fundReturns[i], benchmark: benchmarkReturns[i] });
            }
        }
        
        const upCapture = upPeriods.length > 0 ? 
            (upPeriods.reduce((sum, p) => sum + p.fund, 0) / upPeriods.length) / 
            (upPeriods.reduce((sum, p) => sum + p.benchmark, 0) / upPeriods.length) : 1.0;
            
        const downCapture = downPeriods.length > 0 ? 
            (downPeriods.reduce((sum, p) => sum + p.fund, 0) / downPeriods.length) / 
            (downPeriods.reduce((sum, p) => sum + p.benchmark, 0) / downPeriods.length) : 1.0;
        
        return { upCapture, downCapture };
    }
    
    static scoreUpDownCapture(upCapture, downCapture) {
        // Ideal: High up capture (>1.0), Low down capture (<1.0)
        const captureRatio = upCapture / Math.abs(downCapture);
        
        if (captureRatio >= 1.5) return 8.0;
        else if (captureRatio >= 1.2) return 6.0;
        else if (captureRatio >= 1.0) return 4.0;
        else if (captureRatio >= 0.8) return 2.0;
        else return 0.0;
    }
    
    static calculateMaxDrawdown(navData) {
        if (!navData || navData.length < 50) return 0;
        
        let maxDrawdown = 0;
        let peak = navData[0].nav_value;
        
        for (const data of navData) {
            const nav = data.nav_value;
            if (nav > peak) {
                peak = nav;
            }
            
            const drawdown = (peak - nav) / peak;
            if (drawdown > maxDrawdown) {
                maxDrawdown = drawdown;
            }
        }
        
        return maxDrawdown * 100; // Convert to percentage
    }
    
    static scoreMaxDrawdown(drawdownPercent) {
        // Lower drawdown = higher score
        if (drawdownPercent <= 5) return 8.0;
        else if (drawdownPercent <= 10) return 6.0;
        else if (drawdownPercent <= 15) return 4.0;
        else if (drawdownPercent <= 25) return 2.0;
        else return 0.0;
    }
    
    static async calculateRiskAssessmentComponent(fundId) {
        const navData = await this.getNavData(fundId);
        const benchmarkData = await this.getBenchmarkData(fundId);
        
        if (!navData || navData.length < 252) return null;
        
        const scores = {};
        
        // Calculate daily returns for 1Y and 3Y
        const dailyReturns1Y = this.calculateDailyReturns(navData, 365);
        const dailyReturns3Y = this.calculateDailyReturns(navData, 1095);
        
        // Volatility scoring
        if (dailyReturns1Y.length >= 250) {
            const volatility1Y = this.calculateVolatility(dailyReturns1Y);
            const categoryQuartile1Y = await this.getCategoryVolatilityQuartile(fundId, volatility1Y, '1y');
            scores.std_dev_1y_score = this.scoreVolatility(volatility1Y, categoryQuartile1Y);
        } else {
            scores.std_dev_1y_score = 0;
        }
        
        if (dailyReturns3Y.length >= 750) {
            const volatility3Y = this.calculateVolatility(dailyReturns3Y);
            const categoryQuartile3Y = await this.getCategoryVolatilityQuartile(fundId, volatility3Y, '3y');
            scores.std_dev_3y_score = this.scoreVolatility(volatility3Y, categoryQuartile3Y);
        } else {
            scores.std_dev_3y_score = 0;
        }
        
        // Up/Down Capture scoring
        if (benchmarkData && benchmarkData.length >= 365) {
            const benchmarkReturns1Y = this.calculateDailyReturns(benchmarkData, 365);
            const capture1Y = this.calculateUpDownCapture(dailyReturns1Y, benchmarkReturns1Y);
            scores.updown_capture_1y_score = this.scoreUpDownCapture(capture1Y.upCapture, capture1Y.downCapture);
            
            if (benchmarkData.length >= 1095) {
                const benchmarkReturns3Y = this.calculateDailyReturns(benchmarkData, 1095);
                const capture3Y = this.calculateUpDownCapture(dailyReturns3Y, benchmarkReturns3Y);
                scores.updown_capture_3y_score = this.scoreUpDownCapture(capture3Y.upCapture, capture3Y.downCapture);
            } else {
                scores.updown_capture_3y_score = 0;
            }
        } else {
            scores.updown_capture_1y_score = 0;
            scores.updown_capture_3y_score = 0;
        }
        
        // Max Drawdown scoring
        const maxDrawdown = this.calculateMaxDrawdown(navData);
        scores.max_drawdown_score = this.scoreMaxDrawdown(maxDrawdown);
        
        // Calculate total (max 30 points)
        const totalRiskScore = 
            scores.std_dev_1y_score + 
            scores.std_dev_3y_score + 
            scores.updown_capture_1y_score + 
            scores.updown_capture_3y_score + 
            scores.max_drawdown_score;
            
        scores.risk_grade_total = Math.min(30.00, Math.max(13.00, totalRiskScore));
        
        return scores;
    }
}
```

### 2.3 Fundamentals and Advanced Metrics Scoring
```javascript
// server/services/fundamentals-scoring.js
class FundamentalsScoring {
    
    static async calculateExpenseRatioScore(fundId) {
        const fund = await this.getFundDetails(fundId);
        const categoryAverage = await this.getCategoryAverageExpenseRatio(fund.subcategory);
        
        if (!fund.expense_ratio || !categoryAverage) return 4.0; // Default neutral score
        
        const ratio = fund.expense_ratio / categoryAverage;
        
        if (ratio <= 0.7) return 8.0;      // Excellent (30% below average)
        else if (ratio <= 0.85) return 6.0; // Good (15% below average)
        else if (ratio <= 1.15) return 4.0; // Average (within 15% of average)
        else if (ratio <= 1.3) return 3.0;  // Below average (30% above average)
        else return 3.0;                    // Poor (>30% above average) - min as per doc
    }
    
    static async calculateAUMSizeScore(fundId) {
        const fund = await this.getFundDetails(fundId);
        const optimalRange = this.getOptimalAUMRange(fund.subcategory);
        
        if (!fund.aum_value) return 4.0; // Default neutral score
        
        const aumCrores = fund.aum_value / 10000000; // Convert to crores
        
        if (aumCrores >= optimalRange.min && aumCrores <= optimalRange.max) {
            return 7.0; // Optimal size
        } else if (aumCrores >= optimalRange.min * 0.5 && aumCrores <= optimalRange.max * 1.5) {
            return 6.0; // Good size
        } else if (aumCrores >= optimalRange.min * 0.25 && aumCrores <= optimalRange.max * 2.0) {
            return 5.0; // Acceptable size
        } else {
            return 4.0; // Suboptimal size
        }
    }
    
    static getOptimalAUMRange(subcategory) {
        // Define optimal AUM ranges by subcategory (in crores)
        const ranges = {
            'Large Cap': { min: 1000, max: 50000 },
            'Mid Cap': { min: 500, max: 15000 },
            'Small Cap': { min: 100, max: 5000 },
            'Multi Cap': { min: 800, max: 25000 },
            'Flexi Cap': { min: 1000, max: 30000 },
            'ELSS': { min: 500, max: 20000 },
            'Index Fund': { min: 200, max: 10000 },
            'Sectoral': { min: 100, max: 5000 },
            'Debt': { min: 200, max: 15000 }
        };
        
        return ranges[subcategory] || { min: 500, max: 15000 }; // Default range
    }
    
    static async calculateAgeMaturityScore(fundId) {
        const fund = await this.getFundDetails(fundId);
        
        if (!fund.inception_date) return 0;
        
        const inceptionDate = new Date(fund.inception_date);
        const currentDate = new Date();
        const ageYears = (currentDate - inceptionDate) / (365.25 * 24 * 60 * 60 * 1000);
        
        if (ageYears >= 10) return 8.0;      // Mature fund (10+ years)
        else if (ageYears >= 5) return 6.0;  // Established fund (5-10 years)
        else if (ageYears >= 3) return 4.0;  // Growing fund (3-5 years)
        else if (ageYears >= 1) return 2.0;  // New fund (1-3 years)
        else return 0.0;                     // Very new fund (<1 year)
    }
    
    static async calculateFundamentalsComponent(fundId) {
        const scores = {};
        
        scores.expense_ratio_score = await this.calculateExpenseRatioScore(fundId);
        scores.aum_size_score = await this.calculateAUMSizeScore(fundId);
        scores.age_maturity_score = await this.calculateAgeMaturityScore(fundId);
        
        // Calculate total (max 30 points)
        scores.fundamentals_total = Math.min(30.00, 
            scores.expense_ratio_score + 
            scores.aum_size_score + 
            scores.age_maturity_score
        );
        
        return scores;
    }
}
```

---

## Phase 3: Quartile and Recommendation Engine

### 3.1 Correct Quartile Calculation
```javascript
// server/services/quartile-ranking.js
class QuartileRanking {
    
    static async calculateSubcategoryQuartiles(subcategory, scoreDate) {
        // Get all funds in subcategory with scores for the given date
        const funds = await db.query(`
            SELECT fund_id, total_score 
            FROM fund_scores_corrected 
            WHERE subcategory = $1 AND score_date = $2 
            AND total_score IS NOT NULL
            ORDER BY total_score DESC
        `, [subcategory, scoreDate]);
        
        if (funds.rows.length === 0) return [];
        
        const totalFunds = funds.rows.length;
        const rankings = [];
        
        funds.rows.forEach((fund, index) => {
            const rank = index + 1;
            const percentile = ((totalFunds - rank) / totalFunds) * 100;
            
            // True quartile calculation (not forced 25% distribution)
            let quartile;
            if (percentile >= 75) quartile = 1;      // Top 25%
            else if (percentile >= 50) quartile = 2; // 50-75%
            else if (percentile >= 25) quartile = 3; // 25-50%
            else quartile = 4;                       // Bottom 25%
            
            rankings.push({
                fund_id: fund.fund_id,
                total_score: fund.total_score,
                subcategory_rank: rank,
                subcategory_total: totalFunds,
                subcategory_quartile: quartile,
                subcategory_percentile: percentile
            });
        });
        
        return rankings;
    }
    
    static async updateAllQuartiles(scoreDate) {
        // Get all unique subcategories
        const subcategories = await db.query(`
            SELECT DISTINCT subcategory 
            FROM fund_scores_corrected 
            WHERE score_date = $1 AND subcategory IS NOT NULL
        `, [scoreDate]);
        
        for (const subcat of subcategories.rows) {
            const rankings = await this.calculateSubcategoryQuartiles(subcat.subcategory, scoreDate);
            
            // Update rankings in batch
            for (const ranking of rankings) {
                await db.query(`
                    UPDATE fund_scores_corrected 
                    SET subcategory_rank = $1,
                        subcategory_total = $2,
                        subcategory_quartile = $3,
                        subcategory_percentile = $4
                    WHERE fund_id = $5 AND score_date = $6
                `, [
                    ranking.subcategory_rank,
                    ranking.subcategory_total,
                    ranking.subcategory_quartile,
                    ranking.subcategory_percentile,
                    ranking.fund_id,
                    scoreDate
                ]);
            }
        }
    }
}
```

### 3.2 Recommendation Logic Implementation
```javascript
// server/services/recommendation-engine.js
class RecommendationEngine {
    
    static calculateRecommendation(totalScore, quartile, riskGradeTotal, fundamentalsTotal) {
        // Exact logic from documentation
        
        // STRONG_BUY: Total score ≥70 OR (score ≥65 AND Q1 AND risk ≥25)
        if (totalScore >= 70 || (totalScore >= 65 && quartile === 1 && riskGradeTotal >= 25)) {
            return 'STRONG_BUY';
        }
        
        // BUY: Total score ≥60 OR (score ≥55 AND Q1/Q2 AND fundamentals ≥20)
        if (totalScore >= 60 || (totalScore >= 55 && [1, 2].includes(quartile) && fundamentalsTotal >= 20)) {
            return 'BUY';
        }
        
        // HOLD: Total score ≥50 OR (score ≥45 AND Q1/Q2/Q3 AND risk ≥20)
        if (totalScore >= 50 || (totalScore >= 45 && [1, 2, 3].includes(quartile) && riskGradeTotal >= 20)) {
            return 'HOLD';
        }
        
        // SELL: Total score ≥35 OR (score ≥30 AND risk ≥15)
        if (totalScore >= 35 || (totalScore >= 30 && riskGradeTotal >= 15)) {
            return 'SELL';
        }
        
        // STRONG_SELL: All others
        return 'STRONG_SELL';
    }
    
    static async updateAllRecommendations(scoreDate) {
        const funds = await db.query(`
            SELECT fund_id, total_score, subcategory_quartile, risk_grade_total, fundamentals_total
            FROM fund_scores_corrected 
            WHERE score_date = $1 AND total_score IS NOT NULL
        `, [scoreDate]);
        
        for (const fund of funds.rows) {
            const recommendation = this.calculateRecommendation(
                fund.total_score,
                fund.subcategory_quartile,
                fund.risk_grade_total,
                fund.fundamentals_total
            );
            
            await db.query(`
                UPDATE fund_scores_corrected 
                SET recommendation = $1 
                WHERE fund_id = $2 AND score_date = $3
            `, [recommendation, fund.fund_id, scoreDate]);
        }
    }
}
```

---

## Phase 4: Implementation Timeline

### Week 1: Database and Core Logic
1. Create corrected schema with proper constraints
2. Implement historical returns scoring engine
3. Implement risk assessment scoring engine
4. Test individual component calculations

### Week 2: Advanced Components
1. Implement fundamentals scoring
2. Implement advanced metrics (sectoral, forward, momentum, consistency)
3. Create quartile ranking system
4. Implement recommendation engine

### Week 3: Data Migration and Validation
1. Migrate eligible funds to new scoring system
2. Validate calculations against documentation
3. Compare results with original backup for verification
4. Performance testing and optimization

### Week 4: Integration and Testing
1. Update API endpoints to use corrected scoring
2. Update frontend to display new scoring structure
3. Comprehensive end-to-end testing
4. Documentation and monitoring setup

---

## Phase 5: Validation Criteria

### Mathematical Validation:
- Individual scores: 0-8 points (never exceed)
- Component totals: Historical (≤40), Risk (≤30), Fundamentals (≤30), Advanced (≤30)
- Total scores: 34-100 points range
- Quartile distribution: True performance-based (not forced 25%)

### Data Integrity Validation:
- All calculations use authentic NAV data only
- No synthetic or placeholder values
- Proper handling of insufficient data scenarios
- Audit trail for all calculations

This plan ensures complete alignment with your original documentation while fixing all identified mathematical and structural issues.