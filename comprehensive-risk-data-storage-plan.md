# Comprehensive Risk Data Storage Plan

## Current Storage Status Analysis

### ✅ Currently Stored (Basic Scores):
- `std_dev_1y_score` - Volatility score (5 points max)
- `std_dev_3y_score` - 3-year volatility score (5 points max)
- `max_drawdown_score` - Drawdown score (4 points max)
- `updown_capture_1y_score` - Up/down capture score (8 points max)
- `updown_capture_3y_score` - 3-year capture score (8 points max)
- `risk_grade_total` - Total risk score (30 points max)

### ❌ Missing Critical Raw Data:
- Actual volatility percentages (1Y, 3Y)
- Actual maximum drawdown percentage
- Daily returns arrays
- Peak values and drawdown periods
- Up/down market performance ratios
- Risk-free rate comparisons
- Beta calculations vs benchmarks

## Enhanced Database Schema Plan

### Phase 1A: Extend fund_scores Table
```sql
ALTER TABLE fund_scores ADD COLUMN IF NOT EXISTS
-- Raw Volatility Data
volatility_1y_percent NUMERIC(8,4),           -- Actual 1Y volatility %
volatility_3y_percent NUMERIC(8,4),           -- Actual 3Y volatility %
volatility_calculation_date DATE,             -- When calculated

-- Drawdown Analysis
max_drawdown_percent NUMERIC(8,4),            -- Actual max drawdown %
max_drawdown_start_date DATE,                 -- Peak date before drawdown
max_drawdown_end_date DATE,                   -- Valley date of drawdown
current_drawdown_percent NUMERIC(8,4),        -- Current drawdown from peak

-- Performance Ratios
up_capture_ratio_1y NUMERIC(8,4),             -- Actual up market capture
down_capture_ratio_1y NUMERIC(8,4),           -- Actual down market capture
up_capture_ratio_3y NUMERIC(8,4),             -- 3Y up market capture
down_capture_ratio_3y NUMERIC(8,4),           -- 3Y down market capture

-- Beta and Correlation
beta_1y NUMERIC(8,4),                         -- 1Y beta vs benchmark
beta_3y NUMERIC(8,4),                         -- 3Y beta vs benchmark
correlation_1y NUMERIC(8,4),                  -- 1Y correlation
correlation_3y NUMERIC(8,4),                  -- 3Y correlation

-- Sharpe and Risk Metrics
sharpe_ratio_1y NUMERIC(8,4),                 -- Risk-adjusted returns
sharpe_ratio_3y NUMERIC(8,4),                 -- 3Y Sharpe ratio
information_ratio_1y NUMERIC(8,4),            -- vs benchmark
information_ratio_3y NUMERIC(8,4),            -- 3Y information ratio

-- Statistical Data
return_skewness_1y NUMERIC(8,4),              -- Return distribution skew
return_kurtosis_1y NUMERIC(8,4),              -- Return distribution kurtosis
var_95_1y NUMERIC(8,4),                       -- Value at Risk 95%
cvar_95_1y NUMERIC(8,4);                      -- Conditional VaR 95%
```

### Phase 1B: Create Risk Analytics Table
```sql
CREATE TABLE risk_analytics (
  id SERIAL PRIMARY KEY,
  fund_id INTEGER REFERENCES funds(id),
  calculation_date DATE NOT NULL,
  
  -- Daily Returns Statistics
  daily_returns_mean NUMERIC(10,6),
  daily_returns_std NUMERIC(10,6),
  daily_returns_count INTEGER,
  
  -- Monthly Returns Statistics  
  monthly_returns_mean NUMERIC(8,4),
  monthly_returns_std NUMERIC(8,4),
  monthly_returns_count INTEGER,
  
  -- Rolling Metrics
  rolling_volatility_3m NUMERIC(8,4),
  rolling_volatility_6m NUMERIC(8,4),
  rolling_volatility_12m NUMERIC(8,4),
  rolling_volatility_24m NUMERIC(8,4),
  rolling_volatility_36m NUMERIC(8,4),
  
  -- Drawdown Series
  max_drawdown_duration_days INTEGER,
  avg_drawdown_duration_days NUMERIC(8,2),
  drawdown_frequency_per_year NUMERIC(6,2),
  
  -- Performance Consistency
  positive_months_percentage NUMERIC(6,2),
  negative_months_percentage NUMERIC(6,2),
  months_beating_benchmark NUMERIC(6,2),
  
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(fund_id, calculation_date)
);
```

### Phase 1C: Create Benchmark Comparison Table
```sql
CREATE TABLE benchmark_analysis (
  id SERIAL PRIMARY KEY,
  fund_id INTEGER REFERENCES funds(id),
  benchmark_name VARCHAR(100),           -- NIFTY50, SENSEX, etc.
  calculation_date DATE NOT NULL,
  
  -- Relative Performance
  excess_return_1y NUMERIC(8,4),         -- Fund return - benchmark return
  excess_return_3y NUMERIC(8,4),
  tracking_error_1y NUMERIC(8,4),        -- Volatility of excess returns
  tracking_error_3y NUMERIC(8,4),
  
  -- Market Timing
  up_market_capture NUMERIC(8,4),        -- Performance in rising markets
  down_market_capture NUMERIC(8,4),      -- Performance in falling markets
  market_timing_ratio NUMERIC(8,4),      -- Up capture / Down capture
  
  -- Risk Adjusted
  treynor_ratio_1y NUMERIC(8,4),         -- Excess return / Beta
  treynor_ratio_3y NUMERIC(8,4),
  jensen_alpha_1y NUMERIC(8,4),          -- CAPM alpha
  jensen_alpha_3y NUMERIC(8,4),
  
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(fund_id, benchmark_name, calculation_date)
);
```

## Implementation Strategy

### Step 1: Extend Current Implementation
```javascript
// Enhanced calculateAdvancedRiskMetrics function
async function calculateComprehensiveRiskMetrics(fundId) {
  const navData = await getAuthenticNavData(fundId);
  
  // Calculate all raw metrics
  const volatilityData = calculateDetailedVolatility(navData);
  const drawdownAnalysis = calculateComprehensiveDrawdown(navData);
  const performanceRatios = calculateDetailedPerformanceRatios(navData);
  const statisticalMetrics = calculateStatisticalMetrics(navData);
  
  // Store comprehensive data
  await storeComprehensiveRiskData(fundId, {
    volatilityData,
    drawdownAnalysis,
    performanceRatios,
    statisticalMetrics
  });
}
```

### Step 2: Raw Data Calculations

#### Detailed Volatility Analysis:
```javascript
function calculateDetailedVolatility(navData) {
  const dailyReturns = calculateDailyReturns(navData);
  
  return {
    volatility_1y_percent: calculateAnnualizedVolatility(dailyReturns.slice(-252)),
    volatility_3y_percent: calculateAnnualizedVolatility(dailyReturns.slice(-756)),
    rolling_volatility_3m: calculateRollingVolatility(dailyReturns, 63),
    rolling_volatility_6m: calculateRollingVolatility(dailyReturns, 126),
    rolling_volatility_12m: calculateRollingVolatility(dailyReturns, 252),
    return_skewness: calculateSkewness(dailyReturns),
    return_kurtosis: calculateKurtosis(dailyReturns)
  };
}
```

#### Comprehensive Drawdown Analysis:
```javascript
function calculateComprehensiveDrawdown(navData) {
  const drawdowns = [];
  let peak = navData[0].value;
  let peakDate = navData[0].date;
  let currentDrawdown = 0;
  
  for (const point of navData) {
    if (point.value > peak) {
      peak = point.value;
      peakDate = point.date;
    }
    
    const drawdown = (peak - point.value) / peak * 100;
    if (drawdown > 0) {
      drawdowns.push({
        date: point.date,
        drawdown: drawdown,
        peakDate: peakDate,
        duration: calculateDaysBetween(peakDate, point.date)
      });
    }
  }
  
  return {
    max_drawdown_percent: Math.max(...drawdowns.map(d => d.drawdown)),
    max_drawdown_start_date: drawdowns.find(d => d.drawdown === Math.max(...drawdowns.map(dd => dd.drawdown)))?.peakDate,
    max_drawdown_duration_days: Math.max(...drawdowns.map(d => d.duration)),
    avg_drawdown_duration: drawdowns.reduce((sum, d) => sum + d.duration, 0) / drawdowns.length,
    drawdown_frequency_per_year: (drawdowns.length / (navData.length / 252))
  };
}
```

### Step 3: Benchmark Integration
```javascript
async function calculateBenchmarkMetrics(fundId, benchmarkData) {
  const fundReturns = await getFundReturns(fundId);
  const benchmarkReturns = getBenchmarkReturns(benchmarkData);
  
  return {
    beta_1y: calculateBeta(fundReturns.slice(-252), benchmarkReturns.slice(-252)),
    correlation_1y: calculateCorrelation(fundReturns.slice(-252), benchmarkReturns.slice(-252)),
    up_capture_ratio: calculateUpCapture(fundReturns, benchmarkReturns),
    down_capture_ratio: calculateDownCapture(fundReturns, benchmarkReturns),
    tracking_error_1y: calculateTrackingError(fundReturns, benchmarkReturns),
    sharpe_ratio_1y: calculateSharpeRatio(fundReturns, riskFreeRate),
    jensen_alpha_1y: calculateJensenAlpha(fundReturns, benchmarkReturns, riskFreeRate)
  };
}
```

## Data Storage Implementation

### Enhanced Storage Function:
```javascript
async function storeComprehensiveRiskData(fundId, metrics) {
  // Update fund_scores with raw data
  await pool.query(`
    UPDATE fund_scores 
    SET 
      volatility_1y_percent = $1,
      volatility_3y_percent = $2,
      max_drawdown_percent = $3,
      max_drawdown_start_date = $4,
      max_drawdown_end_date = $5,
      up_capture_ratio_1y = $6,
      down_capture_ratio_1y = $7,
      sharpe_ratio_1y = $8,
      beta_1y = $9,
      correlation_1y = $10
    WHERE fund_id = $11 AND score_date = CURRENT_DATE
  `, [
    metrics.volatility_1y_percent,
    metrics.volatility_3y_percent,
    metrics.max_drawdown_percent,
    // ... all other metrics
  ]);
  
  // Insert detailed analytics
  await pool.query(`
    INSERT INTO risk_analytics (
      fund_id, calculation_date,
      daily_returns_mean, daily_returns_std,
      rolling_volatility_3m, rolling_volatility_6m,
      drawdown_frequency_per_year, positive_months_percentage
    ) VALUES ($1, CURRENT_DATE, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
      daily_returns_mean = EXCLUDED.daily_returns_mean,
      -- ... update all fields
  `, [fundId, ...metrics]);
}
```

## Data Validation Framework

### Validation Queries:
```sql
-- Validate data completeness
SELECT 
  COUNT(*) as total_funds,
  COUNT(volatility_1y_percent) as has_volatility_raw,
  COUNT(max_drawdown_percent) as has_drawdown_raw,
  COUNT(sharpe_ratio_1y) as has_sharpe_ratio
FROM fund_scores 
WHERE score_date = CURRENT_DATE;

-- Validate data quality
SELECT fund_id, volatility_1y_percent, max_drawdown_percent
FROM fund_scores
WHERE score_date = CURRENT_DATE 
  AND (volatility_1y_percent < 0 OR volatility_1y_percent > 200
       OR max_drawdown_percent < 0 OR max_drawdown_percent > 100);
```

## Timeline for Implementation

### Week 1: Schema Enhancement
- Extend fund_scores table with raw metrics columns
- Create risk_analytics table
- Create benchmark_analysis table

### Week 2: Enhanced Calculations
- Implement detailed volatility calculations
- Add comprehensive drawdown analysis
- Create statistical metrics functions

### Week 3: Benchmark Integration
- Source benchmark data (NIFTY, SENSEX)
- Implement beta and correlation calculations
- Add Sharpe ratio and alpha calculations

### Week 4: Data Migration
- Process existing 49 funds with enhanced metrics
- Validate all stored data
- Create monitoring and alerting

This plan ensures all calculated risk metrics are permanently stored in your database for analysis, reporting, and historical tracking.