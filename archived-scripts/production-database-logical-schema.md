# Production Database Logical Schema
## Mutual Fund Analysis Platform - June 2025

## Core Entity Relationships

### 1. FUNDS (Master Entity) - 16,766 records
```
Primary Key: id (integer, auto-increment)
Unique Key: scheme_code (text)

Core Attributes:
- fund_name (text, required)
- amc_name (text, required) 
- category (text, required)
- subcategory (text, optional)
- benchmark_name (text, optional)
- fund_manager (text, optional)
- inception_date (date, optional)
- expense_ratio (numeric 4,2, optional)
- minimum_investment (integer, optional)
- exit_load (numeric 4,2, optional)
- lock_in_period (integer, optional)
- aum_crores (numeric 12,2, optional)
- fund_age_years (numeric 6,2, optional)
- status (text, default: 'ACTIVE')

Timestamps: created_at, updated_at
```

### 2. NAV_DATA (Time Series) - 20M+ records, 1.7GB
```
Composite Primary Key: (fund_id, nav_date)
Foreign Key: fund_id → funds(id)

Core Attributes:
- nav_date (date, required)
- nav_value (numeric 12,4, required)
- nav_change (numeric 12,4, optional)
- nav_change_pct (numeric 8,4, optional)
- aum_cr (numeric 15,2, optional)

Timestamps: created_at
Indexes: fund_id, nav_date
```

### 3. FUND_SCORES_CORRECTED (Primary Scoring) - 11,800 records, 11MB
```
Composite Primary Key: (fund_id, score_date)
Foreign Key: fund_id → funds(id)

Score Components (User-Defined Methodology):
- return_3m_score → return_5y_score (numeric 4,2)
- historical_returns_total (numeric 5,2, max 40 points)
- std_dev_1y_score → max_drawdown_score (numeric 4,2)  
- risk_grade_total (numeric 5,2, max 30 points)
- expense_ratio_score → consistency_score (numeric 4,2)
- fundamentals_total (numeric 5,2, max 15 points)
- sectoral_similarity_score → momentum_score (numeric 4,2)
- other_metrics_total (numeric 5,2, max 15 points)

Final Results:
- total_score (numeric 6,2, range: 25.80-76.00)
- quartile (integer, 1-4)
- recommendation (text: STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL)

Category Analysis:
- category (varchar 50)
- subcategory (varchar 100)
- subcategory_rank, subcategory_total (integer)
- subcategory_quartile (integer, 1-4)
- subcategory_percentile (numeric 5,2)

Advanced Risk Metrics:
- calmar_ratio_1y, sortino_ratio_1y (numeric)
- var_95_1y, downside_deviation_1y (numeric)
- volatility_1y_percent, tracking_error_1y (numeric 10,4)
- alpha, beta, sharpe_ratio, information_ratio (numeric)

Absolute Returns:
- return_3m_absolute → return_5y_absolute (numeric 10,4)
```

### 4. FUND_PERFORMANCE_METRICS (Auxiliary Calculations) - 40MB
```
Primary Key: id (integer)
Foreign Key: fund_id → funds(id)
Unique Key: (fund_id, calculation_date)

Performance Calculations:
- returns_1y, returns_3y, returns_5y (numeric 8,4)
- volatility, max_drawdown (numeric 8,4)
- sharpe_ratio, alpha, beta, information_ratio (numeric 8,4)
- consistency_score (numeric 6,4)
- total_nav_records (integer, required)
- data_quality_score (numeric 6,4, required)
- composite_score (numeric 8,4, required)

Extended Scoring Fields (56 columns total):
- Duplicates some fund_scores_corrected fields for compatibility
- Includes additional momentum, age_maturity calculations
```

## Supporting Entities

### 5. QUARTILE_RANKINGS (Category Analysis) - 5.3MB
```
Primary Key: id (integer)
Foreign Key: fund_id → funds(id)
Unique Key: (fund_id, category, calculation_date)

Ranking Attributes:
- category (text, required)
- calculation_date (timestamp, required)
- quartile (integer 1-4, required)
- quartile_label (text: BUY/HOLD/REVIEW/SELL)
- rank, total_funds (integer, required)
- percentile (numeric 6,4, required)
- composite_score (numeric 8,4, required)
```

### 6. PERFORMANCE_ATTRIBUTION (Benchmark Analysis) - 2.5MB
```
Primary Key: id (integer)
Foreign Key: fund_id → funds(id) 
Unique Key: (fund_id, attribution_date)

Attribution Metrics:
- category (varchar 100, required)
- benchmark_name (varchar 200)
- fund_return_1y, category_avg_return_1y (numeric 10,4)
- category_outperformance_1y (numeric 10,4)
- category_percentile_1y (numeric 5,2)
- fund_return_3y, category_avg_return_3y (numeric 10,4)
- category_outperformance_3y (numeric 10,4)
- category_percentile_3y (numeric 5,2)
- fund_sharpe_ratio, category_avg_sharpe (numeric 8,4)
- sharpe_outperformance (numeric 8,4)
- fund_alpha, fund_beta (numeric 8,4)
- attribution_score (numeric 5,2)
- attribution_grade (varchar 2)
```

### 7. RISK_ANALYTICS (Advanced Risk) - 2MB
```
Primary Key: id (integer)
Foreign Key: fund_id → funds(id)

Advanced Risk Calculations:
- 23 columns of sophisticated risk metrics
- Volatility measures across multiple timeframes
- Drawdown analysis and recovery metrics
- Correlation and beta calculations
```

## Validation & Quality Assurance

### 8. BACKTESTING_RESULTS (Validation) - 40KB
```
Primary Key: id (integer)
Foreign Key: fund_id → funds(id)
Unique Key: (fund_id, validation_date, historical_score_date)

Validation Metrics:
- historical_total_score (numeric 10,2)
- historical_recommendation (varchar 20)
- historical_quartile (integer)
- actual_return_3m, actual_return_6m, actual_return_1y (numeric 10,4)
- predicted_performance, actual_performance (varchar 20)
- prediction_accuracy, quartile_maintained (boolean)
- score_accuracy_3m, score_accuracy_6m, score_accuracy_1y (numeric 5,2)
- quartile_accuracy_score (numeric 5,2)
```

### 9. VALIDATION_SUMMARY_REPORTS (System Health) - 48KB
```
Primary Key: id (integer)
Unique Key: validation_run_id (text)

Summary Metrics:
- total_funds_tested, validation_period_months (integer)
- overall_prediction_accuracy_3m → 1y (numeric 5,2)
- overall_score_correlation_3m → 1y (numeric 5,2)
- quartile_stability_3m → 1y (numeric 5,2)
- strong_buy_accuracy → strong_sell_accuracy (numeric 5,2)
- validation_status (text)
```

## Market Data & Framework

### 10. MARKET_INDICES (Benchmark Data) - 32KB
```
Composite Primary Key: (index_name, index_date)

Market Metrics:
- open_value, high_value, low_value, close_value (numeric 12,2)
- volume (integer)
- market_cap (numeric 18,2)
- pe_ratio, pb_ratio (numeric 6,2)
- dividend_yield (numeric 4,2)
```

### 11. ELIVATE_SCORES (Market Framework) - 48KB
```
Primary Key: id (integer)
Unique Key: score_date (date)

Framework Components (100-point system):
External Influence (20 points):
- us_gdp_growth, fed_funds_rate, dxy_index, china_pmi

Local Story (20 points):
- india_gdp_growth, gst_collection_cr, iip_growth, india_pmi

Inflation & Rates (20 points):
- cpi_inflation, wpi_inflation, repo_rate, ten_year_yield

Valuation & Earnings (20 points):
- nifty_pe, nifty_pb, earnings_growth

Allocation of Capital (10 points):
- fii_flows_cr, dii_flows_cr, sip_inflows_cr

Trends & Sentiments (10 points):
- stocks_above_200dma_pct, india_vix, advance_decline_ratio

Final Results:
- total_elivate_score (numeric 5,1, required)
- market_stance (text, required)
```

## Data Pipeline & Monitoring

### 12. ETL_PIPELINE_RUNS (Process Monitoring) - 336KB
```
Primary Key: id (integer)

Pipeline Tracking:
- pipeline_name (text, required)
- status (text, required)
- start_time, end_time (timestamp)
- records_processed (integer)
- error_message (text)
```

## Key Relationships & Constraints

### Primary Relationships:
1. **funds** ←→ **nav_data** (1:many)
2. **funds** ←→ **fund_scores_corrected** (1:many by date)
3. **funds** ←→ **fund_performance_metrics** (1:many by date)
4. **funds** ←→ **quartile_rankings** (1:many by category/date)
5. **funds** ←→ **performance_attribution** (1:many by date)
6. **funds** ←→ **backtesting_results** (1:many)

### Data Integrity Rules:
- All scoring uses authentic NAV data only
- No synthetic data generation anywhere in system
- Composite scores follow exact user-defined methodology
- Score range: 25.80-76.00 points (validated)
- Quartile distribution maintained across categories

### Active vs Inactive Tables:
**ACTIVE PRODUCTION:** funds, nav_data, fund_scores_corrected, fund_performance_metrics, quartile_rankings, performance_attribution, risk_analytics, backtesting_results, validation_summary_reports, market_indices, elivate_scores, etl_pipeline_runs

**BACKUP/LEGACY:** nav_data_backup, fund_scores_backup, fund_scores (deprecated)

This schema represents the current production state with 11,800 actively scored funds using authentic AMFI data across 20+ million NAV records.