# Complete Database Fields Documentation
## Production Tables - Field Specifications

## CORE PRODUCTION TABLES

### 1. FUNDS (Master Entity) - 22 Fields
```sql
id                    integer(32,0)     PRIMARY KEY, AUTO INCREMENT
scheme_code          text              UNIQUE, NOT NULL
isin_div_payout      text              NULLABLE
isin_div_reinvest    text              NULLABLE  
fund_name            text              NOT NULL
amc_name             text              NOT NULL
category             text              NOT NULL
subcategory          text              NULLABLE
benchmark_name       text              NULLABLE
fund_manager         text              NULLABLE
inception_date       date              NULLABLE
status               text              DEFAULT 'ACTIVE'
minimum_investment   integer(32,0)     NULLABLE
minimum_additional   integer(32,0)     NULLABLE
exit_load            numeric(4,2)      NULLABLE
lock_in_period       integer(32,0)     NULLABLE
expense_ratio        numeric(4,2)      NULLABLE
created_at          timestamp         DEFAULT now()
updated_at          timestamp         DEFAULT now()
aum_crores          numeric(12,2)     NULLABLE
fund_age_years      numeric(6,2)      NULLABLE
benchmark_index     varchar(100)      NULLABLE
```

### 2. NAV_DATA (Time Series) - 7 Fields
```sql
fund_id             integer(32,0)     FOREIGN KEY → funds(id), UNIQUE COMPOSITE
nav_date            date              NOT NULL, UNIQUE COMPOSITE
nav_value           numeric(12,4)     NOT NULL
nav_change          numeric(12,4)     NULLABLE
nav_change_pct      numeric(8,4)      NULLABLE
aum_cr              numeric(15,2)     NULLABLE
created_at          timestamp         DEFAULT now()
```

### 3. FUND_SCORES_CORRECTED (Primary Scoring) - 72 Fields
```sql
-- Primary Keys
fund_id                          integer(32,0)     PRIMARY KEY
score_date                       date              PRIMARY KEY, DEFAULT CURRENT_DATE
created_at                       timestamp         DEFAULT now()

-- Return Score Components (User-Defined Methodology)
return_3m_score                  numeric(4,2)      NULLABLE
return_6m_score                  numeric(4,2)      NULLABLE
return_1y_score                  numeric(4,2)      NULLABLE
return_3y_score                  numeric(4,2)      NULLABLE
return_5y_score                  numeric(4,2)      NULLABLE
historical_returns_total         numeric(5,2)      NULLABLE (Max 40 points)

-- Risk Grade Components
std_dev_1y_score                numeric(4,2)      NULLABLE
std_dev_3y_score                numeric(4,2)      NULLABLE
updown_capture_1y_score         numeric(4,2)      NULLABLE
updown_capture_3y_score         numeric(4,2)      NULLABLE
max_drawdown_score              numeric(4,2)      NULLABLE
risk_grade_total                numeric(5,2)      NULLABLE (Max 30 points)

-- Fundamentals Components
expense_ratio_score             numeric(4,2)      NULLABLE
aum_size_score                  numeric(4,2)      NULLABLE
age_maturity_score              numeric(4,2)      NULLABLE
fundamentals_total              numeric(5,2)      NULLABLE (Max 15 points)

-- Other Metrics Components
sectoral_similarity_score       numeric(4,2)      NULLABLE
forward_score                   numeric(4,2)      NULLABLE
momentum_score                  numeric(4,2)      NULLABLE
consistency_score               numeric(4,2)      NULLABLE
other_metrics_total             numeric(5,2)      NULLABLE (Max 15 points)

-- Final Scoring Results
total_score                     numeric(6,2)      NULLABLE (Range: 25.80-76.00)
quartile                        integer(32,0)     NULLABLE (1-4)
category_rank                   integer(32,0)     NULLABLE
category_total                  integer(32,0)     NULLABLE
recommendation                  text              NULLABLE (STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL)

-- Category & Subcategory Analysis
category                        varchar(50)       NULLABLE
subcategory                     varchar(100)      NULLABLE
subcategory_rank                integer(32,0)     NULLABLE
subcategory_total               integer(32,0)     NULLABLE
subcategory_quartile            integer(32,0)     NULLABLE (1-4)
subcategory_percentile          numeric(5,2)      NULLABLE

-- Advanced Risk Metrics
calmar_ratio_1y                 numeric           NULLABLE
sortino_ratio_1y                numeric           NULLABLE
downside_deviation_1y           numeric           NULLABLE
var_95_1y                       numeric           NULLABLE
tracking_error_1y               numeric(10,4)     NULLABLE

-- Rolling Volatility Analysis
rolling_volatility_3m           numeric           NULLABLE
rolling_volatility_6m           numeric           NULLABLE
rolling_volatility_12m          numeric           NULLABLE
rolling_volatility_24m          numeric           NULLABLE
rolling_volatility_36m          numeric           NULLABLE
volatility_1y_percent           numeric           NULLABLE
volatility_3y_percent           numeric           NULLABLE

-- Performance Distribution Analysis
positive_months_percentage      numeric           NULLABLE
negative_months_percentage      numeric           NULLABLE
consecutive_positive_months_max integer(32,0)     NULLABLE
consecutive_negative_months_max integer(32,0)     NULLABLE

-- Drawdown Analysis
max_drawdown                    numeric           NULLABLE
max_drawdown_duration_days      integer(32,0)     NULLABLE
avg_drawdown_duration_days      numeric           NULLABLE
drawdown_frequency_per_year     numeric           NULLABLE
recovery_time_avg_days          numeric           NULLABLE

-- Daily Returns Statistics
daily_returns_count             integer(32,0)     NULLABLE
daily_returns_mean              numeric           NULLABLE
daily_returns_std               numeric           NULLABLE

-- Core Financial Ratios
alpha                           numeric           NULLABLE
beta                            numeric           NULLABLE
sharpe_ratio                    numeric           NULLABLE
information_ratio               numeric           NULLABLE
volatility                      numeric           NULLABLE
correlation_1y                  numeric           NULLABLE

-- Rating & Recommendation System
overall_rating                  integer(32,0)     NULLABLE
recommendation_text             text              NULLABLE

-- Absolute Return Values
return_3m_absolute              numeric(10,4)     NULLABLE
return_6m_absolute              numeric(10,4)     NULLABLE
return_1y_absolute              numeric(10,4)     NULLABLE
return_3y_absolute              numeric(10,4)     NULLABLE
return_5y_absolute              numeric(10,4)     NULLABLE
```

### 4. FUND_PERFORMANCE_METRICS (Auxiliary Calculations) - 56 Fields
```sql
-- Primary Identifiers
id                              integer(32,0)     PRIMARY KEY
fund_id                         integer(32,0)     FOREIGN KEY → funds(id)
calculation_date                timestamp         UNIQUE, NOT NULL
created_at                      timestamp         DEFAULT now()

-- Core Performance Metrics
returns_1y                      numeric(8,4)      NULLABLE
returns_3y                      numeric(8,4)      NULLABLE
returns_5y                      numeric(8,4)      NULLABLE
returns_3m                      numeric           NULLABLE
returns_6m                      numeric           NULLABLE
returns_ytd                     numeric           NULLABLE

-- Risk Metrics
volatility                      numeric(8,4)      NULLABLE
sharpe_ratio                    numeric(8,4)      NULLABLE
max_drawdown                    numeric(8,4)      NULLABLE
alpha                           numeric(8,4)      NULLABLE
beta                            numeric(8,4)      NULLABLE
information_ratio               numeric(8,4)      NULLABLE

-- Quality & Validation Metrics
consistency_score               numeric(6,4)      NULLABLE
total_nav_records               integer(32,0)     NOT NULL
data_quality_score              numeric(6,4)      NOT NULL
composite_score                 numeric(8,4)      NOT NULL

-- Scoring System Fields (Overlapping with fund_scores_corrected)
total_score                     numeric           NULLABLE
quartile                        integer(32,0)     NULLABLE
subcategory_quartile            integer(32,0)     NULLABLE
overall_rating                  integer(32,0)     NULLABLE
recommendation                  text              NULLABLE
scoring_date                    date              NULLABLE

-- Individual Score Components
return_3m_score                 numeric           NULLABLE
return_6m_score                 numeric           NULLABLE
return_1y_score                 numeric           NULLABLE
return_3y_score                 numeric           NULLABLE
return_5y_score                 numeric           NULLABLE
return_ytd_score                numeric           NULLABLE
historical_returns_total        numeric           NULLABLE

-- Risk Score Components
std_dev_1y_score               numeric           NULLABLE
std_dev_3y_score               numeric           NULLABLE
updown_capture_1y_score        numeric           NULLABLE
updown_capture_3y_score        numeric           NULLABLE
max_drawdown_score             numeric           NULLABLE
risk_grade_total               numeric           NULLABLE

-- Fundamental Analysis Scores
sectoral_similarity_score      numeric           NULLABLE
forward_score                  numeric           NULLABLE
aum_size_score                 numeric           NULLABLE
expense_ratio_score            numeric           NULLABLE
momentum_score                 numeric           NULLABLE
age_maturity_score             numeric           NULLABLE
sharpe_ratio_score             integer(32,0)     NULLABLE
beta_score                     integer(32,0)     NULLABLE

-- Total Score Categories
other_metrics_total            numeric           NULLABLE
fundamentals_total             numeric           NULLABLE

-- Category Analysis
subcategory                    varchar(100)      NULLABLE
subcategory_percentile         numeric           NULLABLE
subcategory_rank               integer(32,0)     NULLABLE
subcategory_total              integer(32,0)     NULLABLE
category_rank                  integer(32,0)     NULLABLE
category_total                 integer(32,0)     NULLABLE
category_total_funds           integer(32,0)     NULLABLE
```

## ACTIVE SUPPORTING SYSTEMS

### 5. QUARTILE_RANKINGS (Category Analysis) - 11 Fields
```sql
id                  integer(32,0)     PRIMARY KEY
fund_id             integer(32,0)     FOREIGN KEY → funds(id), UNIQUE COMPOSITE
category            text              NOT NULL, UNIQUE COMPOSITE
calculation_date    timestamp         NOT NULL, UNIQUE COMPOSITE
quartile            integer(32,0)     NOT NULL (1-4)
quartile_label      text              NOT NULL (BUY/HOLD/REVIEW/SELL)
rank                integer(32,0)     NOT NULL
total_funds         integer(32,0)     NOT NULL
percentile          numeric(6,4)      NOT NULL
composite_score     numeric(8,4)      NOT NULL
created_at          timestamp         DEFAULT now()
```

### 6. PERFORMANCE_ATTRIBUTION (Benchmark Analysis) - 21 Fields
```sql
id                              integer(32,0)     PRIMARY KEY
fund_id                         integer(32,0)     UNIQUE
attribution_date                date              UNIQUE
category                        varchar(100)      NOT NULL
benchmark_name                  varchar(200)      NULLABLE

-- 1-Year Performance Attribution
fund_return_1y                  numeric(10,4)     NULLABLE
category_avg_return_1y          numeric(10,4)     NULLABLE
category_outperformance_1y      numeric(10,4)     NULLABLE
category_percentile_1y          numeric(5,2)      NULLABLE

-- 3-Year Performance Attribution
fund_return_3y                  numeric(10,4)     NULLABLE
category_avg_return_3y          numeric(10,4)     NULLABLE
category_outperformance_3y      numeric(10,4)     NULLABLE
category_percentile_3y          numeric(5,2)      NULLABLE

-- Risk-Adjusted Performance
fund_sharpe_ratio              numeric(8,4)      NULLABLE
category_avg_sharpe            numeric(8,4)      NULLABLE
sharpe_outperformance          numeric(8,4)      NULLABLE

-- Alpha & Beta Analysis
fund_alpha                     numeric(8,4)      NULLABLE
fund_beta                      numeric(8,4)      NULLABLE

-- Attribution Summary
attribution_score              numeric(5,2)      NULLABLE
attribution_grade              varchar(2)        NULLABLE

created_at                     timestamp         DEFAULT CURRENT_TIMESTAMP
```

### 7. RISK_ANALYTICS (Advanced Risk) - 23 Fields
```sql
id                              integer(32,0)     PRIMARY KEY
fund_id                         integer(32,0)     FOREIGN KEY → funds(id), UNIQUE
calculation_date                timestamp         UNIQUE
fund_name                       text              NULLABLE
category                        varchar(100)      NULLABLE

-- Core Risk Metrics
volatility_1y                   numeric(8,4)      NULLABLE
volatility_3y                   numeric(8,4)      NULLABLE
max_drawdown_1y                 numeric(8,4)      NULLABLE
max_drawdown_3y                 numeric(8,4)      NULLABLE

-- Advanced Risk Ratios
sharpe_ratio_1y                 numeric(8,4)      NULLABLE
sharpe_ratio_3y                 numeric(8,4)      NULLABLE
sortino_ratio_1y                numeric(8,4)      NULLABLE
sortino_ratio_3y                numeric(8,4)      NULLABLE
calmar_ratio_1y                 numeric(8,4)      NULLABLE
calmar_ratio_3y                 numeric(8,4)      NULLABLE

-- Value at Risk Analysis
var_95_1y                       numeric(8,4)      NULLABLE
var_99_1y                       numeric(8,4)      NULLABLE
cvar_95_1y                      numeric(8,4)      NULLABLE

-- Correlation & Beta Analysis
correlation_nifty50             numeric(8,4)      NULLABLE
beta_nifty50                    numeric(8,4)      NULLABLE
tracking_error                  numeric(8,4)      NULLABLE

-- Distribution Analysis
skewness_1y                     numeric(8,4)      NULLABLE
kurtosis_1y                     numeric(8,4)      NULLABLE
downside_deviation_1y           numeric(8,4)      NULLABLE

-- Risk Score & Grade
risk_score                      numeric(6,2)      NULLABLE
risk_grade                      varchar(2)        NULLABLE

created_at                      timestamp         DEFAULT CURRENT_TIMESTAMP
```

## VALIDATION & QUALITY ASSURANCE

### 8. BACKTESTING_RESULTS (Validation Framework) - 19 Fields
```sql
id                              integer(32,0)     PRIMARY KEY
fund_id                         integer(32,0)     UNIQUE
validation_date                 date              UNIQUE
historical_score_date           date              UNIQUE

-- Historical Predictions
historical_total_score          numeric(10,2)     NULLABLE
historical_recommendation      varchar(20)       NULLABLE
historical_quartile             integer(32,0)     NULLABLE

-- Actual Performance Results
actual_return_3m                numeric(10,4)     NULLABLE
actual_return_6m                numeric(10,4)     NULLABLE
actual_return_1y                numeric(10,4)     NULLABLE

-- Prediction vs Reality Analysis
predicted_performance           varchar(20)       NULLABLE
actual_performance              varchar(20)       NULLABLE
prediction_accuracy             boolean           NULLABLE
quartile_maintained             boolean           NULLABLE

-- Accuracy Scoring
score_accuracy_3m               numeric(5,2)      NULLABLE
score_accuracy_6m               numeric(5,2)      NULLABLE
score_accuracy_1y               numeric(5,2)      NULLABLE
quartile_accuracy_score         numeric(5,2)      NULLABLE

created_at                      timestamp         DEFAULT CURRENT_TIMESTAMP
```

### 9. VALIDATION_SUMMARY_REPORTS (System Health) - 22 Fields
```sql
id                              integer(32,0)     PRIMARY KEY
validation_run_id               text              UNIQUE, NOT NULL
run_date                        date              NOT NULL
total_funds_tested              integer(32,0)     NOT NULL
validation_period_months        integer(32,0)     NOT NULL

-- Prediction Accuracy Metrics
overall_prediction_accuracy_3m  numeric(5,2)      NULLABLE
overall_prediction_accuracy_6m  numeric(5,2)      NULLABLE
overall_prediction_accuracy_1y  numeric(5,2)      NULLABLE

-- Score Correlation Analysis
overall_score_correlation_3m    numeric(5,2)      NULLABLE
overall_score_correlation_6m    numeric(5,2)      NULLABLE
overall_score_correlation_1y    numeric(5,2)      NULLABLE

-- Quartile Stability Metrics
quartile_stability_3m           numeric(5,2)      NULLABLE
quartile_stability_6m           numeric(5,2)      NULLABLE
quartile_stability_1y           numeric(5,2)      NULLABLE

-- Recommendation Accuracy by Type
strong_buy_accuracy             numeric(5,2)      NULLABLE
buy_accuracy                    numeric(5,2)      NULLABLE
hold_accuracy                   numeric(5,2)      NULLABLE
sell_accuracy                   numeric(5,2)      NULLABLE
strong_sell_accuracy            numeric(5,2)      NULLABLE

-- Validation Status
validation_status               text              NOT NULL
created_at                      timestamp         DEFAULT CURRENT_TIMESTAMP
```

## MARKET DATA & FRAMEWORK

### 10. MARKET_INDICES (Benchmark Data) - 12 Fields
```sql
index_name                      text              PRIMARY KEY
index_date                      date              PRIMARY KEY
open_value                      numeric(12,2)     NULLABLE
high_value                      numeric(12,2)     NULLABLE
low_value                       numeric(12,2)     NULLABLE
close_value                     numeric(12,2)     NULLABLE
volume                          integer(32,0)     NULLABLE
market_cap                      numeric(18,2)     NULLABLE
pe_ratio                        numeric(6,2)      NULLABLE
pb_ratio                        numeric(6,2)      NULLABLE
dividend_yield                  numeric(4,2)      NULLABLE
created_at                      timestamp         DEFAULT now()
```

### 11. ELIVATE_SCORES (Market Framework) - 32 Fields
```sql
id                              integer(32,0)     PRIMARY KEY
score_date                      date              UNIQUE, NOT NULL

-- External Influence (20 points)
us_gdp_growth                   numeric(5,2)      NULLABLE
fed_funds_rate                  numeric(4,2)      NULLABLE
dxy_index                       numeric(6,2)      NULLABLE
china_pmi                       numeric(4,1)      NULLABLE
external_influence_score        numeric(4,1)      NULLABLE

-- Local Story (20 points)
india_gdp_growth                numeric(5,2)      NULLABLE
gst_collection_cr               numeric(10,2)     NULLABLE
iip_growth                      numeric(5,2)      NULLABLE
india_pmi                       numeric(4,1)      NULLABLE
local_story_score               numeric(4,1)      NULLABLE

-- Inflation & Rates (20 points)
cpi_inflation                   numeric(4,2)      NULLABLE
wpi_inflation                   numeric(4,2)      NULLABLE
repo_rate                       numeric(4,2)      NULLABLE
ten_year_yield                  numeric(4,2)      NULLABLE
inflation_rates_score           numeric(4,1)      NULLABLE

-- Valuation & Earnings (20 points)
nifty_pe                        numeric(5,2)      NULLABLE
nifty_pb                        numeric(4,2)      NULLABLE
earnings_growth                 numeric(5,2)      NULLABLE
valuation_earnings_score        numeric(4,1)      NULLABLE

-- Allocation of Capital (10 points)
fii_flows_cr                    numeric(8,2)      NULLABLE
dii_flows_cr                    numeric(8,2)      NULLABLE
sip_inflows_cr                  numeric(8,2)      NULLABLE
allocation_capital_score        numeric(4,1)      NULLABLE

-- Trends & Sentiments (10 points)
stocks_above_200dma_pct         numeric(4,1)      NULLABLE
india_vix                       numeric(5,2)      NULLABLE
advance_decline_ratio           numeric(4,2)      NULLABLE
trends_sentiments_score         numeric(4,1)      NULLABLE

-- Final Framework Results
total_elivate_score             numeric(5,1)      NOT NULL
market_stance                   text              NOT NULL

created_at                      timestamp         DEFAULT now()
```

### 12. ETL_PIPELINE_RUNS (Process Monitoring) - 10 Fields
```sql
id                              integer(32,0)     PRIMARY KEY
pipeline_name                   text              NOT NULL
status                          text              NOT NULL
start_time                      timestamp         NOT NULL
end_time                        timestamp         NULLABLE
records_processed               integer(32,0)     NULLABLE
error_message                   text              NULLABLE
health_score                    integer(32,0)     NULLABLE
data_freshness_hours            numeric           NULLABLE
created_at                      timestamp         DEFAULT now()
```

## SUMMARY STATISTICS
- **Total Tables**: 12 active production tables
- **Total Fields**: 334 fields across all tables
- **Primary Keys**: 12 (one per table)
- **Foreign Keys**: 6 relationships
- **Unique Constraints**: 15 composite unique keys
- **Data Volume**: 20M+ NAV records, 11,800 scored funds
- **Scoring Range**: 25.80-76.00 points (fund_scores_corrected.total_score)
- **Recommendation Types**: 5 (STRONG_BUY, BUY, HOLD, SELL, STRONG_SELL)