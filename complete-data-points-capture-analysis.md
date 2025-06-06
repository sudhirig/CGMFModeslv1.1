# Complete Data Points Capture Analysis
## All Database Tables - What Data We're Capturing

## CORE PRODUCTION TABLES

### 1. FUNDS TABLE - Master Fund Registry
**Fund Identification Data:**
- Unique scheme codes from AMFI
- ISIN codes for dividend payout and reinvestment options
- Fund names and AMC (Asset Management Company) names
- Fund categories and subcategories

**Fund Characteristics:**
- Benchmark indices for performance comparison
- Fund manager names
- Fund inception dates (fund age calculation)
- Fund status (active/inactive)
- Assets Under Management (AUM) in crores
- Fund age in years

**Investment Parameters:**
- Minimum investment amounts
- Minimum additional investment amounts
- Exit load percentages
- Lock-in periods (in days)
- Expense ratios (annual fees)
- Benchmark index assignments

### 2. NAV_DATA TABLE - Historical Pricing Data
**Daily NAV Information:**
- Fund NAV values with 4-decimal precision
- Daily NAV changes (absolute amounts)
- Daily NAV change percentages
- AUM values in crores for each date
- Complete historical NAV series for 20+ million records

**Time Series Coverage:**
- Daily NAV data spanning multiple years
- Authentic data from AMFI sources only
- No synthetic or interpolated values

### 3. FUND_SCORES_CORRECTED - Primary Scoring System
**Return Performance Scores (40 points maximum):**
- 3-month return scores
- 6-month return scores  
- 1-year return scores
- 3-year return scores
- 5-year return scores
- Year-to-date return scores
- Historical returns total score

**Risk Grade Scores (30 points maximum):**
- 1-year standard deviation scores
- 3-year standard deviation scores
- Up/down capture ratio scores (1-year and 3-year)
- Maximum drawdown scores
- Risk grade total score

**Fundamentals Scores (15 points maximum):**
- Expense ratio scores
- AUM size scores
- Fund age/maturity scores
- Fundamentals total score

**Other Metrics Scores (15 points maximum):**
- Sectoral similarity scores
- Forward-looking scores
- Momentum scores
- Consistency scores
- Other metrics total score

**Final Scoring Results:**
- Total scores (25.80-76.00 point range)
- Quartile rankings (1-4)
- Investment recommendations (STRONG_BUY/BUY/HOLD/SELL/STRONG_SELL)
- Category and subcategory rankings
- Percentile positions within subcategories

**Advanced Risk Analytics:**
- Calmar ratios (return/max drawdown)
- Sortino ratios (downside risk-adjusted returns)
- Value-at-Risk (VaR) 95% confidence levels
- Downside deviation measurements
- Tracking error calculations
- Rolling volatility across multiple timeframes (3M, 6M, 12M, 24M, 36M)

**Performance Distribution Analysis:**
- Positive months percentage
- Negative months percentage
- Maximum consecutive positive/negative months
- Daily returns statistics (count, mean, standard deviation)

**Drawdown Analysis:**
- Maximum drawdown percentages
- Drawdown duration in days
- Average drawdown duration
- Drawdown frequency per year
- Recovery time averages

**Financial Ratios:**
- Alpha (excess returns over benchmark)
- Beta (market sensitivity)
- Sharpe ratios (risk-adjusted returns)
- Information ratios (active return/tracking error)
- Correlation coefficients with market indices

**Absolute Return Values:**
- 3-month absolute returns
- 6-month absolute returns
- 1-year absolute returns
- 3-year absolute returns
- 5-year absolute returns

### 4. FUND_PERFORMANCE_METRICS - Auxiliary Calculations
**Core Performance Data:**
- Annualized returns (1Y, 3Y, 5Y)
- Short-term returns (3M, 6M, YTD)
- Volatility measurements
- Maximum drawdown calculations
- Consistency scoring

**Advanced Ratios:**
- Sharpe ratios
- Alpha calculations
- Beta measurements
- Information ratios
- Total NAV record counts for data quality assessment

**Quality Assurance Metrics:**
- Data quality scores
- Composite performance scores
- Total NAV records available for calculations

**Extended Scoring Framework:**
- Duplicated scoring fields for compatibility
- Additional momentum calculations
- Age/maturity scoring
- Beta-specific scoring
- Sharpe ratio scoring

## ACTIVE SUPPORTING SYSTEMS

### 5. QUARTILE_RANKINGS - Category-Based Analysis
**Ranking Data:**
- Fund rankings within categories
- Total funds in each category
- Quartile assignments (1-4)
- Quartile labels (BUY/HOLD/REVIEW/SELL)
- Percentile positions
- Composite scores for ranking

**Category Analysis:**
- Category-specific performance comparisons
- Peer group analysis within fund categories
- Relative performance metrics

### 6. PERFORMANCE_ATTRIBUTION - Benchmark Analysis
**Benchmark Comparison Data:**
- Fund returns vs category averages (1Y, 3Y)
- Outperformance calculations
- Category percentile rankings
- Benchmark-specific analysis

**Risk-Adjusted Comparisons:**
- Fund Sharpe ratios vs category averages
- Sharpe ratio outperformance
- Alpha and beta vs benchmarks

**Attribution Scoring:**
- Attribution scores for relative performance
- Attribution grades (A, B, C, D ratings)
- Benchmark outperformance tracking

### 7. RISK_ANALYTICS - Advanced Risk Calculations
**Volatility Analysis:**
- 1-year and 3-year volatility
- Maximum drawdown measurements (1Y, 3Y)
- Standard deviation calculations

**Advanced Risk Ratios:**
- Sharpe ratios (1Y, 3Y)
- Sortino ratios (1Y, 3Y)
- Calmar ratios (1Y, 3Y)

**Value-at-Risk Analysis:**
- VaR at 95% and 99% confidence levels
- Conditional VaR (CVaR) calculations
- Downside deviation measurements

**Market Correlation Data:**
- Correlation with Nifty 50 index
- Beta calculations vs Nifty 50
- Tracking error measurements

**Distribution Analytics:**
- Return distribution skewness
- Return distribution kurtosis
- Statistical distribution characteristics

**Risk Grading:**
- Overall risk scores
- Risk grade assignments (A-F scale)

## VALIDATION & QUALITY ASSURANCE

### 8. BACKTESTING_RESULTS - Validation Framework
**Historical Prediction Data:**
- Historical total scores
- Historical recommendations
- Historical quartile assignments

**Actual Performance Results:**
- Actual returns (3M, 6M, 1Y forward performance)
- Realized performance vs predictions

**Prediction Accuracy Analysis:**
- Predicted vs actual performance comparisons
- Prediction accuracy flags (boolean)
- Quartile maintenance tracking
- Score accuracy percentages across timeframes

**Validation Scoring:**
- 3-month, 6-month, 1-year accuracy scores
- Quartile accuracy scores
- Prediction reliability metrics

### 9. VALIDATION_SUMMARY_REPORTS - System Health
**Validation Run Data:**
- Unique validation run identifiers
- Total funds tested in each validation
- Validation period lengths (months)

**Accuracy Metrics:**
- Overall prediction accuracy (3M, 6M, 1Y)
- Score correlation analysis
- Quartile stability measurements

**Recommendation-Specific Accuracy:**
- STRONG_BUY recommendation accuracy
- BUY recommendation accuracy
- HOLD recommendation accuracy
- SELL recommendation accuracy
- STRONG_SELL recommendation accuracy

**System Health Status:**
- Validation completion status
- System reliability indicators

## MARKET DATA & FRAMEWORK

### 10. MARKET_INDICES - Benchmark Data
**Daily Market Data:**
- Index opening values
- Daily high values
- Daily low values
- Closing values
- Trading volumes

**Market Valuation Metrics:**
- Total market capitalization
- Price-to-earnings ratios
- Price-to-book ratios
- Dividend yield percentages

**Index Coverage:**
- Multiple benchmark indices
- Historical index performance data

### 11. ELIVATE_SCORES - Market Framework (100-Point System)
**External Influence Indicators (20 points):**
- US GDP growth rates
- Federal funds rates
- Dollar strength index (DXY)
- China PMI indicators

**Local Story Indicators (20 points):**
- India GDP growth rates
- GST collection data (in crores)
- Industrial production growth
- India PMI indicators

**Inflation & Rates Data (20 points):**
- Consumer Price Index inflation
- Wholesale Price Index inflation
- RBI repo rates
- 10-year government bond yields

**Valuation & Earnings Metrics (20 points):**
- Nifty 50 P/E ratios
- Nifty 50 P/B ratios
- Corporate earnings growth rates

**Capital Allocation Flows (10 points):**
- Foreign Institutional Investor flows (crores)
- Domestic Institutional Investor flows (crores)
- SIP inflows (crores)

**Market Trends & Sentiment (10 points):**
- Percentage of stocks above 200-day moving average
- India VIX (volatility index)
- Advance-decline ratios

**Framework Results:**
- Total ELIVATE scores (0-100 scale)
- Market stance classifications
- Investment environment assessments

### 12. ETL_PIPELINE_RUNS - Process Monitoring
**Pipeline Execution Data:**
- Pipeline names and types
- Execution status tracking
- Start and end timestamps
- Records processed counts
- Error messages and debugging information

**System Health Metrics:**
- Pipeline health scores
- Data freshness indicators (hours)
- Process reliability tracking

## DATA CAPTURE SUMMARY

**Total Data Points Captured:**
- **334 unique data fields** across 12 production tables
- **20+ million NAV records** with daily granularity
- **11,800 actively scored funds** with comprehensive metrics
- **100+ risk and performance indicators** per fund
- **50+ market and economic indicators** in ELIVATE framework

**Data Source Authentication:**
- 100% authentic AMFI NAV data
- Real-time market index data from authorized sources
- Genuine economic indicators (no synthetic data)
- Official regulatory filings and announcements

**Data Granularity:**
- Daily NAV updates
- Monthly scoring cycles
- Quarterly performance attribution
- Annual comprehensive reviews

**Key Performance Indicators:**
- Return metrics across 5 timeframes (3M to 5Y)
- Risk metrics across 15+ different calculations
- Quality metrics for data validation
- Prediction accuracy tracking for system validation

This comprehensive data capture enables sophisticated mutual fund analysis, authentic performance scoring, and reliable investment recommendations based entirely on genuine market data.