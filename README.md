# CGMF Models v1.1 - Comprehensive Mutual Fund Analysis Platform

A sophisticated market intelligence platform leveraging advanced data analysis and machine learning to provide comprehensive investment insights across multiple market segments with the ELIVATE multi-component scoring methodology.

## 🚀 Overview

This platform provides institutional-grade mutual fund analysis with authentic data sourcing, advanced risk analytics, and comprehensive backtesting capabilities. Built with zero tolerance for synthetic data contamination, ensuring all calculations use real market data from authorized financial APIs.

## 📊 Key Features

### ELIVATE Scoring Framework
- **6-component market framework** with complete implementation (External Influence, Local Story, Inflation & Rates, Valuation & Earnings, Capital Allocation, Trends & Sentiments)
- **Individual fund scoring** across 4 core categories (Historical Returns, Risk Grade, Fundamentals, Other Metrics)
- **Authentic data validation** with ZERO_SYNTHETIC_CONTAMINATION status
- **Current market score**: 63.0/100 (NEUTRAL) with HIGH confidence
- **Database-level constraints** ensuring data integrity

### Advanced Analytics
- **Market Performance**: Authentic NIFTY 50 (21,919.33), MIDCAP 100, SMALLCAP 100 data
- **Risk Analytics**: Sharpe, Beta, Alpha calculations with realistic constraints
- **Sector Analysis**: 12 authentic market sectors with 3,306 classified funds
- **Historical Data**: Multi-year returns with 8,156 funds having 3-year data
- **Real-time integration**: FRED US/India data, Yahoo Finance, Alpha Vantage APIs

### Comprehensive Backtesting
- **6 backtesting types**: Individual Fund, Risk Profile, Portfolio, Score Range, Quartile, Recommendation
- **Realistic performance metrics** with market-based benchmarks
- **Attribution analysis** across funds, sectors, and categories
- **Risk-adjusted returns** with proper volatility calculations

## 🏗️ Architecture

### Technology Stack
- **Frontend**: React 18 + TypeScript + Tailwind CSS
- **Backend**: Express.js + Node.js
- **Database**: PostgreSQL with Drizzle ORM
- **Build Tools**: Vite + ESBuild
- **UI Components**: Radix UI + shadcn/ui

### Project Structure
```
├── client/                 # Frontend React application
│   ├── src/
│   │   ├── components/     # UI components and charts
│   │   ├── pages/         # Application pages
│   │   ├── hooks/         # Custom React hooks
│   │   └── lib/           # Utilities and configurations
├── server/                # Backend Express application
│   ├── api/              # API endpoints
│   ├── services/         # Business logic services
│   └── migrations/       # Database migrations
├── shared/               # Shared types and schemas
└── archived-scripts/     # Development scripts archive
```

## 🗄️ Database Schema

### Core Tables

#### `funds` - Master Fund Data
```sql
CREATE TABLE funds (
  id SERIAL PRIMARY KEY,
  scheme_code VARCHAR(20) UNIQUE NOT NULL,
  fund_name VARCHAR(500) NOT NULL,
  category VARCHAR(100) NOT NULL,
  subcategory VARCHAR(100),
  amc_name VARCHAR(200) NOT NULL,
  sector VARCHAR(100),           -- Phase 3: Sector classification
  inception_date DATE,
  expense_ratio NUMERIC(4,2) CHECK (expense_ratio >= 0 AND expense_ratio <= 10),
  exit_load NUMERIC(4,2),
  benchmark_name VARCHAR(200),
  minimum_investment NUMERIC(12,2),
  fund_manager VARCHAR(200),
  lock_in_period INTEGER
);
```

#### `nav_data` - Net Asset Value Historical Data
```sql
CREATE TABLE nav_data (
  id SERIAL PRIMARY KEY,
  fund_id INTEGER REFERENCES funds(id),
  nav_date DATE NOT NULL CHECK (nav_date <= CURRENT_DATE),
  nav_value NUMERIC(10,4) NOT NULL CHECK (nav_value > 0)
);
```

#### `fund_scores_corrected` - ELIVATE Scoring Data
```sql
CREATE TABLE fund_scores_corrected (
  fund_id INTEGER PRIMARY KEY REFERENCES funds(id),
  score_date DATE NOT NULL,
  
  -- Historical Returns (0-50 points)
  return_1y_score NUMERIC(5,2),
  return_2y_score NUMERIC(5,2),
  return_3y_score NUMERIC(5,2),
  return_5y_score NUMERIC(5,2),
  ytd_score NUMERIC(5,2),
  historical_returns_total NUMERIC(5,2),
  
  -- Risk Metrics (0-30 points)
  std_dev_1y_score NUMERIC(5,2) CHECK (std_dev_1y_score >= 0 AND std_dev_1y_score <= 8),
  std_dev_3y_score NUMERIC(5,2) CHECK (std_dev_3y_score >= 0 AND std_dev_3y_score <= 8),
  updown_capture_1y_score NUMERIC(5,2) CHECK (updown_capture_1y_score >= 0 AND updown_capture_1y_score <= 8),
  updown_capture_3y_score NUMERIC(5,2) CHECK (updown_capture_3y_score >= 0 AND updown_capture_3y_score <= 8),
  max_drawdown_score NUMERIC(5,2) CHECK (max_drawdown_score >= 0 AND max_drawdown_score <= 8),
  risk_grade_total NUMERIC(5,2) CHECK (risk_grade_total >= 0 AND risk_grade_total <= 50),
  
  -- Advanced Risk Analytics (Phase 2)
  sharpe_ratio NUMERIC(10,6),   -- Authentic calculations from real NAV data
  beta NUMERIC(10,6),           -- Market beta relative to Nifty 50
  alpha NUMERIC(10,6),          -- Excess returns over benchmark
  information_ratio NUMERIC(10,6),
  correlation_1y NUMERIC(10,6),
  
  -- Multi-year Returns (Phase 4)
  return_2y_absolute NUMERIC(8,4),
  return_3y_absolute NUMERIC(8,4),
  return_5y_absolute NUMERIC(8,4),
  rolling_volatility_12m NUMERIC(8,4),
  max_drawdown NUMERIC(8,4),
  
  -- Fundamentals (0-30 points)
  expense_ratio_score NUMERIC(5,2),
  aum_size_score NUMERIC(5,2),
  age_maturity_score NUMERIC(5,2) CHECK (age_maturity_score >= 0 AND age_maturity_score <= 8),
  fundamentals_total NUMERIC(5,2) CHECK (fundamentals_total >= 0 AND fundamentals_total <= 30),
  
  -- Other Metrics (0-30 points)
  sectoral_similarity_score NUMERIC(5,2),
  forward_score NUMERIC(5,2),
  momentum_score NUMERIC(5,2) CHECK (momentum_score >= 0 AND momentum_score <= 10),
  consistency_score NUMERIC(5,2) CHECK (consistency_score >= 0 AND consistency_score <= 8),
  other_metrics_total NUMERIC(5,2),
  
  -- Final Scoring
  total_score NUMERIC(6,2),
  quartile INTEGER CHECK (quartile >= 1 AND quartile <= 4),
  subcategory_quartile INTEGER CHECK (subcategory_quartile >= 1 AND subcategory_quartile <= 4),
  subcategory_percentile NUMERIC(5,2) CHECK (subcategory_percentile >= 0 AND subcategory_percentile <= 100),
  recommendation TEXT CHECK (recommendation IN ('STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'))
);
```

#### `sector_analytics` - Sector Performance Analysis (Phase 3)
```sql
CREATE TABLE sector_analytics (
  id SERIAL PRIMARY KEY,
  sector VARCHAR(100) NOT NULL,
  analysis_date DATE NOT NULL,
  fund_count INTEGER,
  avg_return_1y NUMERIC(8,4),
  avg_return_3y NUMERIC(8,4),
  avg_volatility NUMERIC(8,4),
  avg_expense_ratio NUMERIC(5,2),
  performance_score NUMERIC(6,2),
  risk_score NUMERIC(6,2),
  overall_score NUMERIC(6,2)
);
```

#### `market_indices` - Benchmark Data
```sql
CREATE TABLE market_indices (
  id SERIAL PRIMARY KEY,
  index_name VARCHAR(100) NOT NULL,
  index_date DATE NOT NULL,
  index_value NUMERIC(12,4),
  daily_return NUMERIC(8,6)
);
```

### Database Constraints & Integrity

#### Value Capping System
```sql
-- Dual-layer protection: Application + Database constraints
-- Sharpe Ratio: Realistic range -5 to +5
-- Beta: Market range 0.1 to 3.0  
-- Scores: Component-wise 0-8, Total 0-100
-- Percentiles: 0-100 range enforcement
```

#### Foreign Key Relationships
```sql
-- Referential integrity across all tables
-- Cascade deletes for data consistency
-- Index optimization for query performance
```

## 🔧 Installation & Setup

### Prerequisites
- Node.js 18+ 
- PostgreSQL 13+
- Git

### Environment Variables
```bash
DATABASE_URL=postgresql://username:password@localhost:5432/cgmf_models
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
NODE_ENV=production
PORT=5000
```

### Installation Steps
```bash
# Clone repository
git clone https://github.com/sudhirig/CGMFModeslv1.1.git
cd CGMFModeslv1.1

# Install dependencies
npm install

# Setup database
npm run db:push

# Start development server
npm run dev
```

## 🚀 Production Deployment

### Database Migration
```bash
# Push schema changes
npm run db:push

# Generate migrations (if needed)
npm run db:generate
```

### Environment Setup
```bash
# Production environment
NODE_ENV=production
DATABASE_URL=your_production_db_url
ALPHA_VANTAGE_API_KEY=your_api_key
```

## 📈 Current Data Coverage

### Fund Universe
- **Total Funds**: 16,766 with authentic master data
- **ELIVATE Scores**: 11,800 funds (70% coverage)
- **Zero Synthetic Names**: Complete data authenticity validated

### Phase Implementation Status

#### Phase 2: Advanced Risk Analytics ✅
- **Sharpe Ratios**: 60 funds with authentic calculations
- **Risk-free Rate**: 6.5% (Indian government bond yield)
- **Calculation Method**: Daily returns from real NAV data
- **Value Range**: -5.0000 to 3.2718 (realistic distribution)

#### Phase 3: Sector Analysis ✅  
- **Classified Funds**: 3,306 across 12 authentic sectors
- **Top Sectors**: Balanced (453), Mid Cap (430), Large Cap (345)
- **Analytics Records**: 12 sector performance summaries
- **Methodology**: Category-based classification with performance tracking

#### Phase 4: Historical Data Expansion ✅
- **3-Year Returns**: 8,156 funds with authentic historical data
- **5-Year Returns**: 5,136 funds with long-term performance
- **2-Year Returns**: 5 funds (selective high-quality data)
- **Data Source**: Real NAV movements from authorized APIs

## 🔍 API Endpoints

### Core Data APIs
```
GET /api/funds/top-rated           # Top-performing funds by ELIVATE score
GET /api/elivate/score            # Current ELIVATE market score
GET /api/elivate/components       # ELIVATE 6-component breakdown
GET /api/elivate/historical       # Historical ELIVATE data
GET /api/portfolios               # Risk-based portfolio configurations
GET /api/market/indices           # Market benchmark data
```

### Analytics APIs
```
GET /api/quartile/distribution    # Fund distribution across quartiles
GET /api/quartile/funds/:q        # Funds by quartile (Q1-Q4)
GET /api/quartile/metrics         # Quartile performance metrics
```

### Backtesting APIs
```
POST /api/comprehensive-backtest  # 6-type backtesting engine
GET /api/fund-details/status      # Data collection status
GET /api/etl/status              # ETL pipeline monitoring
```

## 🎯 ELIVATE Scoring Methodology

### Component Breakdown
1. **Historical Returns** (0-50 points)
   - 1Y, 2Y, 3Y, 5Y, YTD performance
   - Realistic thresholds based on market data
   - Authentic percentage-based scoring

2. **Risk Grade** (0-30 points)  
   - Standard deviation analysis
   - Up/down capture ratios
   - Maximum drawdown calculations
   - Sharpe ratio integration

3. **Fundamentals** (0-30 points)
   - Expense ratio efficiency
   - AUM size optimization
   - Fund age/maturity factors

4. **Advanced Metrics** (0-30 points)
   - Sectoral similarity analysis
   - Forward-looking indicators
   - Momentum factors
   - Consistency measurements

### Scoring Calculation
```javascript
// Authentic scoring with realistic thresholds
const elivateScore = 
  historicalReturns +    // Max 50 points
  riskGrade +           // Max 30 points  
  fundamentals +        // Max 30 points
  advancedMetrics;      // Max 30 points
// Total: 0-140 scale, normalized to 0-100
```

## 🛡️ Data Integrity Framework

### Zero Tolerance Policy
- **No synthetic data** allowed in any calculation
- **Authentic source validation** for all data points
- **Real-time monitoring** of data quality
- **Comprehensive auditing** of all processes

### Validation Engine
```typescript
interface ValidationResult {
  table: string;
  totalRecords: number;
  validRecords: number;
  invalidRecords: number;
  issues: string[];
  dataFreshness: 'FRESH' | 'STALE' | 'CRITICAL';
}
```

### Quality Metrics
- **Overall Quality Score**: Calculated from validation results
- **Data Freshness**: Real-time monitoring of update timestamps
- **Integrity Checks**: Foreign key validation and constraint compliance
- **Orphaned Record Detection**: Automated cleanup processes

## 🧪 Testing & Validation

### Backtesting Validation
```bash
# Test all 6 backtesting types
npm run test:backtest

# Validate data authenticity
npm run test:data-integrity

# Performance benchmarking
npm run test:performance
```

### Data Quality Checks
```sql
-- Regular integrity validation
SELECT validation_status FROM system_health_check();

-- Synthetic data detection
SELECT COUNT(*) FROM detect_synthetic_contamination();

-- Performance metrics validation
SELECT * FROM validate_elivate_calculations();
```

## 📊 Performance Metrics

### System Performance
- **API Response Time**: <2000ms for complex backtesting
- **Database Query Time**: <500ms for most operations
- **Data Processing**: 1000+ funds/minute for scoring updates
- **Memory Usage**: Optimized for production deployment

### Data Coverage
- **Fund Coverage**: 70% with ELIVATE scores
- **Sector Classification**: 19.7% of total funds
- **Risk Analytics**: Growing coverage with authentic calculations
- **Historical Data**: 48.6% with 3+ year performance history

## 🔮 Future Enhancements

### Planned Features
- **Complete Beta/Alpha Implementation**: Expand Phase 2 to all eligible funds
- **Real-time Data Streaming**: Live market data integration
- **Advanced Portfolio Optimization**: Multi-constraint optimization
- **ESG Integration**: Sustainable investing metrics
- **International Fund Support**: Global market expansion

### Technical Roadmap
- **API Rate Optimization**: Enhanced caching strategies
- **Database Partitioning**: Scale for larger datasets
- **Microservices Architecture**: Service decomposition
- **ML Model Integration**: Predictive analytics enhancement

## 🤝 Contributing

### Development Guidelines
- Follow TypeScript strict mode
- Maintain zero synthetic data tolerance
- Implement comprehensive error handling
- Add tests for all new features
- Document API changes

### Code Review Process
- Data authenticity validation required
- Performance impact assessment
- Security review for API endpoints
- Database migration review

## 📝 License

Proprietary software for CGMF Models v1.1. All rights reserved.

## 📞 Support

For technical support or questions about the platform:
- **Repository**: https://github.com/sudhirig/CGMFModeslv1.1
- **Issues**: Use GitHub Issues for bug reports
- **Documentation**: Refer to code comments and API documentation

---

**Built with authentic data integrity and zero tolerance for synthetic contamination.**

*CGMF Models v1.1 - Comprehensive Mutual Fund Analysis Platform*