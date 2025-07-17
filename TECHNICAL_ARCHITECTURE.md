# Technical Architecture - CGMF Models v1.1

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Frontend Layer                           │
├─────────────────────────────────────────────────────────────────┤
│  React 18 + TypeScript + Tailwind CSS + Radix UI              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│  │ Dashboard   │ │ ELIVATE     │ │ Backtesting │              │
│  │ Components  │ │ Framework   │ │ Engine      │              │
│  └─────────────┘ └─────────────┘ └─────────────┘              │
│                                                                 │
│  Data Quality: ZERO_SYNTHETIC_CONTAMINATION                    │
└─────────────────────────────────────────────────────────────────┘
                               │
                         HTTP/REST API
                               │
┌─────────────────────────────────────────────────────────────────┐
│                      Backend Layer                              │
├─────────────────────────────────────────────────────────────────┤
│  Express.js + Node.js + TypeScript                             │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│  │ API Routes  │ │ Services    │ │ Validation  │              │
│  │ Controller  │ │ Layer       │ │ Engine      │              │
│  └─────────────┘ └─────────────┘ └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                               │
                         SQL Queries
                               │
┌─────────────────────────────────────────────────────────────────┐
│                     Database Layer                              │
├─────────────────────────────────────────────────────────────────┤
│  PostgreSQL + Drizzle ORM                                      │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│  │ Fund Data   │ │ Scoring     │ │ Market      │              │
│  │ Tables      │ │ Analytics   │ │ Indices     │              │
│  └─────────────┘ └─────────────┘ └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                               │
                        External APIs
                               │
┌─────────────────────────────────────────────────────────────────┐
│                    Data Sources                                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐              │
│  │ Alpha       │ │ MFAPI       │ │ AMFI        │              │
│  │ Vantage     │ │ Service     │ │ Portal      │              │
│  └─────────────┘ └─────────────┘ └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow Architecture

### 1. Data Ingestion Pipeline
```
External APIs → Data Validation → Database Storage → Index Updates
     │               │                    │              │
     │               │                    │              │
  Rate Limit    Authenticity         Constraint      Performance
  Management     Validation           Enforcement     Optimization
```

### 2. Scoring Engine Flow

#### Individual Fund Scoring
```
NAV Data → Performance Calculation → Risk Analytics → Performance Score
    │              │                      │               │
    │              │                      │               │
Historical     Multi-year            Risk Grade       Quartile
Analysis       Returns               (Low/Med/High)   Assignment
```

#### Market-Wide ELIVATE Scoring
```
Economic Data → ELIVATE Framework → 6-Component Score → Market Stance
     │               │                    │                    │
     │               │                    │                    │
External APIs    Component           Aggregation          NEUTRAL/
(FRED, Yahoo)    Scoring             (63/100)            BULLISH/BEARISH
FRED/Yahoo     Component Calc      Weighted Score    NEUTRAL/BUY
Sources        (External/Local)     (63.0/100)        Interpretation
```

### 3. Backtesting Pipeline
```
Portfolio Config → Fund Selection → Performance Calc → Risk Analysis
       │               │                 │               │
       │               │                 │               │
   User Input      ELIVATE Filter    Historical NAV   Attribution
   Validation      Application       Processing       Analysis
```

## Component Architecture

### Frontend Components
```
src/
├── components/
│   ├── ui/                    # Base UI components (Radix + shadcn)
│   ├── charts/               # Financial chart components
│   ├── dashboard/            # Dashboard-specific components
│   └── layout/               # Layout and navigation
├── pages/
│   ├── dashboard.tsx         # Main dashboard
│   ├── backtesting.tsx       # Backtesting interface
│   ├── quartile-analysis.tsx # Quartile performance analysis
│   └── fund-analysis.tsx     # Individual fund analysis
├── hooks/
│   ├── use-funds.ts          # Fund data management
│   ├── use-elivate.ts        # ELIVATE score hooks
│   └── use-portfolio-backtest.ts # Backtesting logic
└── lib/
    ├── queryClient.ts        # TanStack Query setup
    └── utils.ts              # Utility functions
```

### Backend Services
```
server/
├── api/                      # API endpoint definitions
├── services/
│   ├── authentic-elivate-calculator.ts    # Core scoring engine
│   ├── comprehensive-backtesting-engine.ts # Backtesting logic
│   ├── advanced-risk-metrics.ts           # Risk calculations
│   ├── fund-performance-engine.ts         # Performance analytics
│   └── authentic-validation-engine.ts     # Data validation
├── db.ts                     # Database connection
├── routes.ts                 # Route definitions
└── storage.ts                # Data access layer
```

## Database Design Patterns

### 1. Normalization Strategy
- **3NF Compliance**: Eliminates redundancy while maintaining performance
- **Selective Denormalization**: Calculated fields for query optimization
- **Foreign Key Constraints**: Referential integrity enforcement

### 2. Indexing Strategy
```sql
-- Performance-critical indexes
CREATE INDEX idx_funds_category ON funds(category);
CREATE INDEX idx_nav_data_fund_date ON nav_data(fund_id, nav_date);
CREATE INDEX idx_scores_total_score ON fund_scores_corrected(total_score);
CREATE INDEX idx_scores_quartile ON fund_scores_corrected(quartile);
```

### 3. Constraint Framework
```sql
-- Value range enforcement
ALTER TABLE fund_scores_corrected 
ADD CONSTRAINT chk_sharpe_range 
CHECK (sharpe_ratio >= -5 AND sharpe_ratio <= 5);

-- Data quality constraints
ALTER TABLE nav_data 
ADD CONSTRAINT chk_nav_positive 
CHECK (nav_value > 0);
```

## Performance Optimization

### 1. Query Optimization
- **Connection Pooling**: Efficient database connection management
- **Prepared Statements**: SQL injection prevention + performance
- **Batch Processing**: Bulk operations for large datasets

### 2. Caching Strategy
```typescript
// TanStack Query caching
const CACHE_CONFIG = {
  staleTime: 5 * 60 * 1000,     // 5 minutes
  cacheTime: 10 * 60 * 1000,    // 10 minutes
  refetchOnWindowFocus: false,
  refetchOnMount: false
};
```

### 3. Memory Management
- **Streaming**: Large dataset processing without memory overflow
- **Pagination**: Client-side data loading optimization
- **Cleanup**: Proper resource disposal in async operations

## Security Architecture

### 1. Data Validation
```typescript
// Input validation with Zod schemas
const fundQuerySchema = z.object({
  category: z.string().optional(),
  quartile: z.number().min(1).max(4).optional(),
  limit: z.number().max(1000).default(50)
});
```

### 2. API Security
- **Rate Limiting**: Prevents API abuse
- **CORS Configuration**: Controlled cross-origin access
- **Environment Variables**: Secure configuration management

### 3. Database Security
- **Parameterized Queries**: SQL injection prevention
- **Connection Encryption**: TLS for database connections
- **Access Control**: Role-based database permissions

## Scalability Considerations

### 1. Horizontal Scaling
- **Stateless Services**: Enable load balancer distribution
- **Database Partitioning**: Partition by date/category for large tables
- **CDN Integration**: Static asset distribution

### 2. Vertical Scaling
- **Connection Pool Tuning**: Optimize for concurrent load
- **Index Optimization**: Query performance enhancement
- **Memory Allocation**: JVM/Node.js heap optimization

### 3. Data Architecture
```sql
-- Partitioning strategy for large tables
CREATE TABLE nav_data_2024 PARTITION OF nav_data
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Materialized views for heavy calculations
CREATE MATERIALIZED VIEW fund_performance_summary AS
SELECT fund_id, AVG(nav_value) as avg_nav, 
       COUNT(*) as data_points
FROM nav_data GROUP BY fund_id;
```

## Error Handling & Monitoring

### 1. Error Classification
```typescript
// Structured error handling
enum ErrorType {
  VALIDATION_ERROR = 'VALIDATION_ERROR',
  DATA_INTEGRITY_ERROR = 'DATA_INTEGRITY_ERROR',
  EXTERNAL_API_ERROR = 'EXTERNAL_API_ERROR',
  CALCULATION_ERROR = 'CALCULATION_ERROR'
}
```

### 2. Logging Strategy
- **Structured Logging**: JSON format for analysis
- **Error Tracking**: Comprehensive error context capture
- **Performance Monitoring**: Query timing and resource usage

### 3. Health Checks
```typescript
// System health monitoring
interface HealthCheck {
  database: 'healthy' | 'degraded' | 'down';
  externalAPIs: 'healthy' | 'degraded' | 'down';
  dataQuality: 'good' | 'warning' | 'critical';
  lastUpdated: Date;
}
```

## Deployment Architecture

### 1. Environment Configuration
```bash
# Production environment
NODE_ENV=production
DATABASE_URL=postgresql://user:pass@host:5432/db
ALPHA_VANTAGE_API_KEY=api_key
PORT=5000
```

### 2. Build Process
```bash
# Optimized production build
npm run build:client    # Frontend Vite build
npm run build:server    # TypeScript compilation
npm run db:push         # Schema deployment
```

### 3. Infrastructure Requirements
- **CPU**: 2+ cores for backend processing
- **Memory**: 4GB+ for data processing
- **Storage**: SSD for database performance
- **Network**: Reliable connection for external APIs

## Integration Points

### 1. External API Integration
```typescript
// Alpha Vantage integration
interface AlphaVantageConfig {
  apiKey: string;
  rateLimit: number;    // 5 calls per minute
  retryPolicy: RetryConfig;
  timeout: number;      // 30 seconds
}
```

### 2. Data Synchronization
- **ETL Pipelines**: Scheduled data updates
- **Real-time Sync**: WebSocket for live data
- **Batch Processing**: Nightly data reconciliation

### 3. Third-party Services
- **Market Data Providers**: Multiple source integration
- **Backup Systems**: Data redundancy and recovery
- **Monitoring Tools**: Application performance monitoring

This architecture ensures scalable, secure, and maintainable operation of the comprehensive mutual fund analysis platform.