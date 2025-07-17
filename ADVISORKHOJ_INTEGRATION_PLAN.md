# AdvisorKhoj Integration Plan for CGMF Models v1.1

## Executive Summary
This plan details the integration of AdvisorKhoj data scraping capabilities into the existing CGMF Models v1.1 platform while maintaining the zero synthetic data policy and enhancing the platform's market intelligence capabilities.

## 1. Database Integration Strategy

### New Tables (Drizzle Schema)
Create new schema definitions in `shared/schema.ts`:

```typescript
// AUM Analytics Table
export const aumAnalytics = pgTable('aum_analytics', {
  id: serial('id').primaryKey(),
  amcName: varchar('amc_name', { length: 200 }),
  fundName: varchar('fund_name', { length: 500 }),
  aumCrores: decimal('aum_crores', { precision: 15, scale: 2 }),
  totalAumCrores: decimal('total_aum_crores', { precision: 15, scale: 2 }),
  fundCount: integer('fund_count'),
  category: varchar('category', { length: 100 }),
  dataDate: date('data_date').notNull(),
  source: varchar('source', { length: 100 }).default('advisorkhoj'),
  createdAt: timestamp('created_at').defaultNow(),
  updatedAt: timestamp('updated_at').defaultNow()
});

// Portfolio Overlap Table
export const portfolioOverlap = pgTable('portfolio_overlap', {
  id: serial('id').primaryKey(),
  fund1SchemeCode: varchar('fund1_scheme_code', { length: 20 }),
  fund2SchemeCode: varchar('fund2_scheme_code', { length: 20 }),
  fund1Name: varchar('fund1_name', { length: 500 }),
  fund2Name: varchar('fund2_name', { length: 500 }),
  overlapPercentage: decimal('overlap_percentage', { precision: 5, scale: 2 }),
  analysisDate: date('analysis_date').notNull(),
  source: varchar('source', { length: 100 }).default('advisorkhoj'),
  createdAt: timestamp('created_at').defaultNow()
});

// Manager Analytics Table
export const managerAnalytics = pgTable('manager_analytics', {
  id: serial('id').primaryKey(),
  managerName: varchar('manager_name', { length: 200 }).notNull(),
  managedFundsCount: integer('managed_funds_count'),
  totalAumManaged: decimal('total_aum_managed', { precision: 15, scale: 2 }),
  avgPerformance1y: decimal('avg_performance_1y', { precision: 8, scale: 4 }),
  avgPerformance3y: decimal('avg_performance_3y', { precision: 8, scale: 4 }),
  analysisDate: date('analysis_date').notNull(),
  source: varchar('source', { length: 100 }).default('advisorkhoj'),
  createdAt: timestamp('created_at').defaultNow()
});

// Category Performance Table
export const categoryPerformance = pgTable('category_performance', {
  id: serial('id').primaryKey(),
  categoryName: varchar('category_name', { length: 100 }).notNull(),
  subcategory: varchar('subcategory', { length: 100 }),
  avgReturn1y: decimal('avg_return_1y', { precision: 8, scale: 4 }),
  avgReturn3y: decimal('avg_return_3y', { precision: 8, scale: 4 }),
  avgReturn5y: decimal('avg_return_5y', { precision: 8, scale: 4 }),
  fundCount: integer('fund_count'),
  analysisDate: date('analysis_date').notNull(),
  source: varchar('source', { length: 100 }).default('advisorkhoj'),
  createdAt: timestamp('created_at').defaultNow()
});
```

### Database Migration Strategy
1. Create migration file: `server/migrations/add_advisorkhoj_tables.sql`
2. Use Drizzle's `db:push` command to apply schema changes
3. Add foreign key relationships to existing `funds` table where applicable

## 2. Backend Service Architecture

### Python Scraper Integration
Create a new directory structure:
```
server/scrapers/
├── advisorkhoj/
│   ├── __init__.py
│   ├── scraper.py
│   ├── config.py
│   ├── requirements.txt
│   └── run_scraper.ts  # TypeScript wrapper
```

### TypeScript Service Wrapper
Create `server/services/advisorkhoj-scraper-service.ts`:

```typescript
import { spawn } from 'child_process';
import { db } from '../db';
import { aumAnalytics, portfolioOverlap, managerAnalytics, categoryPerformance } from '@shared/schema';

export class AdvisorKhojScraperService {
  private pythonPath = '/usr/bin/python3';
  private scraperPath = './server/scrapers/advisorkhoj/scraper.py';

  async runScraper(options?: {
    categories?: string[];
    skipExisting?: boolean;
  }): Promise<{
    success: boolean;
    recordsScraped: {
      aum: number;
      overlap: number;
      managers: number;
      categories: number;
    };
    errors: string[];
  }> {
    return new Promise((resolve, reject) => {
      const args = [];
      if (options?.categories) {
        args.push('--categories', options.categories.join(','));
      }
      if (options?.skipExisting) {
        args.push('--skip-existing');
      }

      const pythonProcess = spawn(this.pythonPath, [this.scraperPath, ...args]);
      
      let output = '';
      let errorOutput = '';

      pythonProcess.stdout.on('data', (data) => {
        output += data.toString();
      });

      pythonProcess.stderr.on('data', (data) => {
        errorOutput += data.toString();
      });

      pythonProcess.on('close', (code) => {
        if (code === 0) {
          try {
            const result = JSON.parse(output);
            resolve({
              success: true,
              recordsScraped: result.recordsScraped,
              errors: []
            });
          } catch (e) {
            resolve({
              success: true,
              recordsScraped: { aum: 0, overlap: 0, managers: 0, categories: 0 },
              errors: []
            });
          }
        } else {
          resolve({
            success: false,
            recordsScraped: { aum: 0, overlap: 0, managers: 0, categories: 0 },
            errors: [errorOutput]
          });
        }
      });
    });
  }

  async getLatestAumData() {
    return await db
      .select()
      .from(aumAnalytics)
      .orderBy(desc(aumAnalytics.dataDate))
      .limit(100);
  }

  async getPortfolioOverlaps(minOverlap = 50) {
    return await db
      .select()
      .from(portfolioOverlap)
      .where(gte(portfolioOverlap.overlapPercentage, minOverlap))
      .orderBy(desc(portfolioOverlap.overlapPercentage));
  }

  async getTopManagers() {
    return await db
      .select()
      .from(managerAnalytics)
      .orderBy(desc(managerAnalytics.totalAumManaged))
      .limit(20);
  }

  async getCategoryPerformance() {
    return await db
      .select()
      .from(categoryPerformance)
      .orderBy(desc(categoryPerformance.avgReturn1y));
  }
}
```

### API Endpoints
Add to `server/routes.ts`:

```typescript
// AdvisorKhoj Data Routes
app.get('/api/advisorkhoj/aum', async (req, res) => {
  try {
    const service = new AdvisorKhojScraperService();
    const data = await service.getLatestAumData();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch AUM data' });
  }
});

app.get('/api/advisorkhoj/portfolio-overlap', async (req, res) => {
  try {
    const { minOverlap = 50 } = req.query;
    const service = new AdvisorKhojScraperService();
    const data = await service.getPortfolioOverlaps(Number(minOverlap));
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch portfolio overlap data' });
  }
});

app.get('/api/advisorkhoj/managers', async (req, res) => {
  try {
    const service = new AdvisorKhojScraperService();
    const data = await service.getTopManagers();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch manager data' });
  }
});

app.get('/api/advisorkhoj/category-performance', async (req, res) => {
  try {
    const service = new AdvisorKhojScraperService();
    const data = await service.getCategoryPerformance();
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch category performance' });
  }
});

// Manual scraper trigger (admin only)
app.post('/api/advisorkhoj/scrape', requireAuth, requireAdmin, async (req, res) => {
  try {
    const service = new AdvisorKhojScraperService();
    const result = await service.runScraper(req.body);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: 'Scraping failed' });
  }
});
```

## 3. Frontend Implementation

### New Pages
1. **Enhanced Analytics Dashboard** (`client/src/pages/enhanced-analytics.tsx`)
2. **Portfolio Overlap Analysis** (`client/src/pages/portfolio-overlap.tsx`)
3. **Manager Performance** (`client/src/pages/manager-performance.tsx`)
4. **Category Comparison** (`client/src/pages/category-comparison.tsx`)

### Custom Hooks
Create `client/src/hooks/use-advisorkhoj-data.ts`:

```typescript
import { useQuery } from '@tanstack/react-query';

export const useAumData = () => {
  return useQuery({
    queryKey: ['/api/advisorkhoj/aum'],
    staleTime: 30 * 60 * 1000, // 30 minutes
  });
};

export const usePortfolioOverlap = (minOverlap = 50) => {
  return useQuery({
    queryKey: ['/api/advisorkhoj/portfolio-overlap', minOverlap],
    staleTime: 30 * 60 * 1000,
  });
};

export const useManagerAnalytics = () => {
  return useQuery({
    queryKey: ['/api/advisorkhoj/managers'],
    staleTime: 30 * 60 * 1000,
  });
};

export const useCategoryPerformance = () => {
  return useQuery({
    queryKey: ['/api/advisorkhoj/category-performance'],
    staleTime: 30 * 60 * 1000,
  });
};
```

### Dashboard Components
Create new components in `client/src/components/advisorkhoj/`:

1. **AumAnalyticsCard.tsx** - Display AMC AUM rankings
2. **PortfolioOverlapMatrix.tsx** - Visual overlap matrix
3. **ManagerPerformanceTable.tsx** - Manager analytics table
4. **CategoryPerformanceChart.tsx** - Category comparison charts

### Integration with Existing Features
1. **Fund Analysis Page**: Add portfolio overlap data
2. **Market Overview**: Include category performance metrics
3. **Top Rated Funds**: Enhance with AUM data
4. **ELIVATE Framework**: Use category performance for market analysis

## 4. Data Flow & Synchronization

### Data Collection Schedule
1. **Daily**: Category performance and market indices
2. **Weekly**: AUM data and manager analytics
3. **Monthly**: Portfolio overlap analysis

### Data Validation Pipeline
```typescript
// server/services/advisorkhoj-validator.ts
export class AdvisorKhojValidator {
  async validateAumData(data: any[]): Promise<ValidationResult> {
    // Check for:
    // - Reasonable AUM values (not negative, not extreme)
    // - Valid AMC names matching existing funds
    // - Date consistency
  }

  async validateOverlapData(data: any[]): Promise<ValidationResult> {
    // Check for:
    // - Overlap percentage between 0-100
    // - Valid fund scheme codes
    // - Symmetric overlap relationships
  }
}
```

### Data Integrity Measures
1. **Foreign Key Constraints**: Link to existing funds via scheme_code
2. **Duplicate Prevention**: Unique constraints on (fund_id, date) combinations
3. **Audit Trail**: Track source and scrape timestamps
4. **Error Logging**: Comprehensive error tracking for failed scrapes

## 5. Implementation Phases

### Phase 1: Infrastructure (Week 1)
- [ ] Create database tables and migrations
- [ ] Set up Python scraper environment
- [ ] Implement TypeScript service wrapper
- [ ] Add basic API endpoints

### Phase 2: Data Collection (Week 2)
- [ ] Implement core scraping logic
- [ ] Add data validation
- [ ] Test with sample categories
- [ ] Set up error handling

### Phase 3: Backend Integration (Week 3)
- [ ] Complete all API endpoints
- [ ] Add authentication/authorization
- [ ] Implement caching strategy
- [ ] Create admin interface for manual scraping

### Phase 4: Frontend Development (Week 4)
- [ ] Build new dashboard pages
- [ ] Create visualization components
- [ ] Integrate with existing features
- [ ] Add export functionality

### Phase 5: Testing & Deployment (Week 5)
- [ ] Comprehensive testing
- [ ] Performance optimization
- [ ] Documentation
- [ ] Production deployment

## 6. Technical Considerations

### Performance
1. **Database Indexing**: Add indexes on frequently queried columns
2. **Pagination**: Implement for large datasets
3. **Caching**: Use Redis for frequently accessed data
4. **Batch Processing**: Process scraping in chunks

### Security
1. **Rate Limiting**: Respect AdvisorKhoj's servers
2. **User Agent**: Identify scraper properly
3. **Error Handling**: Graceful failure without exposing internals
4. **Data Sanitization**: Clean all scraped data

### Monitoring
1. **Scraping Metrics**: Track success/failure rates
2. **Data Quality**: Monitor for anomalies
3. **Performance**: Track query times
4. **Alerts**: Set up for scraping failures

## 7. Benefits & Impact

### Enhanced Analytics
- **AUM Insights**: Track market share and fund growth
- **Portfolio Analysis**: Identify overlapping holdings
- **Manager Tracking**: Performance attribution by manager
- **Category Trends**: Market segment analysis

### Competitive Advantage
- **Unique Data**: Portfolio overlap not available elsewhere
- **Real-time Updates**: Fresh market intelligence
- **Comprehensive View**: Complete fund ecosystem analysis
- **Decision Support**: Enhanced investment insights

## 8. Risk Mitigation

### Technical Risks
- **Website Changes**: Monitor for structure changes
- **Rate Limiting**: Implement exponential backoff
- **Data Quality**: Validation at multiple levels
- **Service Availability**: Graceful degradation

### Compliance
- **Terms of Service**: Educational use compliance
- **Data Storage**: Secure and encrypted
- **Access Control**: Role-based permissions
- **Audit Trail**: Complete activity logging

## 9. Future Enhancements

### Phase 2 Features
1. **Real-time Alerts**: Portfolio overlap notifications
2. **Trend Analysis**: Historical category performance
3. **Predictive Analytics**: Manager performance forecasting
4. **API Expansion**: External API for scraped data

### Integration Opportunities
1. **ELIVATE Enhancement**: Use category data for market scoring
2. **Backtesting**: Include manager performance in simulations
3. **Recommendations**: Factor in portfolio overlap
4. **Risk Analysis**: Enhanced with category trends

## 10. Success Metrics

### Technical KPIs
- Scraping success rate > 95%
- Data freshness < 24 hours
- Query response time < 200ms
- Zero data integrity violations

### Business KPIs
- Enhanced fund analysis capabilities
- Improved investment decisions
- Increased platform usage
- Positive user feedback

This comprehensive plan ensures seamless integration of AdvisorKhoj data into your CGMF Models v1.1 platform while maintaining data integrity and enhancing analytical capabilities.