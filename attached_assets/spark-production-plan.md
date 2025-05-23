# Spark Capital MF Selection Model - Production Implementation Plan

## 🎯 Implementation Strategy Overview

### **Phase-Based Development Approach**
```
Phase 1: Data Infrastructure (Weeks 1-3)
├── Database setup & schema
├── Data collectors for Indian markets
├── ETL pipeline implementation
└── Data quality validation

Phase 2: Core Engine (Weeks 4-6)
├── ELIVATE framework implementation
├── Mutual fund scoring engine
├── Risk calculation modules
└── Portfolio construction logic

Phase 3: Backtesting (Weeks 7-8)
├── Historical data processing
├── Backtesting engine
├── Performance analytics
└── Model validation framework

Phase 4: APIs & Frontend (Weeks 9-11)
├── REST API development
├── Interactive dashboard
├── Reporting system
└── User management

Phase 5: Production Deployment (Weeks 12-13)
├── Containerization & orchestration
├── Monitoring & alerting
├── Performance optimization
└── Documentation & training
```

## 🏗️ Technical Architecture

### **System Architecture Diagram**
```
┌─────────────────────────────────────────────────────────────────┐
│                    SPARK CAPITAL MF SELECTION SYSTEM            │
├─────────────────────────────────────────────────────────────────┤
│                          FRONTEND LAYER                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   React     │  │  Streamlit  │  │   Grafana   │             │
│  │  Dashboard  │  │   Admin     │  │  Monitoring │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
├─────────────────────────────────────────────────────────────────┤
│                         API GATEWAY                             │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │               FastAPI Application                           │ │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────────┐   │ │
│  │  │  Fund   │ │ Market  │ │Portfolio│ │   Backtesting   │   │ │
│  │  │  APIs   │ │  APIs   │ │  APIs   │ │      APIs       │   │ │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                       BUSINESS LOGIC LAYER                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │   ELIVATE   │  │  Scoring    │  │ Backtesting │             │
│  │  Framework  │  │   Engine    │  │   Engine    │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
├─────────────────────────────────────────────────────────────────┤
│                        DATA LAYER                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ PostgreSQL  │  │    Redis    │  │    S3/      │             │
│  │(TimescaleDB)│  │   Cache     │  │  MinIO      │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
├─────────────────────────────────────────────────────────────────┤
│                    DATA INGESTION LAYER                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │    NSE      │  │    AMFI     │  │     RBI     │             │
│  │  Collector  │  │  Collector  │  │  Collector  │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Technology Stack

| **Layer** | **Technology** | **Justification** | **Alternatives** |
|-----------|----------------|-------------------|------------------|
| **Backend** | Python 3.11 + FastAPI | High performance, async support, auto docs | Django REST, Flask |
| **Database** | PostgreSQL 15 + TimescaleDB | Time-series optimization, ACID compliance | InfluxDB, ClickHouse |
| **Cache** | Redis 7.0 | In-memory performance, pub/sub | Memcached |
| **Message Queue** | Celery + Redis | Distributed task processing | RQ, Dramatiq |
| **Frontend** | React 18 + TypeScript | Component reusability, type safety | Vue.js, Angular |
| **Visualization** | Recharts + D3.js | Rich interactive charts | Chart.js, Plotly |
| **Container** | Docker + Docker Compose | Consistent deployments | Podman |
| **Orchestration** | Kubernetes | Production scalability | Docker Swarm |
| **Monitoring** | Prometheus + Grafana | Comprehensive metrics | Datadog, New Relic |
| **Logging** | ELK Stack | Centralized log management | Loki + Grafana |
| **CI/CD** | GitHub Actions | Integration with code repo | GitLab CI, Jenkins |

## 🚀 Phase 1: Data Infrastructure Implementation

### **1.1 Database Schema Design**

```sql
-- Core database schema for production
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Fund master data
CREATE TABLE funds (
    fund_id SERIAL PRIMARY KEY,
    scheme_code VARCHAR(20) UNIQUE NOT NULL,
    isin_div_payout VARCHAR(12),
    isin_div_reinvest VARCHAR(12),
    fund_name VARCHAR(200) NOT NULL,
    amc_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),
    benchmark_name VARCHAR(100),
    fund_manager VARCHAR(100),
    inception_date DATE,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    minimum_investment BIGINT,
    minimum_additional BIGINT,
    exit_load DECIMAL(4,2),
    lock_in_period INTEGER, -- in days
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Time-series NAV data
CREATE TABLE nav_data (
    fund_id INTEGER REFERENCES funds(fund_id),
    nav_date DATE NOT NULL,
    nav_value DECIMAL(12,4) NOT NULL,
    nav_change DECIMAL(12,4),
    nav_change_pct DECIMAL(8,4),
    aum_cr DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fund_id, nav_date)
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('nav_data', 'nav_date');

-- Fund scores and rankings
CREATE TABLE fund_scores (
    fund_id INTEGER REFERENCES funds(fund_id),
    score_date DATE NOT NULL,
    
    -- Historical returns scores (40 points)
    return_3m_score DECIMAL(4,1),
    return_6m_score DECIMAL(4,1),
    return_1y_score DECIMAL(4,1),
    return_3y_score DECIMAL(4,1),
    return_5y_score DECIMAL(4,1),
    historical_returns_total DECIMAL(5,1),
    
    -- Risk grade scores (30 points)
    std_dev_1y_score DECIMAL(4,1),
    std_dev_3y_score DECIMAL(4,1),
    updown_capture_1y_score DECIMAL(4,1),
    updown_capture_3y_score DECIMAL(4,1),
    max_drawdown_score DECIMAL(4,1),
    risk_grade_total DECIMAL(5,1),
    
    -- Other metrics scores (30 points)
    sectoral_similarity_score DECIMAL(4,1),
    forward_score DECIMAL(4,1),
    aum_size_score DECIMAL(4,1),
    expense_ratio_score DECIMAL(4,1),
    other_metrics_total DECIMAL(5,1),
    
    -- Final scoring
    total_score DECIMAL(5,1) NOT NULL,
    quartile INTEGER CHECK (quartile BETWEEN 1 AND 4),
    category_rank INTEGER,
    category_total INTEGER,
    recommendation VARCHAR(10) CHECK (recommendation IN ('BUY', 'HOLD', 'REVIEW', 'SELL')),
    
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (fund_id, score_date)
);

SELECT create_hypertable('fund_scores', 'score_date');

-- Portfolio holdings
CREATE TABLE portfolio_holdings (
    holding_id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(fund_id),
    holding_date DATE NOT NULL,
    stock_symbol VARCHAR(20),
    stock_name VARCHAR(100),
    holding_percent DECIMAL(5,2),
    market_value_cr DECIMAL(12,2),
    sector VARCHAR(50),
    industry VARCHAR(100),
    market_cap_category VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Market data
CREATE TABLE market_indices (
    index_name VARCHAR(50),
    index_date DATE,
    open_value DECIMAL(12,2),
    high_value DECIMAL(12,2),
    low_value DECIMAL(12,2),
    close_value DECIMAL(12,2),
    volume BIGINT,
    market_cap DECIMAL(18,2),
    pe_ratio DECIMAL(6,2),
    pb_ratio DECIMAL(6,2),
    dividend_yield DECIMAL(4,2),
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (index_name, index_date)
);

SELECT create_hypertable('market_indices', 'index_date');

-- ELIVATE framework data
CREATE TABLE elivate_scores (
    score_date DATE PRIMARY KEY,
    
    -- External Influence (20 points)
    us_gdp_growth DECIMAL(5,2),
    fed_funds_rate DECIMAL(4,2),
    dxy_index DECIMAL(6,2),
    china_pmi DECIMAL(4,1),
    external_influence_score DECIMAL(4,1),
    
    -- Local Story (20 points)
    india_gdp_growth DECIMAL(5,2),
    gst_collection_cr DECIMAL(10,2),
    iip_growth DECIMAL(5,2),
    india_pmi DECIMAL(4,1),
    local_story_score DECIMAL(4,1),
    
    -- Inflation & Rates (20 points)
    cpi_inflation DECIMAL(4,2),
    wpi_inflation DECIMAL(4,2),
    repo_rate DECIMAL(4,2),
    ten_year_yield DECIMAL(4,2),
    inflation_rates_score DECIMAL(4,1),
    
    -- Valuation & Earnings (20 points)
    nifty_pe DECIMAL(5,2),
    nifty_pb DECIMAL(4,2),
    earnings_growth DECIMAL(5,2),
    valuation_earnings_score DECIMAL(4,1),
    
    -- Allocation of Capital (10 points)
    fii_flows_cr DECIMAL(8,2),
    dii_flows_cr DECIMAL(8,2),
    sip_inflows_cr DECIMAL(8,2),
    allocation_capital_score DECIMAL(4,1),
    
    -- Trends & Sentiments (10 points)
    stocks_above_200dma_pct DECIMAL(4,1),
    india_vix DECIMAL(5,2),
    advance_decline_ratio DECIMAL(4,2),
    trends_sentiments_score DECIMAL(4,1),
    
    -- Total ELIVATE score
    total_elivate_score DECIMAL(5,1) NOT NULL,
    market_stance VARCHAR(20) CHECK (market_stance IN ('BULLISH', 'NEUTRAL', 'BEARISH')),
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_funds_category ON funds(category);
CREATE INDEX idx_funds_amc ON funds(amc_name);
CREATE INDEX idx_nav_data_date ON nav_data(nav_date DESC);
CREATE INDEX idx_fund_scores_date ON fund_scores(score_date DESC);
CREATE INDEX idx_fund_scores_total ON fund_scores(total_score DESC);
CREATE INDEX idx_holdings_date ON portfolio_holdings(holding_date DESC);
```

### **1.2 Data Collection Framework**

```python
# data_collectors/base.py
from abc import ABC, abstractmethod
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any
import time

class BaseDataCollector(ABC):
    def __init__(self, name: str, base_url: str = None):
        self.name = name
        self.base_url = base_url
        self.session = None
        self.logger = logging.getLogger(f"collector.{name}")
        self.rate_limit_delay = 1.0  # seconds between requests
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def fetch_with_retry(self, url: str, params: Dict = None, 
                              max_retries: int = 3) -> Optional[Dict]:
        """Fetch data with retry logic and rate limiting"""
        
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(self.rate_limit_delay)
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limited
                        wait_time = 2 ** attempt
                        self.logger.warning(f"Rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                    else:
                        self.logger.error(f"HTTP {response.status} for {url}")
                        
            except Exception as e:
                self.logger.error(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        
        return None
    
    @abstractmethod
    async def collect_data(self) -> pd.DataFrame:
        """Collect data and return as DataFrame"""
        pass
    
    def validate_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Validate collected data quality"""
        return {
            'total_records': len(data),
            'missing_values': data.isnull().sum().to_dict(),
            'data_types': data.dtypes.to_dict(),
            'date_range': {
                'min': data.index.min() if hasattr(data.index, 'min') else None,
                'max': data.index.max() if hasattr(data.index, 'max') else None
            }
        }

# data_collectors/amfi_collector.py
import io
from .base import BaseDataCollector

class AMFICollector(BaseDataCollector):
    def __init__(self):
        super().__init__("AMFI", "https://www.amfiindia.com")
        
    async def collect_nav_data(self) -> pd.DataFrame:
        """Collect latest NAV data from AMFI"""
        url = f"{self.base_url}/spages/NAVAll.txt"
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    text_data = await response.text()
                    return self._parse_nav_data(text_data)
                else:
                    self.logger.error(f"Failed to fetch AMFI data: {response.status}")
                    return pd.DataFrame()
                    
        except Exception as e:
            self.logger.error(f"Error collecting AMFI data: {e}")
            return pd.DataFrame()
    
    def _parse_nav_data(self, text_data: str) -> pd.DataFrame:
        """Parse AMFI NAV text data"""
        lines = text_data.strip().split('\n')
        nav_records = []
        
        for line in lines:
            if ';' in line and line.count(';') >= 5:
                parts = [p.strip() for p in line.split(';')]
                
                # Skip headers and invalid lines
                if len(parts) < 6 or parts[4] in ['N.A.', 'NAV']:
                    continue
                
                try:
                    nav_value = float(parts[4])
                    
                    nav_records.append({
                        'scheme_code': parts[0],
                        'isin_div_payout': parts[1] if parts[1] != '-' else None,
                        'isin_div_reinvest': parts[2] if parts[2] != '-' else None,
                        'scheme_name': parts[3],
                        'nav': nav_value,
                        'date': parts[5]
                    })
                    
                except (ValueError, IndexError) as e:
                    continue
        
        df = pd.DataFrame(nav_records)
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], format='%d-%b-%Y', errors='coerce')
            df = df.dropna(subset=['date', 'nav'])
            df = df.sort_values(['scheme_code', 'date'])
        
        return df
    
    async def collect_data(self) -> pd.DataFrame:
        return await self.collect_nav_data()

# data_collectors/nse_collector.py
class NSECollector(BaseDataCollector):
    def __init__(self):
        super().__init__("NSE", "https://www.nseindia.com")
        
    async def collect_indices_data(self) -> pd.DataFrame:
        """Collect NSE indices data"""
        indices = ['NIFTY 50', 'NIFTY 500', 'NIFTY MIDCAP 100', 'NIFTY SMALLCAP 100']
        all_data = []
        
        for index in indices:
            url = f"{self.base_url}/api/equity-stockIndices"
            params = {'index': index}
            
            data = await self.fetch_with_retry(url, params)
            if data and 'data' in data:
                for item in data['data']:
                    if item.get('symbol') == index:
                        all_data.append({
                            'index_name': index,
                            'index_date': datetime.now().date(),
                            'open_value': self._safe_float(item.get('open')),
                            'high_value': self._safe_float(item.get('dayHigh')),
                            'low_value': self._safe_float(item.get('dayLow')),
                            'close_value': self._safe_float(item.get('last')),
                            'volume': self._safe_int(item.get('totalTradedVolume')),
                            'change': self._safe_float(item.get('change')),
                            'change_pct': self._safe_float(item.get('pChange'))
                        })
        
        return pd.DataFrame(all_data)
    
    async def collect_stock_data(self, symbol: str) -> Dict:
        """Collect individual stock data"""
        url = f"{self.base_url}/api/quote-equity"
        params = {'symbol': symbol}
        
        return await self.fetch_with_retry(url, params)
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert to float"""
        try:
            if value is None or value == '' or value == '-':
                return None
            return float(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value) -> Optional[int]:
        """Safely convert to int"""
        try:
            if value is None or value == '' or value == '-':
                return None
            return int(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None
    
    async def collect_data(self) -> pd.DataFrame:
        return await self.collect_indices_data()

# data_collectors/rbi_collector.py
class RBICollector(BaseDataCollector):
    def __init__(self):
        super().__init__("RBI", "https://www.rbi.org.in")
        
    async def collect_economic_indicators(self) -> pd.DataFrame:
        """Collect RBI economic indicators"""
        # RBI provides data through database downloads
        # This would involve parsing Excel/CSV files from RBI database
        
        indicators = {
            'repo_rate': await self._get_repo_rate(),
            'cpi_inflation': await self._get_cpi_data(),
            'wpi_inflation': await self._get_wpi_data(),
            'ten_year_yield': await self._get_gsec_yield()
        }
        
        return pd.DataFrame([{
            'date': datetime.now().date(),
            **indicators
        }])
    
    async def _get_repo_rate(self) -> Optional[float]:
        """Get current repo rate"""
        # Implementation would parse RBI monetary policy pages
        # For now, return a placeholder
        return 6.50
    
    async def _get_cpi_data(self) -> Optional[float]:
        """Get latest CPI inflation"""
        # Implementation would parse MOSPI data
        return 5.2
    
    async def _get_wpi_data(self) -> Optional[float]:
        """Get latest WPI inflation"""
        return 1.8
    
    async def _get_gsec_yield(self) -> Optional[float]:
        """Get 10-year G-Sec yield"""
        return 7.2
    
    async def collect_data(self) -> pd.DataFrame:
        return await self.collect_economic_indicators()
```

### **1.3 Data Pipeline Orchestration**

```python
# data_pipeline/orchestrator.py
import asyncio
from datetime import datetime, time
import schedule
from typing import List
from data_collectors import AMFICollector, NSECollector, RBICollector
from database import DatabaseManager
from monitoring import MetricsCollector

class DataPipelineOrchestrator:
    def __init__(self):
        self.db = DatabaseManager()
        self.collectors = {
            'amfi': AMFICollector(),
            'nse': NSECollector(),
            'rbi': RBICollector()
        }
        self.metrics = MetricsCollector()
        
    async def run_daily_pipeline(self):
        """Run daily data collection pipeline"""
        start_time = datetime.now()
        
        try:
            # Collect NAV data (after market close)
            nav_data = await self.collect_nav_data()
            
            # Collect market indices data
            market_data = await self.collect_market_data()
            
            # Calculate derived metrics
            await self.calculate_returns()
            await self.calculate_risk_metrics()
            
            # Run scoring model
            await self.run_fund_scoring()
            
            # Update ELIVATE scores
            await self.update_elivate_scores()
            
            # Log success metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_pipeline_success(duration)
            
        except Exception as e:
            self.metrics.record_pipeline_failure(str(e))
            raise
    
    async def collect_nav_data(self):
        """Collect and store NAV data"""
        async with self.collectors['amfi'] as collector:
            nav_data = await collector.collect_data()
            
            if not nav_data.empty:
                await self.db.upsert_nav_data(nav_data)
                self.metrics.record_data_collection('nav', len(nav_data))
                return nav_data
            else:
                raise Exception("No NAV data collected")
    
    async def collect_market_data(self):
        """Collect and store market data"""
        async with self.collectors['nse'] as collector:
            market_data = await collector.collect_data()
            
            if not market_data.empty:
                await self.db.upsert_market_data(market_data)
                self.metrics.record_data_collection('market', len(market_data))
                return market_data
            else:
                raise Exception("No market data collected")
    
    def setup_scheduler(self):
        """Setup scheduled jobs"""
        
        # Daily NAV collection (9:30 PM IST after NAV publication)
        schedule.every().day.at("21:30").do(
            lambda: asyncio.run(self.run_daily_pipeline())
        )
        
        # Weekly economic data (Sunday)
        schedule.every().sunday.at("10:00").do(
            lambda: asyncio.run(self.collect_economic_data())
        )
        
        # Monthly portfolio holdings (1st of each month)
        schedule.every().month.do(
            lambda: asyncio.run(self.collect_portfolio_holdings())
        )
    
    async def run_scheduler(self):
        """Run the scheduler"""
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)  # Check every minute
```

## 🧮 Phase 2: Core Scoring Engine Implementation

### **2.1 ELIVATE Framework Engine**

```python
# scoring/elivate_framework.py
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Tuple
from dataclasses import dataclass

@dataclass
class ELIVATEWeights:
    external: float = 0.20
    local: float = 0.20
    inflation: float = 0.20
    valuation: float = 0.20
    allocation: float = 0.10
    trends: float = 0.10

class ELIVATEFramework:
    def __init__(self, db_manager):
        self.db = db_manager
        self.weights = ELIVATEWeights()
        
    async def calculate_elivate_score(self, date: datetime = None) -> Dict:
        """Calculate comprehensive ELIVATE score"""
        
        if date is None:
            date = datetime.now()
        
        # Get latest data for each component
        market_data = await self.db.get_latest_market_data(date)
        economic_data = await self.db.get_latest_economic_data(date)
        
        scores = {}
        
        # External Influence (0-20 points)
        scores['external'] = await self._score_external_factors(market_data, economic_data)
        
        # Local Story (0-20 points)
        scores['local'] = await self._score_local_factors(market_data, economic_data)
        
        # Inflation & Rates (0-20 points)
        scores['inflation'] = await self._score_inflation_rates(economic_data)
        
        # Valuation & Earnings (0-20 points)
        scores['valuation'] = await self._score_valuation_earnings(market_data)
        
        # Allocation of Capital (0-10 points)
        scores['allocation'] = await self._score_capital_allocation(market_data)
        
        # Trends & Sentiments (0-10 points)
        scores['trends'] = await self._score_trends_sentiment(market_data)
        
        # Calculate total score
        total_score = sum(scores.values())
        
        # Determine market stance
        stance = self._determine_market_stance(total_score)
        
        return {
            'date': date,
            'scores': scores,
            'total_score': total_score,
            'market_stance': stance,
            'components': {
                'external_influence': scores['external'],
                'local_story': scores['local'],
                'inflation_rates': scores['inflation'],
                'valuation_earnings': scores['valuation'],
                'allocation_capital': scores['allocation'],
                'trends_sentiments': scores['trends']
            }
        }
    
    async def _score_external_factors(self, market_data: Dict, economic_data: Dict) -> float:
        """Score external influence factors (0-20 points)"""
        
        scores = {}
        
        # US Economic Indicators (0-5 points)
        us_gdp_growth = economic_data.get('us_gdp_growth', 0)
        fed_rate = economic_data.get('fed_funds_rate', 0)
        
        if us_gdp_growth > 2.5 and fed_rate < 5.0:
            scores['us_economy'] = 5.0
        elif us_gdp_growth > 1.5 and fed_rate < 6.0:
            scores['us_economy'] = 3.5
        elif us_gdp_growth > 0.5:
            scores['us_economy'] = 2.0
        else:
            scores['us_economy'] = 1.0
        
        # Eurozone Stability (0-5 points)
        # Implementation based on available data
        scores['eurozone'] = 3.0  # Placeholder
        
        # China Growth (0-5 points)
        china_pmi = economic_data.get('china_pmi', 50)
        if china_pmi > 52:
            scores['china'] = 5.0
        elif china_pmi > 50:
            scores['china'] = 3.5
        elif china_pmi > 48:
            scores['china'] = 2.0
        else:
            scores['china'] = 1.0
        
        # Global Trade Dynamics (0-5 points)
        dxy_index = economic_data.get('dxy_index', 100)
        if dxy_index < 95:  # Weaker dollar is positive for emerging markets
            scores['trade'] = 5.0
        elif dxy_index < 105:
            scores['trade'] = 3.0
        else:
            scores['trade'] = 1.0
        
        return sum(scores.values())
    
    async def _score_local_factors(self, market_data: Dict, economic_data: Dict) -> float:
        """Score local story factors (0-20 points)"""
        
        scores = {}
        
        # Fiscal Health (0-5 points)
        gst_growth = economic_data.get('gst_collection_growth', 0)
        if gst_growth > 15:
            scores['fiscal'] = 5.0
        elif gst_growth > 10:
            scores['fiscal'] = 4.0
        elif gst_growth > 5:
            scores['fiscal'] = 3.0
        else:
            scores['fiscal'] = 2.0
        
        # Economic Activity (0-5 points)
        india_pmi = economic_data.get('india_pmi', 50)
        iip_growth = economic_data.get('iip_growth', 0)
        
        if india_pmi > 55 and iip_growth > 5:
            scores['activity'] = 5.0
        elif india_pmi > 52 and iip_growth > 2:
            scores['activity'] = 4.0
        elif india_pmi > 50:
            scores['activity'] = 3.0
        else:
            scores['activity'] = 2.0
        
        # Corporate Health (0-5 points)
        earnings_growth = market_data.get('earnings_growth', 0)
        if earnings_growth > 20:
            scores['corporate'] = 5.0
        elif earnings_growth > 15:
            scores['corporate'] = 4.0
        elif earnings_growth > 10:
            scores['corporate'] = 3.0
        else:
            scores['corporate'] = 2.0
        
        # Consumption Trends (0-5 points)
        # Based on auto sales, FMCG growth, etc.
        scores['consumption'] = 3.5  # Placeholder
        
        return sum(scores.values())
    
    async def _score_inflation_rates(self, economic_data: Dict) -> float:
        """Score inflation and rates environment (0-20 points)"""
        
        scores = {}
        
        cpi_inflation = economic_data.get('cpi_inflation', 0)
        repo_rate = economic_data.get('repo_rate', 0)
        ten_year_yield = economic_data.get('ten_year_yield', 0)
        
        # Inflation Trajectory (0-5 points)
        if 2 <= cpi_inflation <= 4:  # RBI target range
            scores['inflation'] = 5.0
        elif 4 < cpi_inflation <= 6:
            scores['inflation'] = 3.0
        elif cpi_inflation < 2:
            scores['inflation'] = 4.0  # Slight deflation concern
        else:
            scores['inflation'] = 1.0
        
        # Central Bank Stance (0-5 points)
        if cpi_inflation < 4 and repo_rate >= 6:
            scores['cb_stance'] = 5.0  # Room for rate cuts
        elif cpi_inflation <= 6:
            scores['cb_stance'] = 3.0
        else:
            scores['cb_stance'] = 1.0
        
        # Global Rate Environment (0-5 points)
        fed_rate = economic_data.get('fed_funds_rate', 0)
        rate_differential = repo_rate - fed_rate
        
        if rate_differential > 2:
            scores['global_rates'] = 5.0
        elif rate_differential > 0:
            scores['global_rates'] = 3.0
        else:
            scores['global_rates'] = 1.0
        
        # Liquidity Conditions (0-5 points)
        yield_spread = ten_year_yield - repo_rate
        if 0.5 <= yield_spread <= 2.0:
            scores['liquidity'] = 5.0
        elif yield_spread > 2.0:
            scores['liquidity'] = 3.0
        else:
            scores['liquidity'] = 2.0
        
        return sum(scores.values())
    
    def _determine_market_stance(self, total_score: float) -> str:
        """Determine market stance based on ELIVATE score"""
        if total_score >= 75:
            return "BULLISH"
        elif total_score >= 50:
            return "NEUTRAL"
        else:
            return "BEARISH"
```

This is just the beginning of the comprehensive implementation. Would you like me to continue with the complete production system including:

1. **Complete Scoring Engine** (Historical Returns, Risk Grade, Other Metrics)
2. **Backtesting Framework** with walk-forward analysis
3. **FastAPI REST APIs** with authentication and rate limiting
4. **React Dashboard** with real-time updates
5. **Docker containerization** and Kubernetes deployment
6. **Monitoring & Alerting** setup
7. **CI/CD pipelines** for automated deployment

The full implementation would be quite extensive. Shall I proceed with specific components you're most interested in, or would you prefer the complete end-to-end implementation?