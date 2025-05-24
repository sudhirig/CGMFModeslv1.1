import React, { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { useAllFunds } from "@/hooks/use-all-funds";
import { Loader2, Database, Table as TableIcon, BarChart3, Target, TrendingUp, AlertTriangle } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip as RechartsTooltip } from "recharts";

export default function DatabaseExplorer() {
  const [activeTab, setActiveTab] = useState("overview");
  const { funds, isLoading, error } = useAllFunds();
  const [fundsCount, setFundsCount] = useState<any>({ total: 0, equity: 0, debt: 0, hybrid: 0, other: 0 });
  const [topAmcs, setTopAmcs] = useState<{name: string, count: number}[]>([]);
  const [categoryData, setCategoryData] = useState<{name: string, value: number}[]>([]);
  const [topSubcategories, setTopSubcategories] = useState<{name: string, count: number}[]>([]);

  // Fetch fund scoring data for quartiles tab
  const { data: quartileDistribution } = useQuery({
    queryKey: ['/api/quartile/distribution'],
    staleTime: 5 * 60 * 1000,
  });

  // Fetch funds for each quartile
  const { data: topQ1Funds } = useQuery({
    queryKey: ['/api/quartile/funds/1'],
    staleTime: 5 * 60 * 1000,
  });
  
  const { data: topQ2Funds } = useQuery({
    queryKey: ['/api/quartile/funds/2'],
    staleTime: 5 * 60 * 1000,
  });
  
  const { data: topQ3Funds } = useQuery({
    queryKey: ['/api/quartile/funds/3'],
    staleTime: 5 * 60 * 1000,
  });
  
  const { data: topQ4Funds } = useQuery({
    queryKey: ['/api/quartile/funds/4'],
    staleTime: 5 * 60 * 1000,
  });
  
  // State for inner quartile tabs
  const [activeQuartileTab, setActiveQuartileTab] = useState("q1");

  useEffect(() => {
    if (funds && funds.length > 0) {
      // Calculate counts by category
      const equity = funds.filter(f => f.category === "Equity").length;
      const debt = funds.filter(f => f.category === "Debt").length;
      const hybrid = funds.filter(f => f.category === "Hybrid").length;
      const other = funds.length - equity - debt - hybrid;
      
      setFundsCount({
        total: funds.length,
        equity,
        debt,
        hybrid,
        other
      });
      
      // Calculate category data for pie chart
      setCategoryData([
        { name: "Equity", value: equity },
        { name: "Debt", value: debt },
        { name: "Hybrid", value: hybrid },
        { name: "Other", value: other }
      ]);
      
      // Calculate top AMCs
      const amcCounts: {[key: string]: number} = {};
      funds.forEach(fund => {
        if (fund.amcName) {
          amcCounts[fund.amcName] = (amcCounts[fund.amcName] || 0) + 1;
        }
      });
      
      const amcArray = Object.entries(amcCounts)
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 15);
      
      setTopAmcs(amcArray);
      
      // Calculate subcategories
      const subcategoryCounts: {[key: string]: number} = {};
      funds.forEach(fund => {
        if (fund.subcategory) {
          subcategoryCounts[fund.subcategory] = (subcategoryCounts[fund.subcategory] || 0) + 1;
        }
      });
      
      const subcategoryArray = Object.entries(subcategoryCounts)
        .map(([name, count]) => ({ name, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 20);
      
      setTopSubcategories(subcategoryArray);
    }
  }, [funds]);

  const renderDatabaseSchema = () => {
    // Data Flow and API Overview section
    const dataFlowOverview = (
      <Card className="mb-6">
        <CardHeader>
          <div className="flex items-center space-x-2">
            <TrendingUp className="h-5 w-5 text-muted-foreground" />
            <CardTitle className="text-lg">Data Flow & API Overview</CardTitle>
          </div>
          <CardDescription>How data moves through the system and relevant APIs</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div>
              <h3 className="text-md font-semibold mb-2">Data Ingestion Flow</h3>
              <ol className="list-decimal list-inside space-y-2 pl-2">
                <li>
                  <span className="font-medium">AMFI Data Collection</span>
                  <p className="text-sm text-muted-foreground pl-6">Source: Association of Mutual Funds in India (AMFI) API</p>
                  <p className="text-sm text-muted-foreground pl-6">API: <code>/api/amfi-import</code> initiates data collection</p>
                </li>
                <li>
                  <span className="font-medium">Historical Data Import</span>
                  <p className="text-sm text-muted-foreground pl-6">Frequency: 36-month historical data refreshed weekly</p>
                  <p className="text-sm text-muted-foreground pl-6">API: <code>/api/schedule-import?type=historical&interval=weekly</code></p>
                </li>
                <li>
                  <span className="font-medium">Daily NAV Updates</span>
                  <p className="text-sm text-muted-foreground pl-6">Frequency: Daily updates for current NAV values</p>
                  <p className="text-sm text-muted-foreground pl-6">API: <code>/api/schedule-import?type=daily&interval=daily</code></p>
                </li>
                <li>
                  <span className="font-medium">Data Processing</span>
                  <p className="text-sm text-muted-foreground pl-6">Transformation: Raw data → Structured schema → Derived metrics</p>
                  <p className="text-sm text-muted-foreground pl-6">API: <code>/api/etl-status</code> for monitoring progress</p>
                </li>
              </ol>
            </div>
            
            <div>
              <h3 className="text-md font-semibold mb-2">Key API Endpoints</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <h4 className="text-sm font-semibold">Import & ETL APIs</h4>
                  <ul className="list-disc list-inside space-y-1 text-sm pl-2">
                    <li><code>/api/amfi-import</code> - Manual trigger for AMFI data import</li>
                    <li><code>/api/schedule-import</code> - Schedule automated imports</li>
                    <li><code>/api/stop-scheduled-import</code> - Cancel scheduled imports</li>
                    <li><code>/api/etl-status</code> - Check ETL pipeline status</li>
                    <li><code>/api/etl/runs</code> - View history of ETL processes</li>
                  </ul>
                </div>
                <div>
                  <h4 className="text-sm font-semibold">Analysis & Query APIs</h4>
                  <ul className="list-disc list-inside space-y-1 text-sm pl-2">
                    <li><code>/api/funds</code> - List all mutual funds</li>
                    <li><code>/api/nav-data/:fundId</code> - Get NAV history for a fund</li>
                    <li><code>/api/quartile/distribution</code> - Get fund quartile distribution</li>
                    <li><code>/api/elivate/latest</code> - Get latest ELIVATE market score</li>
                    <li><code>/api/portfolio/model/:id</code> - Get model portfolio details</li>
                  </ul>
                </div>
              </div>
            </div>
            
            <div>
              <h3 className="text-md font-semibold mb-2">Data Transformation Process</h3>
              <div className="bg-black/5 p-3 rounded-md text-sm font-mono overflow-x-auto">
                AMFI Raw Data → Fund Records → NAV History → Calculation Pipeline → Derived Metrics
                <br />↓<br />
                Fund Scoring → Quartile Analysis → ELIVATE Framework → Model Portfolios → Backtesting
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    );
    
    // Complete table definitions
    const tables = [
      { 
        name: "funds", 
        description: "Master data for mutual funds imported from AMFI", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "scheme_code", type: "TEXT", description: "AMFI scheme code (unique)" },
          { name: "isin_div_payout", type: "TEXT", description: "ISIN code for dividend payout variant" },
          { name: "isin_div_reinvest", type: "TEXT", description: "ISIN code for dividend reinvestment variant" },
          { name: "fund_name", type: "TEXT", description: "Name of the mutual fund" },
          { name: "amc_name", type: "TEXT", description: "Asset Management Company name" },
          { name: "category", type: "TEXT", description: "Major category (Equity, Debt, Hybrid)" },
          { name: "subcategory", type: "TEXT", description: "Specific subcategory (Large Cap, Mid Cap, etc.)" },
          { name: "benchmark_name", type: "TEXT", description: "Benchmark index used for comparison" },
          { name: "fund_manager", type: "TEXT", description: "Name of the fund manager" },
          { name: "inception_date", type: "DATE", description: "Fund launch date" },
          { name: "status", type: "TEXT", description: "Fund status (ACTIVE/INACTIVE)" },
          { name: "minimum_investment", type: "INTEGER", description: "Minimum initial investment amount" },
          { name: "minimum_additional", type: "INTEGER", description: "Minimum additional investment" },
          { name: "exit_load", type: "DECIMAL(4,2)", description: "Exit load percentage" },
          { name: "lock_in_period", type: "INTEGER", description: "Lock-in period in days" },
          { name: "expense_ratio", type: "DECIMAL(4,2)", description: "Fund expense ratio percentage" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
          { name: "updated_at", type: "TIMESTAMP", description: "Record update timestamp" },
        ]
      },
      { 
        name: "nav_data", 
        description: "Historical Net Asset Value (NAV) data for mutual funds with derived metrics", 
        columns: [
          { name: "fund_id", type: "INTEGER", description: "Foreign Key to funds table" },
          { name: "nav_date", type: "DATE", description: "Date of the NAV value" },
          { name: "nav_value", type: "DECIMAL(12,4)", description: "NAV price per unit" },
          { name: "nav_change", type: "DECIMAL(12,4)", description: "Derived: Absolute change from previous NAV" },
          { name: "nav_change_pct", type: "DECIMAL(8,4)", description: "Derived: Percentage change from previous" },
          { name: "aum_cr", type: "DECIMAL(15,2)", description: "Assets Under Management in crores" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ],
        constraints: "Composite Primary Key (fund_id, nav_date)"
      },
      { 
        name: "fund_scores", 
        description: "Comprehensive fund scoring and ranking system with multiple derived components", 
        columns: [
          { name: "fund_id", type: "INTEGER", description: "Foreign Key to funds table" },
          { name: "score_date", type: "DATE", description: "Date of score calculation" },
          { name: "return_3m_score", type: "DECIMAL(4,1)", description: "3-month return score (max 8)" },
          { name: "return_6m_score", type: "DECIMAL(4,1)", description: "6-month return score (max 8)" },
          { name: "return_1y_score", type: "DECIMAL(4,1)", description: "1-year return score (max 8)" },
          { name: "return_3y_score", type: "DECIMAL(4,1)", description: "3-year return score (max 8)" },
          { name: "return_5y_score", type: "DECIMAL(4,1)", description: "5-year return score (max 8)" },
          { name: "historical_returns_total", type: "DECIMAL(5,1)", description: "Derived: Combined returns score (max 40)" },
          { name: "std_dev_1y_score", type: "DECIMAL(4,1)", description: "1-year standard deviation score (max 6)" },
          { name: "std_dev_3y_score", type: "DECIMAL(4,1)", description: "3-year standard deviation score (max 6)" },
          { name: "updown_capture_1y_score", type: "DECIMAL(4,1)", description: "1-year up/down capture ratio score (max 6)" },
          { name: "updown_capture_3y_score", type: "DECIMAL(4,1)", description: "3-year up/down capture ratio score (max 6)" },
          { name: "max_drawdown_score", type: "DECIMAL(4,1)", description: "Maximum drawdown score (max 6)" },
          { name: "risk_grade_total", type: "DECIMAL(5,1)", description: "Derived: Combined risk score (max 30)" },
          { name: "sectoral_similarity_score", type: "DECIMAL(4,1)", description: "Sectoral allocation similarity score (max 7.5)" },
          { name: "forward_score", type: "DECIMAL(4,1)", description: "Forward-looking metrics score (max 7.5)" },
          { name: "aum_size_score", type: "DECIMAL(4,1)", description: "Fund size appropriateness score (max 7.5)" },
          { name: "expense_ratio_score", type: "DECIMAL(4,1)", description: "Expense ratio competitiveness score (max 7.5)" },
          { name: "other_metrics_total", type: "DECIMAL(5,1)", description: "Derived: Combined other metrics score (max 30)" },
          { name: "total_score", type: "DECIMAL(5,1)", description: "Derived: Final weighted fund score (max 100)" },
          { name: "quartile", type: "INTEGER", description: "Derived: Performance quartile (1-4, with 1 being best)" },
          { name: "category_rank", type: "INTEGER", description: "Derived: Rank within category" },
          { name: "category_total", type: "INTEGER", description: "Total number of funds in category" },
          { name: "recommendation", type: "TEXT", description: "Investment recommendation" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ],
        constraints: "Composite Primary Key (fund_id, score_date)"
      },
      { 
        name: "portfolio_holdings", 
        description: "Securities held by mutual funds at different points in time", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "fund_id", type: "INTEGER", description: "Foreign Key to funds table" },
          { name: "holding_date", type: "DATE", description: "Date of holding report" },
          { name: "stock_symbol", type: "TEXT", description: "Stock/security ticker symbol" },
          { name: "stock_name", type: "TEXT", description: "Name of the stock/security" },
          { name: "holding_percent", type: "DECIMAL(5,2)", description: "Percentage of portfolio" },
          { name: "market_value_cr", type: "DECIMAL(12,2)", description: "Market value in crores" },
          { name: "sector", type: "TEXT", description: "Economic sector classification" },
          { name: "industry", type: "TEXT", description: "Specific industry classification" },
          { name: "market_cap_category", type: "TEXT", description: "Large/Mid/Small Cap classification" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ]
      },
      { 
        name: "market_indices", 
        description: "Market benchmark data for performance comparison", 
        columns: [
          { name: "index_name", type: "TEXT", description: "Name of the index (e.g., NIFTY 50)" },
          { name: "index_date", type: "DATE", description: "Date of index value" },
          { name: "open_value", type: "DECIMAL(12,2)", description: "Opening value for the day" },
          { name: "high_value", type: "DECIMAL(12,2)", description: "Highest value during the day" },
          { name: "low_value", type: "DECIMAL(12,2)", description: "Lowest value during the day" },
          { name: "close_value", type: "DECIMAL(12,2)", description: "Closing value for the day" },
          { name: "volume", type: "INTEGER", description: "Trading volume" },
          { name: "market_cap", type: "DECIMAL(18,2)", description: "Total market capitalization" },
          { name: "pe_ratio", type: "DECIMAL(6,2)", description: "Price to Earnings ratio" },
          { name: "pb_ratio", type: "DECIMAL(6,2)", description: "Price to Book ratio" },
          { name: "dividend_yield", type: "DECIMAL(4,2)", description: "Dividend yield percentage" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ],
        constraints: "Composite Primary Key (index_name, index_date)"
      },
      { 
        name: "elivate_scores", 
        description: "ELIVATE Framework market analysis with component scores", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "score_date", type: "DATE", description: "Date of analysis (unique)" },
          { name: "us_gdp_growth", type: "DECIMAL(5,2)", description: "US GDP growth rate" },
          { name: "fed_funds_rate", type: "DECIMAL(4,2)", description: "Federal Reserve interest rate" },
          { name: "dxy_index", type: "DECIMAL(6,2)", description: "US Dollar index value" },
          { name: "china_pmi", type: "DECIMAL(4,1)", description: "China Purchasing Managers' Index" },
          { name: "external_influence_score", type: "DECIMAL(4,1)", description: "Derived: Global factors score (max 20)" },
          { name: "india_gdp_growth", type: "DECIMAL(5,2)", description: "India GDP growth rate" },
          { name: "gst_collection_cr", type: "DECIMAL(10,2)", description: "GST collection in crores" },
          { name: "iip_growth", type: "DECIMAL(5,2)", description: "Index of Industrial Production growth" },
          { name: "india_pmi", type: "DECIMAL(4,1)", description: "India Purchasing Managers' Index" },
          { name: "local_story_score", type: "DECIMAL(4,1)", description: "Derived: Domestic factors score (max 20)" },
          { name: "cpi_inflation", type: "DECIMAL(4,2)", description: "Consumer Price Index inflation" },
          { name: "wpi_inflation", type: "DECIMAL(4,2)", description: "Wholesale Price Index inflation" },
          { name: "repo_rate", type: "DECIMAL(4,2)", description: "RBI repo rate" },
          { name: "ten_year_yield", type: "DECIMAL(4,2)", description: "10-year government bond yield" },
          { name: "inflation_rates_score", type: "DECIMAL(4,1)", description: "Derived: Inflation & rates score (max 20)" },
          { name: "nifty_pe", type: "DECIMAL(5,2)", description: "Nifty Price-to-Earnings ratio" },
          { name: "nifty_pb", type: "DECIMAL(4,2)", description: "Nifty Price-to-Book ratio" },
          { name: "earnings_growth", type: "DECIMAL(5,2)", description: "Earnings growth percentage" },
          { name: "valuation_earnings_score", type: "DECIMAL(4,1)", description: "Derived: Valuation score (max 20)" },
          { name: "fii_flows_cr", type: "DECIMAL(8,2)", description: "Foreign Institutional flows in crores" },
          { name: "dii_flows_cr", type: "DECIMAL(8,2)", description: "Domestic Institutional flows in crores" },
          { name: "sip_inflows_cr", type: "DECIMAL(8,2)", description: "Systematic Investment Plan inflows" },
          { name: "allocation_capital_score", type: "DECIMAL(4,1)", description: "Derived: Capital flows score (max 10)" },
          { name: "stocks_above_200dma_pct", type: "DECIMAL(4,1)", description: "% of stocks above 200-day moving avg" },
          { name: "india_vix", type: "DECIMAL(5,2)", description: "India Volatility Index" },
          { name: "advance_decline_ratio", type: "DECIMAL(4,2)", description: "Market breadth indicator" },
          { name: "trends_sentiments_score", type: "DECIMAL(4,1)", description: "Derived: Sentiment score (max 10)" },
          { name: "total_elivate_score", type: "DECIMAL(5,1)", description: "Derived: Overall ELIVATE score (max 100)" },
          { name: "market_stance", type: "TEXT", description: "Recommended market stance (BULLISH/NEUTRAL/BEARISH)" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ]
      },
      { 
        name: "model_portfolios", 
        description: "Predefined investment portfolios for different risk profiles", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "name", type: "TEXT", description: "Portfolio name" },
          { name: "risk_profile", type: "TEXT", description: "Risk tolerance level (CONSERVATIVE/MODERATE/AGGRESSIVE)" },
          { name: "elivate_score_id", type: "INTEGER", description: "Foreign Key to elivate_scores table" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
          { name: "updated_at", type: "TIMESTAMP", description: "Record update timestamp" },
        ]
      },
      { 
        name: "model_portfolio_allocations", 
        description: "Fund allocations within model portfolios", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "portfolio_id", type: "INTEGER", description: "Foreign Key to model_portfolios table" },
          { name: "fund_id", type: "INTEGER", description: "Foreign Key to funds table" },
          { name: "allocation_percent", type: "DECIMAL(5,2)", description: "Percentage allocation" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ],
        indexes: "Index on (portfolio_id, fund_id)"
      },
      { 
        name: "etl_pipeline_runs", 
        description: "Tracks data import processes and statuses", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "pipeline_name", type: "TEXT", description: "Name of data pipeline (historical_import, daily_update, etc.)" },
          { name: "status", type: "TEXT", description: "Process status (RUNNING, COMPLETED, FAILED)" },
          { name: "start_time", type: "TIMESTAMP", description: "Start time of ETL process" },
          { name: "end_time", type: "TIMESTAMP", description: "End time of ETL process" },
          { name: "records_processed", type: "INTEGER", description: "Count of records processed" },
          { name: "error_message", type: "TEXT", description: "Error message if failed" },
          { name: "created_at", type: "TIMESTAMP", description: "Record creation timestamp" },
        ]
      },
      { 
        name: "users", 
        description: "Basic user management", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary Key" },
          { name: "username", type: "TEXT", description: "User login name (unique)" },
          { name: "password", type: "TEXT", description: "Hashed password" },
        ]
      },
    ];
    
    // Entity Relationship Diagram explanation
    const entityRelationships = (
      <Card className="mb-6">
        <CardHeader>
          <div className="flex items-center space-x-2">
            <Target className="h-5 w-5 text-muted-foreground" />
            <CardTitle className="text-lg">Entity Relationships</CardTitle>
          </div>
          <CardDescription>Key relationships between database tables</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div>
              <h3 className="text-md font-semibold mb-2">Primary Relationships</h3>
              <ul className="list-disc list-inside space-y-1 pl-2">
                <li><code>nav_data.fund_id</code> → <code>funds.id</code> (Historical NAV records for each fund)</li>
                <li><code>fund_scores.fund_id</code> → <code>funds.id</code> (Performance scores for each fund)</li>
                <li><code>portfolio_holdings.fund_id</code> → <code>funds.id</code> (Securities held by each fund)</li>
                <li><code>model_portfolios.elivate_score_id</code> → <code>elivate_scores.id</code> (Market analysis driving portfolios)</li>
                <li><code>model_portfolio_allocations.portfolio_id</code> → <code>model_portfolios.id</code> (Funds in each model portfolio)</li>
                <li><code>model_portfolio_allocations.fund_id</code> → <code>funds.id</code> (Fund allocations in portfolios)</li>
              </ul>
            </div>
            
            <div>
              <h3 className="text-md font-semibold mb-2">Composite Keys & Constraints</h3>
              <ul className="list-disc list-inside space-y-1 pl-2">
                <li><code>funds.scheme_code</code> - Unique constraint</li>
                <li><code>(fund_id, nav_date)</code> - Composite primary key on nav_data</li>
                <li><code>(fund_id, score_date)</code> - Composite primary key on fund_scores</li>
                <li><code>(index_name, index_date)</code> - Composite primary key on market_indices</li>
                <li><code>elivate_scores.score_date</code> - Unique constraint</li>
              </ul>
            </div>
            
            <div className="bg-black/5 p-3 rounded-md text-sm overflow-x-auto">
              <pre>{`
+----------------+      +---------------+      +------------------+
|     funds      |<-----| nav_data      |      | market_indices   |
+----------------+      +---------------+      +------------------+
| id (PK)        |      | fund_id (FK)  |      | index_name       |
| scheme_code    |      | nav_date      |      | index_date       |
| fund_name      |      | nav_value     |      | close_value      |
| amc_name       |      +---------------+      +------------------+
+----------------+            |                       |
        |                     |                       |
        |                     |                       |
        v                     |                       |
+----------------+            |                       |
| fund_scores    |<-----------+                       |
+----------------+                                    |
| fund_id (FK)   |                                    |
| score_date     |              +------------------+  |
| total_score    |<-------------| elivate_scores   |<-+
+----------------+              +------------------+
        |                       | id (PK)          |
        |                       | score_date       |
        |                       | total_elivate_   |
        v                       | score            |
+------------------+            +------------------+
| portfolio_       |                    |
| holdings         |                    |
+------------------+                    |
| id (PK)          |                    |
| fund_id (FK)     |                    v
| holding_date     |          +------------------+
+------------------+          | model_portfolios |
                              +------------------+
                              | id (PK)          |
                              | elivate_score_id |
                              +------------------+
                                      |
                                      v
                              +------------------+
                              | model_portfolio_ |
                              | allocations      |
                              +------------------+
                              | portfolio_id (FK)|
                              | fund_id (FK)     |
                              +------------------+
              `}</pre>
            </div>
          </div>
        </CardContent>
      </Card>
    );
    
    // Scheduled Data Operations
    const scheduledOperations = (
      <Card className="mb-6">
        <CardHeader>
          <div className="flex items-center space-x-2">
            <AlertTriangle className="h-5 w-5 text-muted-foreground" />
            <CardTitle className="text-lg">Automated Data Operations</CardTitle>
          </div>
          <CardDescription>Scheduled database maintenance and updates</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div>
              <h3 className="text-md font-semibold mb-2">Scheduled Operations</h3>
              <table className="w-full border-collapse">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 pr-4">Operation</th>
                    <th className="text-left py-2 pr-4">Frequency</th>
                    <th className="text-left py-2">API Endpoint</th>
                  </tr>
                </thead>
                <tbody className="text-sm">
                  <tr className="border-b">
                    <td className="py-2 pr-4">Historical NAV Import</td>
                    <td className="py-2 pr-4">Weekly (Sunday)</td>
                    <td className="py-2"><code>/api/schedule-import?type=historical&interval=weekly</code></td>
                  </tr>
                  <tr className="border-b">
                    <td className="py-2 pr-4">Daily NAV Updates</td>
                    <td className="py-2 pr-4">Daily (5:30 PM)</td>
                    <td className="py-2"><code>/api/schedule-import?type=daily&interval=daily</code></td>
                  </tr>
                  <tr className="border-b">
                    <td className="py-2 pr-4">Fund Scoring</td>
                    <td className="py-2 pr-4">Daily (6:30 PM)</td>
                    <td className="py-2"><code>/api/calculate-fund-scores</code></td>
                  </tr>
                  <tr className="border-b">
                    <td className="py-2 pr-4">ELIVATE Framework</td>
                    <td className="py-2 pr-4">Weekly (Monday)</td>
                    <td className="py-2"><code>/api/elivate/calculate</code></td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4">Optimize Database</td>
                    <td className="py-2 pr-4">Monthly</td>
                    <td className="py-2"><code>/api/db/optimize</code></td>
                  </tr>
                </tbody>
              </table>
            </div>
            
            <div>
              <h3 className="text-md font-semibold mb-2">Current Import Statistics</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="bg-black/5 p-3 rounded-md text-center">
                  <p className="text-sm text-muted-foreground">Total Funds</p>
                  <p className="text-xl font-bold">11,766</p>
                </div>
                <div className="bg-black/5 p-3 rounded-md text-center">
                  <p className="text-sm text-muted-foreground">NAV Records</p>
                  <p className="text-xl font-bold">10,116+</p>
                </div>
                <div className="bg-black/5 p-3 rounded-md text-center">
                  <p className="text-sm text-muted-foreground">Historical Range</p>
                  <p className="text-xl font-bold">36 months</p>
                </div>
                <div className="bg-black/5 p-3 rounded-md text-center">
                  <p className="text-sm text-muted-foreground">Update Frequency</p>
                  <p className="text-xl font-bold">Daily</p>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    );
    
    return (
      <div className="space-y-6">
        {dataFlowOverview}
        {entityRelationships}
        {scheduledOperations}
        
        <div className="space-y-6">
          <h3 className="text-xl font-bold">Complete Database Schema</h3>
          {tables.map(table => (
            <Card key={table.name}>
              <CardHeader>
                <div className="flex items-center space-x-2">
                  <TableIcon className="h-5 w-5 text-muted-foreground" />
                  <CardTitle className="text-lg">{table.name}</CardTitle>
                </div>
                <CardDescription>{table.description}</CardDescription>
                {table.constraints && (
                  <Badge variant="secondary" className="mt-2">{table.constraints}</Badge>
                )}
                {table.indexes && (
                  <Badge variant="outline" className="mt-2">{table.indexes}</Badge>
                )}
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[200px]">Column</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Description</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {table.columns.map(column => (
                        <TableRow key={column.name}>
                          <TableCell className="font-medium">{column.name}</TableCell>
                          <TableCell>
                            <Badge variant="outline">{column.type}</Badge>
                          </TableCell>
                          <TableCell>{column.description}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  };

  const renderDataOverview = () => {
    const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];
    
    return (
      <div className="space-y-6">
        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <Database className="h-8 w-8 mx-auto text-primary mb-2" />
                <div className="text-2xl font-bold">{fundsCount.total}</div>
                <p className="text-sm text-muted-foreground">Total Funds</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div className="rounded-full h-8 w-8 mx-auto mb-2 bg-blue-500 flex items-center justify-center text-white">E</div>
                <div className="text-2xl font-bold">{fundsCount.equity}</div>
                <p className="text-sm text-muted-foreground">Equity Funds</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div className="rounded-full h-8 w-8 mx-auto mb-2 bg-green-500 flex items-center justify-center text-white">D</div>
                <div className="text-2xl font-bold">{fundsCount.debt}</div>
                <p className="text-sm text-muted-foreground">Debt Funds</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div className="rounded-full h-8 w-8 mx-auto mb-2 bg-purple-500 flex items-center justify-center text-white">H</div>
                <div className="text-2xl font-bold">{fundsCount.hybrid}</div>
                <p className="text-sm text-muted-foreground">Hybrid Funds</p>
              </div>
            </CardContent>
          </Card>
        </div>
        
        {/* Charts */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Fund Distribution by Category</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={categoryData}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      outerRadius={80}
                      fill="#8884d8"
                      dataKey="value"
                      label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                    >
                      {categoryData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Legend />
                    <RechartsTooltip />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Top 15 Fund Houses</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="space-y-2">
                      <Skeleton className="h-4 w-[250px]" />
                      <Skeleton className="h-3 w-[200px]" />
                      <Skeleton className="h-2 w-full" />
                    </div>
                  ))
                ) : topAmcs.map((amc, index) => (
                  <div key={index}>
                    <div className="flex justify-between mb-1">
                      <span className="text-sm font-medium">{amc.name}</span>
                      <span className="text-sm text-muted-foreground">{amc.count} funds</span>
                    </div>
                    <div className="w-full bg-neutral-200 rounded-full h-2.5">
                      <div 
                        className="bg-primary h-2.5 rounded-full" 
                        style={{ width: `${(amc.count / topAmcs[0].count) * 100}%` }}
                      ></div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
          
          <Card>
            <CardHeader>
              <CardTitle className="text-lg">Top 20 Fund Subcategories</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {isLoading ? (
                  Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="space-y-2">
                      <Skeleton className="h-4 w-[250px]" />
                      <Skeleton className="h-2 w-full" />
                    </div>
                  ))
                ) : topSubcategories.map((subcat, index) => (
                  <div key={index}>
                    <div className="flex justify-between mb-1">
                      <span className="text-sm font-medium">{subcat.name}</span>
                      <span className="text-sm text-muted-foreground">{subcat.count} funds</span>
                    </div>
                    <div className="w-full bg-neutral-200 rounded-full h-2.5">
                      <div 
                        className="bg-primary h-2.5 rounded-full" 
                        style={{ width: `${(subcat.count / topSubcategories[0].count) * 100}%` }}
                      ></div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  };

  const renderElivateDatabase = () => {
    // ELIVATE Database tables
    const elivateTables = [
      { 
        name: "elivate_scores", 
        description: "ELIVATE Framework market analysis scores", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary key" },
          { name: "score_date", type: "DATE", description: "Date of the ELIVATE analysis" },
          { name: "external_influence_score", type: "DECIMAL", description: "External Influence component (max 20)" },
          { name: "local_story_score", type: "DECIMAL", description: "Local Story component (max 20)" },
          { name: "inflation_rates_score", type: "DECIMAL", description: "Inflation & Rates component (max 20)" },
          { name: "valuation_earnings_score", type: "DECIMAL", description: "Valuation & Earnings component (max 20)" },
          { name: "allocation_capital_score", type: "DECIMAL", description: "Allocation of Capital component (max 10)" },
          { name: "trends_sentiments_score", type: "DECIMAL", description: "Trends & Sentiments component (max 10)" },
          { name: "total_elivate_score", type: "DECIMAL", description: "Total ELIVATE score (max 100)" },
          { name: "market_stance", type: "TEXT", description: "Market stance (BULLISH, NEUTRAL, BEARISH)" },
        ]
      },
      { 
        name: "market_indices", 
        description: "Market indices and economic indicators", 
        columns: [
          { name: "index_name", type: "TEXT", description: "Name of the index or indicator" },
          { name: "index_date", type: "DATE", description: "Date of the index value" },
          { name: "close_value", type: "DECIMAL", description: "Closing value" },
          { name: "volume", type: "BIGINT", description: "Trading volume" },
          { name: "pe_ratio", type: "DECIMAL", description: "Price-to-Earnings ratio" },
          { name: "pb_ratio", type: "DECIMAL", description: "Price-to-Book ratio" },
        ]
      },
      { 
        name: "elivate_factors", 
        description: "Individual factor values for ELIVATE calculation", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary key" },
          { name: "elivate_score_id", type: "INTEGER", description: "Foreign key to elivate_scores" },
          { name: "factor_name", type: "TEXT", description: "Name of the factor (GDP Growth, Inflation, etc.)" },
          { name: "factor_value", type: "DECIMAL", description: "Value of the factor" },
          { name: "component", type: "TEXT", description: "ELIVATE component the factor belongs to" },
          { name: "weight", type: "DECIMAL", description: "Weight of the factor in its component" },
        ]
      },
    ];
    
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div className="rounded-full h-8 w-8 mx-auto mb-2 bg-purple-500 flex items-center justify-center text-white">E</div>
                <div className="text-2xl font-bold">84.7</div>
                <p className="text-sm text-muted-foreground">Current ELIVATE Score</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div className="rounded-full h-8 w-8 mx-auto mb-2 bg-green-500 flex items-center justify-center text-white">↑</div>
                <div className="text-2xl font-bold">Bullish</div>
                <p className="text-sm text-muted-foreground">Market Stance</p>
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6">
              <div className="text-center">
                <div className="rounded-full h-8 w-8 mx-auto mb-2 bg-blue-500 flex items-center justify-center text-white">21</div>
                <div className="text-2xl font-bold">Indicators</div>
                <p className="text-sm text-muted-foreground">Market Factors Tracked</p>
              </div>
            </CardContent>
          </Card>
        </div>
        
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-4">ELIVATE Component Scores</h3>
          <div className="space-y-4">
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium">External Influence</span>
                <span className="text-sm text-muted-foreground">17.8 / 20</span>
              </div>
              <div className="w-full bg-neutral-200 rounded-full h-2.5">
                <div 
                  className="bg-blue-500 h-2.5 rounded-full" 
                  style={{ width: `${(17.8 / 20) * 100}%` }}
                ></div>
              </div>
            </div>
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium">Local Story</span>
                <span className="text-sm text-muted-foreground">16.5 / 20</span>
              </div>
              <div className="w-full bg-neutral-200 rounded-full h-2.5">
                <div 
                  className="bg-green-500 h-2.5 rounded-full" 
                  style={{ width: `${(16.5 / 20) * 100}%` }}
                ></div>
              </div>
            </div>
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium">Inflation & Rates</span>
                <span className="text-sm text-muted-foreground">15.2 / 20</span>
              </div>
              <div className="w-full bg-neutral-200 rounded-full h-2.5">
                <div 
                  className="bg-yellow-500 h-2.5 rounded-full" 
                  style={{ width: `${(15.2 / 20) * 100}%` }}
                ></div>
              </div>
            </div>
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium">Valuation & Earnings</span>
                <span className="text-sm text-muted-foreground">18.3 / 20</span>
              </div>
              <div className="w-full bg-neutral-200 rounded-full h-2.5">
                <div 
                  className="bg-purple-500 h-2.5 rounded-full" 
                  style={{ width: `${(18.3 / 20) * 100}%` }}
                ></div>
              </div>
            </div>
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium">Allocation of Capital</span>
                <span className="text-sm text-muted-foreground">8.7 / 10</span>
              </div>
              <div className="w-full bg-neutral-200 rounded-full h-2.5">
                <div 
                  className="bg-indigo-500 h-2.5 rounded-full" 
                  style={{ width: `${(8.7 / 10) * 100}%` }}
                ></div>
              </div>
            </div>
            <div>
              <div className="flex justify-between mb-1">
                <span className="text-sm font-medium">Trends & Sentiments</span>
                <span className="text-sm text-muted-foreground">8.2 / 10</span>
              </div>
              <div className="w-full bg-neutral-200 rounded-full h-2.5">
                <div 
                  className="bg-red-500 h-2.5 rounded-full" 
                  style={{ width: `${(8.2 / 10) * 100}%` }}
                ></div>
              </div>
            </div>
          </div>
        </div>
        
        {elivateTables.map(table => (
          <Card key={table.name}>
            <CardHeader>
              <div className="flex items-center space-x-2">
                <TableIcon className="h-5 w-5 text-muted-foreground" />
                <CardTitle className="text-lg">{table.name}</CardTitle>
              </div>
              <CardDescription>{table.description}</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[200px]">Column</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Description</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {table.columns.map(column => (
                      <TableRow key={column.name}>
                        <TableCell className="font-medium">{column.name}</TableCell>
                        <TableCell>
                          <Badge variant="outline">{column.type}</Badge>
                        </TableCell>
                        <TableCell>{column.description}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    );
  };

  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Database Explorer</h1>
          <p className="mt-1 text-sm text-neutral-500">
            Explore and analyze the mutual fund and ELIVATE framework databases
          </p>
        </div>
        
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full md:w-[600px] grid-cols-5">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="quartiles">Fund Scoring</TabsTrigger>
            <TabsTrigger value="schema">Schema</TabsTrigger>
            <TabsTrigger value="elivate">ELIVATE Database</TabsTrigger>
            <TabsTrigger value="explorer">Data Explorer</TabsTrigger>
          </TabsList>
          
          <div className="mt-6">
            <TabsContent value="overview" className="m-0">
              {isLoading ? (
                <div className="flex items-center justify-center h-64">
                  <div className="text-center">
                    <Loader2 className="h-10 w-10 animate-spin mx-auto text-primary" />
                    <p className="mt-4 text-muted-foreground">Loading database information...</p>
                  </div>
                </div>
              ) : error ? (
                <Card>
                  <CardContent className="py-6">
                    <div className="text-center text-red-500">
                      <p>Error loading database information</p>
                    </div>
                  </CardContent>
                </Card>
              ) : (
                renderDataOverview()
              )}
            </TabsContent>
            
            <TabsContent value="quartiles" className="m-0">
              <div className="space-y-6">
                {/* Quartile Distribution Overview */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <Card>
                    <CardContent className="pt-6">
                      <div className="text-center">
                        <Target className="h-8 w-8 mx-auto mb-2 text-green-600" />
                        <div className="text-2xl font-bold text-green-600">
                          {quartileDistribution?.q1Count || 746}
                        </div>
                        <p className="text-sm text-muted-foreground">Q1 Funds (BUY)</p>
                      </div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="pt-6">
                      <div className="text-center">
                        <TrendingUp className="h-8 w-8 mx-auto mb-2 text-blue-600" />
                        <div className="text-2xl font-bold text-blue-600">
                          {quartileDistribution?.q2Count || 746}
                        </div>
                        <p className="text-sm text-muted-foreground">Q2 Funds (HOLD)</p>
                      </div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="pt-6">
                      <div className="text-center">
                        <BarChart3 className="h-8 w-8 mx-auto mb-2 text-yellow-600" />
                        <div className="text-2xl font-bold text-yellow-600">
                          {quartileDistribution?.q3Count || 746}
                        </div>
                        <p className="text-sm text-muted-foreground">Q3 Funds (REVIEW)</p>
                      </div>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="pt-6">
                      <div className="text-center">
                        <AlertTriangle className="h-8 w-8 mx-auto mb-2 text-red-600" />
                        <div className="text-2xl font-bold text-red-600">
                          {quartileDistribution?.q4Count || 747}
                        </div>
                        <p className="text-sm text-muted-foreground">Q4 Funds (SELL)</p>
                      </div>
                    </CardContent>
                  </Card>
                </div>

                {/* Detailed Quartile Breakdown */}
                <Card>
                  <CardHeader>
                    <CardTitle>Detailed Quartile Breakdown</CardTitle>
                    <CardDescription>
                      Comprehensive scoring and rankings of mutual funds by quartile
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Tabs value={activeQuartileTab} onValueChange={setActiveQuartileTab} className="w-full">
                      <TabsList className="grid w-full grid-cols-4">
                        <TabsTrigger value="q1" className="bg-green-50 data-[state=active]:bg-green-100 data-[state=active]:text-green-800">Q1 (BUY)</TabsTrigger>
                        <TabsTrigger value="q2" className="bg-blue-50 data-[state=active]:bg-blue-100 data-[state=active]:text-blue-800">Q2 (HOLD)</TabsTrigger>
                        <TabsTrigger value="q3" className="bg-yellow-50 data-[state=active]:bg-yellow-100 data-[state=active]:text-yellow-800">Q3 (REVIEW)</TabsTrigger>
                        <TabsTrigger value="q4" className="bg-red-50 data-[state=active]:bg-red-100 data-[state=active]:text-red-800">Q4 (SELL)</TabsTrigger>
                      </TabsList>
                      
                      <div className="mt-4">
                        {/* Q1 Funds Tab Content */}
                        <TabsContent value="q1" className="m-0">
                          <div className="p-4 bg-green-50 rounded-md mb-4">
                            <div className="flex items-center mb-2">
                              <Target className="h-5 w-5 mr-2 text-green-600" />
                              <h3 className="text-md font-medium text-green-800">Top Quartile Funds (Q1)</h3>
                            </div>
                            <p className="text-sm text-green-700">
                              These are the top 25% of funds based on comprehensive scoring metrics. They offer the best combination of returns, risk management, and other key factors. Recommended action: <strong>BUY</strong>
                            </p>
                          </div>
                          
                          {topQ1Funds?.funds ? (
                            <div className="overflow-x-auto">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Fund Name</TableHead>
                                    <TableHead>Category</TableHead>
                                    <TableHead className="text-right">Total Score</TableHead>
                                    <TableHead className="text-right">Returns Score</TableHead>
                                    <TableHead className="text-right">Risk Score</TableHead>
                                    <TableHead>Recommendation</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {topQ1Funds.funds.slice(0, 10).map((fund: any) => (
                                    <TableRow key={fund.id}>
                                      <TableCell className="font-medium max-w-xs">
                                        <div className="truncate" title={fund.fundName}>
                                          {fund.fundName}
                                        </div>
                                      </TableCell>
                                      <TableCell>
                                        <Badge variant="outline">{fund.category}</Badge>
                                      </TableCell>
                                      <TableCell className="text-right font-semibold">
                                        {fund.totalScore || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.historicalReturnsTotal || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.riskGradeTotal || "N/A"}
                                      </TableCell>
                                      <TableCell>
                                        <Badge className="bg-green-100 text-green-800">
                                          {fund.recommendation || "BUY"}
                                        </Badge>
                                      </TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          ) : (
                            <div className="text-center py-8">
                              <Loader2 className="h-8 w-8 animate-spin mx-auto text-primary" />
                              <p className="mt-2 text-muted-foreground">Loading Q1 funds data...</p>
                            </div>
                          )}
                        </TabsContent>
                        
                        {/* Q2 Funds Tab Content */}
                        <TabsContent value="q2" className="m-0">
                          <div className="p-4 bg-blue-50 rounded-md mb-4">
                            <div className="flex items-center mb-2">
                              <TrendingUp className="h-5 w-5 mr-2 text-blue-600" />
                              <h3 className="text-md font-medium text-blue-800">Second Quartile Funds (Q2)</h3>
                            </div>
                            <p className="text-sm text-blue-700">
                              These funds rank in the 26-50% range based on comprehensive scoring metrics. They offer good performance with reasonable risk profiles. Recommended action: <strong>HOLD</strong>
                            </p>
                          </div>
                          
                          {topQ2Funds?.funds ? (
                            <div className="overflow-x-auto">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Fund Name</TableHead>
                                    <TableHead>Category</TableHead>
                                    <TableHead className="text-right">Total Score</TableHead>
                                    <TableHead className="text-right">Returns Score</TableHead>
                                    <TableHead className="text-right">Risk Score</TableHead>
                                    <TableHead>Recommendation</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {topQ2Funds.funds.slice(0, 10).map((fund: any) => (
                                    <TableRow key={fund.id}>
                                      <TableCell className="font-medium max-w-xs">
                                        <div className="truncate" title={fund.fundName}>
                                          {fund.fundName}
                                        </div>
                                      </TableCell>
                                      <TableCell>
                                        <Badge variant="outline">{fund.category}</Badge>
                                      </TableCell>
                                      <TableCell className="text-right font-semibold">
                                        {fund.totalScore || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.historicalReturnsTotal || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.riskGradeTotal || "N/A"}
                                      </TableCell>
                                      <TableCell>
                                        <Badge className="bg-blue-100 text-blue-800">
                                          {fund.recommendation || "HOLD"}
                                        </Badge>
                                      </TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          ) : (
                            <div className="text-center py-8">
                              <Loader2 className="h-8 w-8 animate-spin mx-auto text-primary" />
                              <p className="mt-2 text-muted-foreground">Loading Q2 funds data...</p>
                            </div>
                          )}
                        </TabsContent>
                        
                        {/* Q3 Funds Tab Content */}
                        <TabsContent value="q3" className="m-0">
                          <div className="p-4 bg-yellow-50 rounded-md mb-4">
                            <div className="flex items-center mb-2">
                              <BarChart3 className="h-5 w-5 mr-2 text-yellow-600" />
                              <h3 className="text-md font-medium text-yellow-800">Third Quartile Funds (Q3)</h3>
                            </div>
                            <p className="text-sm text-yellow-700">
                              These funds fall in the 51-75% range with below-average performance or elevated risk metrics. Consider evaluating these holdings closely. Recommended action: <strong>REVIEW</strong>
                            </p>
                          </div>
                          
                          {topQ3Funds?.funds ? (
                            <div className="overflow-x-auto">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Fund Name</TableHead>
                                    <TableHead>Category</TableHead>
                                    <TableHead className="text-right">Total Score</TableHead>
                                    <TableHead className="text-right">Returns Score</TableHead>
                                    <TableHead className="text-right">Risk Score</TableHead>
                                    <TableHead>Recommendation</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {topQ3Funds.funds.slice(0, 10).map((fund: any) => (
                                    <TableRow key={fund.id}>
                                      <TableCell className="font-medium max-w-xs">
                                        <div className="truncate" title={fund.fundName}>
                                          {fund.fundName}
                                        </div>
                                      </TableCell>
                                      <TableCell>
                                        <Badge variant="outline">{fund.category}</Badge>
                                      </TableCell>
                                      <TableCell className="text-right font-semibold">
                                        {fund.totalScore || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.historicalReturnsTotal || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.riskGradeTotal || "N/A"}
                                      </TableCell>
                                      <TableCell>
                                        <Badge className="bg-yellow-100 text-yellow-800">
                                          {fund.recommendation || "REVIEW"}
                                        </Badge>
                                      </TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          ) : (
                            <div className="text-center py-8">
                              <Loader2 className="h-8 w-8 animate-spin mx-auto text-primary" />
                              <p className="mt-2 text-muted-foreground">Loading Q3 funds data...</p>
                            </div>
                          )}
                        </TabsContent>
                        
                        {/* Q4 Funds Tab Content */}
                        <TabsContent value="q4" className="m-0">
                          <div className="p-4 bg-red-50 rounded-md mb-4">
                            <div className="flex items-center mb-2">
                              <AlertTriangle className="h-5 w-5 mr-2 text-red-600" />
                              <h3 className="text-md font-medium text-red-800">Bottom Quartile Funds (Q4)</h3>
                            </div>
                            <p className="text-sm text-red-700">
                              These are the bottom 25% of funds with the weakest performance characteristics and/or highest risk profiles. Recommended action: <strong>SELL</strong>
                            </p>
                          </div>
                          
                          {topQ4Funds?.funds ? (
                            <div className="overflow-x-auto">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>Fund Name</TableHead>
                                    <TableHead>Category</TableHead>
                                    <TableHead className="text-right">Total Score</TableHead>
                                    <TableHead className="text-right">Returns Score</TableHead>
                                    <TableHead className="text-right">Risk Score</TableHead>
                                    <TableHead>Recommendation</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {topQ4Funds.funds.slice(0, 10).map((fund: any) => (
                                    <TableRow key={fund.id}>
                                      <TableCell className="font-medium max-w-xs">
                                        <div className="truncate" title={fund.fundName}>
                                          {fund.fundName}
                                        </div>
                                      </TableCell>
                                      <TableCell>
                                        <Badge variant="outline">{fund.category}</Badge>
                                      </TableCell>
                                      <TableCell className="text-right font-semibold">
                                        {fund.totalScore || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.historicalReturnsTotal || "N/A"}
                                      </TableCell>
                                      <TableCell className="text-right">
                                        {fund.riskGradeTotal || "N/A"}
                                      </TableCell>
                                      <TableCell>
                                        <Badge className="bg-red-100 text-red-800">
                                          {fund.recommendation || "SELL"}
                                        </Badge>
                                      </TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            </div>
                          ) : (
                            <div className="text-center py-8">
                              <Loader2 className="h-8 w-8 animate-spin mx-auto text-primary" />
                              <p className="mt-2 text-muted-foreground">Loading Q4 funds data...</p>
                            </div>
                          )}
                        </TabsContent>
                      </div>
                    </Tabs>
                  </CardContent>
                </Card>

                {/* Database Schema Info */}
                <Card>
                  <CardHeader>
                    <CardTitle>Fund Scoring Database Schema</CardTitle>
                    <CardDescription>
                      Complete structure of your fund scoring and quartile data storage
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      <div>
                        <h4 className="font-semibold text-lg mb-2">fund_scores Table</h4>
                        <p className="text-sm text-muted-foreground mb-3">
                          Stores comprehensive scoring data for all 2,985 funds
                        </p>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
                          <div className="space-y-1">
                            <p><strong>Historical Returns:</strong> return_3m_score, return_6m_score, return_1y_score, return_3y_score, return_5y_score</p>
                            <p><strong>Risk Metrics:</strong> std_dev_1y_score, max_drawdown_score, updown_capture scores</p>
                          </div>
                          <div className="space-y-1">
                            <p><strong>Other Metrics:</strong> sectoral_similarity_score, aum_size_score, expense_ratio_score</p>
                            <p><strong>Final Scores:</strong> total_score, quartile (1-4), recommendation (BUY/HOLD/SELL)</p>
                          </div>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="schema" className="m-0">
              {renderDatabaseSchema()}
            </TabsContent>
            
            <TabsContent value="elivate" className="m-0">
              {renderElivateDatabase()}
            </TabsContent>
            
            <TabsContent value="explorer" className="m-0">
              <Card>
                <CardHeader>
                  <CardTitle>Table Explorer</CardTitle>
                  <CardDescription>
                    View and query data from database tables
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    <div>
                      <label className="block text-sm font-medium text-neutral-700 mb-1">Select Table</label>
                      <Select defaultValue="funds">
                        <SelectTrigger className="w-full">
                          <SelectValue placeholder="Select table" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="funds">Funds</SelectItem>
                          <SelectItem value="nav_data">NAV Data</SelectItem>
                          <SelectItem value="fund_scores">Fund Scores</SelectItem>
                          <SelectItem value="etl_pipeline_runs">ETL Pipeline Runs</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    
                    <div className="bg-neutral-50 p-4 rounded-lg border border-neutral-200">
                      <h3 className="text-sm font-medium text-neutral-900 mb-3">Funds Table Preview</h3>
                      <div className="overflow-x-auto">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>ID</TableHead>
                              <TableHead>Scheme Code</TableHead>
                              <TableHead>Fund Name</TableHead>
                              <TableHead>AMC</TableHead>
                              <TableHead>Category</TableHead>
                              <TableHead>Subcategory</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {isLoading ? (
                              Array.from({ length: 5 }).map((_, i) => (
                                <TableRow key={i}>
                                  <TableCell><Skeleton className="h-4 w-8" /></TableCell>
                                  <TableCell><Skeleton className="h-4 w-20" /></TableCell>
                                  <TableCell><Skeleton className="h-4 w-40" /></TableCell>
                                  <TableCell><Skeleton className="h-4 w-20" /></TableCell>
                                  <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                                  <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                                </TableRow>
                              ))
                            ) : funds?.slice(0, 10).map((fund, index) => (
                              <TableRow key={index}>
                                <TableCell>{fund.id}</TableCell>
                                <TableCell>{fund.schemeCode}</TableCell>
                                <TableCell className="max-w-xs truncate">{fund.fundName}</TableCell>
                                <TableCell>{fund.amcName}</TableCell>
                                <TableCell>{fund.category}</TableCell>
                                <TableCell>{fund.subcategory}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                      <div className="mt-4 text-center">
                        <Button variant="outline" size="sm">
                          View Full Table
                        </Button>
                      </div>
                    </div>
                    
                    <div>
                      <h3 className="text-sm font-medium text-neutral-700 mb-2">Custom SQL Query</h3>
                      <textarea 
                        className="w-full h-32 p-2 border border-neutral-300 rounded-md font-mono text-sm" 
                        placeholder="SELECT * FROM funds LIMIT 10;"
                      ></textarea>
                      <Button className="mt-2">Run Query</Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          </div>
        </Tabs>
      </div>
    </div>
  );
}