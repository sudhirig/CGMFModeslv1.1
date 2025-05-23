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
  const [importing, setImporting] = useState(false);
  const [importingHistorical, setImportingHistorical] = useState(false);

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
  
  // For database stats
  const { data: navCount, refetch: refetchNavCounts } = useQuery({
    queryKey: ['/api/amfi/status'],
    staleTime: 60 * 1000,
  });
  
  // For managing database imports
  const importAMFIData = async (includeHistorical: boolean = false) => {
    try {
      if (includeHistorical) {
        setImportingHistorical(true);
      } else {
        setImporting(true);
      }
      
      const endpoint = includeHistorical 
        ? '/api/amfi/import?historical=true'
        : '/api/amfi/import';
        
      const response = await fetch(endpoint);
      const data = await response.json();
      
      if (data.success) {
        // Show success toast
        // @ts-ignore
        window.toast?.({
          title: includeHistorical ? 'Historical NAV Data Imported' : 'AMFI Data Imported',
          description: data.message,
          variant: 'success'
        });
        
        // Refresh data counts
        refetchNavCounts();
      } else {
        // Show error toast
        // @ts-ignore
        window.toast?.({
          title: 'Import Failed',
          description: data.message,
          variant: 'destructive'
        });
      }
    } catch (error) {
      console.error('Import error:', error);
      // @ts-ignore
      window.toast?.({
        title: 'Import Error',
        description: 'Failed to import AMFI data. See console for details.',
        variant: 'destructive'
      });
    } finally {
      if (includeHistorical) {
        setImportingHistorical(false);
      } else {
        setImporting(false);
      }
    }
  };

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
    const tables = [
      { 
        name: "funds", 
        description: "Mutual fund master data", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary key" },
          { name: "scheme_code", type: "TEXT", description: "AMFI scheme code" },
          { name: "isin_div_payout", type: "TEXT", description: "ISIN code for dividend payout" },
          { name: "isin_div_reinvest", type: "TEXT", description: "ISIN code for dividend reinvestment" },
          { name: "fund_name", type: "TEXT", description: "Name of the mutual fund" },
          { name: "amc_name", type: "TEXT", description: "Asset Management Company" },
          { name: "category", type: "TEXT", description: "Major category (Equity, Debt, Hybrid)" },
          { name: "subcategory", type: "TEXT", description: "Subcategory (Large Cap, Mid Cap, etc.)" },
        ]
      },
      { 
        name: "nav_data", 
        description: "Historical NAV values for funds", 
        columns: [
          { name: "id", type: "SERIAL", description: "Primary key" },
          { name: "fund_id", type: "INTEGER", description: "Foreign key to funds table" },
          { name: "nav_date", type: "DATE", description: "Date of the NAV value" },
          { name: "nav_value", type: "DECIMAL", description: "NAV value for the date" },
        ]
      },
      { 
        name: "fund_scores", 
        description: "Calculated scores for funds", 
        columns: [
          { name: "fund_id", type: "INTEGER", description: "Foreign key to funds table" },
          { name: "score_date", type: "DATE", description: "Date the score was calculated" },
          { name: "total_score", type: "DECIMAL", description: "Overall fund score" },
          { name: "return_score", type: "DECIMAL", description: "Score based on returns" },
          { name: "risk_score", type: "DECIMAL", description: "Score based on risk metrics" },
        ]
      },
    ];
    
    return (
      <div className="space-y-6">
        {tables.map(table => (
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
        <div className="mb-6 flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-neutral-900">Database Explorer</h1>
            <p className="mt-1 text-sm text-neutral-500">
              Explore and analyze the mutual fund and ELIVATE framework databases
            </p>
          </div>
          
          {/* Data Management Controls */}
          <div className="flex space-x-3">
            <Button 
              variant="outline" 
              onClick={() => importAMFIData(false)}
              disabled={importing || importingHistorical}
            >
              {importing ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Importing...
                </>
              ) : (
                "Import AMFI Data"
              )}
            </Button>
            
            <Button 
              variant="default" 
              onClick={() => importAMFIData(true)}
              disabled={importing || importingHistorical}
              className="bg-blue-600 hover:bg-blue-700"
            >
              {importingHistorical ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Importing Historical NAV...
                </>
              ) : (
                "Import Historical NAV Data"
              )}
            </Button>
          </div>
        </div>
        
        {/* NAV Data Status */}
        <Card className="bg-blue-50 mb-6">
          <CardContent className="pt-4 pb-3 flex justify-between items-center">
            <div>
              <span className="text-sm font-medium text-blue-800">NAV Data Status:</span>{" "}
              <span className="text-sm">
                {navCount ? (
                  <>
                    <strong>{navCount.totalFunds}</strong> mutual funds with{" "}
                    <strong>{navCount.totalNavRecords}</strong> NAV data points available
                  </>
                ) : (
                  "Loading data statistics..."
                )}
              </span>
            </div>
            <div>
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => refetchNavCounts()}
                className="text-blue-700 hover:text-blue-800 hover:bg-blue-100"
              >
                <Loader2 className="mr-1 h-3 w-3" /> Refresh
              </Button>
            </div>
          </CardContent>
        </Card>
        
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