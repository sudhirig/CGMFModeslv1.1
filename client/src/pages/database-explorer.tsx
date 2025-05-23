import React, { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { useAllFunds } from "@/hooks/use-all-funds";
import { Loader2, Database, Table as TableIcon, BarChart3, Target, TrendingUp } from "lucide-react";
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

  const { data: topQ1Funds } = useQuery({
    queryKey: ['/api/quartile/funds/1'],
    staleTime: 5 * 60 * 1000,
  });

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
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Database Explorer</h1>
          <p className="mt-1 text-sm text-neutral-500">
            Explore and analyze the mutual fund and ELIVATE framework databases
          </p>
        </div>
        
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full md:w-[500px] grid-cols-4">
            <TabsTrigger value="overview">Overview</TabsTrigger>
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