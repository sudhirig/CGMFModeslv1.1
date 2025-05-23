import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useFunds } from "@/hooks/use-funds";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function FundAnalysis() {
  const [selectedCategory, setSelectedCategory] = useState<string>("All Categories");
  
  // Update the endpoint when category changes
  const updateCategoryEndpoint = (category: string) => {
    const newEndpoint = `/api/funds${category !== 'All Categories' ? `?category=${category}` : ''}`;
    setEndpoint(newEndpoint);
    console.log("Setting endpoint to:", newEndpoint);
  };
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [selectedFund, setSelectedFund] = useState<any>(null);
  
  const [endpoint, setEndpoint] = useState<string>("/api/funds");
  
  const { funds, isLoading, error, refetch } = useFunds(endpoint);
  
  // Filter funds based on search query
  const filteredFunds = funds?.filter(fund => 
    fund.fundName.toLowerCase().includes(searchQuery.toLowerCase()) ||
    fund.amcName.toLowerCase().includes(searchQuery.toLowerCase())
  );
  
  const handleFundSelect = (fundId: number) => {
    const fund = funds?.find(f => f.id === fundId);
    if (fund) {
      setSelectedFund(fund);
    }
  };
  
  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Fund Analysis</h1>
          <p className="mt-1 text-sm text-neutral-500">
            Detailed analysis and comparison of mutual funds
          </p>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Fund Filter Panel */}
          <div className="md:col-span-1">
            <Card>
              <CardHeader>
                <CardTitle>Fund Filters</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <label className="text-sm font-medium text-neutral-700">Category</label>
                    <Select 
                      value={selectedCategory} 
                      onValueChange={(category) => {
                        setSelectedCategory(category);
                        updateCategoryEndpoint(category);
                      }}
                    >
                      <SelectTrigger className="w-full mt-1">
                        <SelectValue placeholder="Select a category" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="All Categories">All Categories</SelectItem>
                        <SelectItem value="Equity">Equity</SelectItem>
                        <SelectItem value="Debt">Debt</SelectItem>
                        <SelectItem value="Hybrid">Hybrid</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  
                  <div>
                    <label className="text-sm font-medium text-neutral-700">Search</label>
                    <Input
                      type="text"
                      placeholder="Search by fund or AMC name"
                      className="mt-1"
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                    />
                  </div>
                  
                  <div className="pt-4">
                    <label className="text-sm font-medium text-neutral-700 mb-2 block">Funds</label>
                    
                    {isLoading ? (
                      <div className="text-center py-4">Loading funds...</div>
                    ) : error ? (
                      <div className="text-center py-4 text-red-500">Error loading funds</div>
                    ) : filteredFunds?.length === 0 ? (
                      <div className="text-center py-4">No funds found</div>
                    ) : (
                      <div className="space-y-2 max-h-96 overflow-y-auto pr-2">
                        {filteredFunds?.map((fund) => (
                          <div
                            key={fund.id}
                            className={`p-3 rounded-lg cursor-pointer border ${
                              selectedFund?.id === fund.id
                                ? "border-primary-500 bg-primary-50"
                                : "border-neutral-200 hover:bg-neutral-50"
                            }`}
                            onClick={() => handleFundSelect(fund.id)}
                          >
                            <div className="font-medium text-sm">{fund.fundName}</div>
                            <div className="text-xs text-neutral-500">{fund.amcName}</div>
                            <div className="text-xs text-neutral-400 mt-1">{fund.category}</div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                  
                  <div className="mt-4 border-t pt-4">
                    <h3 className="text-sm font-medium text-neutral-700 mb-2">Data Management</h3>
                    <Button 
                      variant="outline" 
                      className="w-full"
                      onClick={async () => {
                        try {
                          if (confirm("This will import around 3,000 mutual funds with real data. It may take a moment to process. Continue?")) {
                            const response = await fetch('/api/import/amfi-data', {
                              method: 'POST',
                              headers: {
                                'Content-Type': 'application/json'
                              }
                            });
                            
                            const result = await response.json();
                            
                            if (result.success) {
                              alert(`Successfully imported mutual fund data! ${result.counts?.importedFunds || 'Many'} funds are now available.`);
                              // Refresh the fund list
                              refetch();
                            } else {
                              alert(`Failed to import data: ${result.message || 'Unknown error'}`);
                            }
                          }
                        } catch (error) {
                          console.error('Error importing AMFI data:', error);
                          alert('Failed to import mutual fund data. Please try again later.');
                        }
                      }}
                    >
                      Import Real Mutual Fund Data (3,000+ Funds)
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
          
          {/* Fund Analysis Panel */}
          <div className="md:col-span-2">
            {selectedFund ? (
              <Card>
                <CardHeader>
                  <CardTitle>{selectedFund.fundName}</CardTitle>
                  <div className="text-sm text-neutral-500">{selectedFund.amcName} • {selectedFund.category}</div>
                </CardHeader>
                <CardContent>
                  <Tabs defaultValue="overview">
                    <TabsList className="mb-4">
                      <TabsTrigger value="overview">Overview</TabsTrigger>
                      <TabsTrigger value="performance">Performance</TabsTrigger>
                      <TabsTrigger value="holdings">Holdings</TabsTrigger>
                      <TabsTrigger value="risk">Risk Analysis</TabsTrigger>
                    </TabsList>
                    
                    <TabsContent value="overview">
                      <div className="space-y-6">
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-sm font-medium text-neutral-500">Total Score</div>
                            <div className="text-2xl font-semibold text-neutral-900">86.5</div>
                            <div className="mt-1 text-xs font-medium text-primary-600">Quartile 1</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-sm font-medium text-neutral-500">Recommendation</div>
                            <div className="mt-1">
                              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                                BUY
                              </span>
                            </div>
                          </div>
                          
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-sm font-medium text-neutral-500">AUM</div>
                            <div className="text-2xl font-semibold text-neutral-900">₹26,456 Cr</div>
                          </div>
                        </div>
                        
                        <div>
                          <h3 className="text-base font-medium text-neutral-900 mb-3">Score Breakdown</h3>
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div>
                              <div className="flex justify-between text-sm">
                                <span className="font-medium text-neutral-700">Historical Returns</span>
                                <span className="text-neutral-900">36.5/40</span>
                              </div>
                              <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                <div className="bg-primary-500 h-2 rounded-full" style={{ width: "91.25%" }}></div>
                              </div>
                            </div>
                            
                            <div>
                              <div className="flex justify-between text-sm">
                                <span className="font-medium text-neutral-700">Risk Grade</span>
                                <span className="text-neutral-900">24.8/30</span>
                              </div>
                              <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                <div className="bg-primary-500 h-2 rounded-full" style={{ width: "82.67%" }}></div>
                              </div>
                            </div>
                            
                            <div>
                              <div className="flex justify-between text-sm">
                                <span className="font-medium text-neutral-700">Other Metrics</span>
                                <span className="text-neutral-900">25.2/30</span>
                              </div>
                              <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                <div className="bg-primary-500 h-2 rounded-full" style={{ width: "84%" }}></div>
                              </div>
                            </div>
                          </div>
                        </div>
                        
                        <div>
                          <h3 className="text-base font-medium text-neutral-900 mb-3">Fund Information</h3>
                          <div className="bg-neutral-50 rounded-lg p-4">
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                              <div>
                                <div className="text-xs font-medium text-neutral-500">Scheme Code</div>
                                <div className="text-sm font-medium text-neutral-900">{selectedFund.schemeCode}</div>
                              </div>
                              
                              <div>
                                <div className="text-xs font-medium text-neutral-500">Inception Date</div>
                                <div className="text-sm font-medium text-neutral-900">
                                  {selectedFund.inceptionDate 
                                    ? new Date(selectedFund.inceptionDate).toLocaleDateString() 
                                    : "N/A"}
                                </div>
                              </div>
                              
                              <div>
                                <div className="text-xs font-medium text-neutral-500">Expense Ratio</div>
                                <div className="text-sm font-medium text-neutral-900">
                                  {selectedFund.expenseRatio 
                                    ? `${selectedFund.expenseRatio}%` 
                                    : "N/A"}
                                </div>
                              </div>
                              
                              <div>
                                <div className="text-xs font-medium text-neutral-500">Exit Load</div>
                                <div className="text-sm font-medium text-neutral-900">
                                  {selectedFund.exitLoad 
                                    ? `${selectedFund.exitLoad}%` 
                                    : "N/A"}
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </TabsContent>
                    
                    <TabsContent value="performance">
                      <div className="space-y-6">
                        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-center">
                          <div className="bg-neutral-50 p-3 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">1M Return</div>
                            <div className="text-base font-semibold text-success">+3.2%</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-3 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">3M Return</div>
                            <div className="text-base font-semibold text-success">+8.5%</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-3 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">6M Return</div>
                            <div className="text-base font-semibold text-success">+14.2%</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-3 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">1Y Return</div>
                            <div className="text-base font-semibold text-success">+22.8%</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-3 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">3Y Return</div>
                            <div className="text-base font-semibold text-success">+18.3%</div>
                          </div>
                        </div>
                        
                        <div>
                          <h3 className="text-base font-medium text-neutral-900 mb-3">Performance Chart</h3>
                          <div className="h-64 bg-neutral-50 rounded-lg p-4">
                            <ResponsiveContainer width="100%" height="100%">
                              <LineChart
                                data={[
                                  { date: 'Jan', fund: 100, index: 100 },
                                  { date: 'Feb', fund: 105, index: 103 },
                                  { date: 'Mar', fund: 110, index: 107 },
                                  { date: 'Apr', fund: 108, index: 105 },
                                  { date: 'May', fund: 112, index: 108 },
                                  { date: 'Jun', fund: 118, index: 112 },
                                  { date: 'Jul', fund: 125, index: 116 },
                                  { date: 'Aug', fund: 122, index: 118 },
                                  { date: 'Sep', fund: 128, index: 121 },
                                  { date: 'Oct', fund: 132, index: 123 },
                                  { date: 'Nov', fund: 130, index: 124 },
                                  { date: 'Dec', fund: 134, index: 126 },
                                ]}
                                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                              >
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="date" />
                                <YAxis />
                                <Tooltip />
                                <Legend />
                                <Line type="monotone" dataKey="fund" stroke="#2271fa" activeDot={{ r: 8 }} name={selectedFund.fundName} />
                                <Line type="monotone" dataKey="index" stroke="#68aff4" name="Benchmark" />
                              </LineChart>
                            </ResponsiveContainer>
                          </div>
                        </div>
                      </div>
                    </TabsContent>
                    
                    <TabsContent value="holdings">
                      <div className="space-y-6">
                        <div className="overflow-x-auto">
                          <table className="min-w-full divide-y divide-neutral-200">
                            <thead>
                              <tr>
                                <th className="px-3 py-2 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Stock</th>
                                <th className="px-3 py-2 bg-neutral-50 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Allocation %</th>
                                <th className="px-3 py-2 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Sector</th>
                                <th className="px-3 py-2 bg-neutral-50 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Market Cap</th>
                              </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-neutral-200">
                              <tr>
                                <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-neutral-900">HDFC Bank</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">8.5%</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-neutral-500">Banking & Finance</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">Large Cap</td>
                              </tr>
                              <tr>
                                <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-neutral-900">Reliance Industries</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">7.2%</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-neutral-500">Oil & Gas</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">Large Cap</td>
                              </tr>
                              <tr>
                                <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-neutral-900">Infosys</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">6.8%</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-neutral-500">Technology</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">Large Cap</td>
                              </tr>
                              <tr>
                                <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-neutral-900">ICICI Bank</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">5.9%</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-neutral-500">Banking & Finance</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">Large Cap</td>
                              </tr>
                              <tr>
                                <td className="px-3 py-2 whitespace-nowrap text-sm font-medium text-neutral-900">TCS</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">4.7%</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-neutral-500">Technology</td>
                                <td className="px-3 py-2 whitespace-nowrap text-sm text-right">Large Cap</td>
                              </tr>
                            </tbody>
                          </table>
                        </div>
                        
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                          <div>
                            <h3 className="text-base font-medium text-neutral-900 mb-3">Sector Allocation</h3>
                            <div className="space-y-2">
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Banking & Finance</span>
                                  <span className="text-neutral-900">28.4%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-primary-500 h-2 rounded-full" style={{ width: "28.4%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Technology</span>
                                  <span className="text-neutral-900">15.2%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-info h-2 rounded-full" style={{ width: "15.2%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Consumer Goods</span>
                                  <span className="text-neutral-900">12.8%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-success h-2 rounded-full" style={{ width: "12.8%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Automobile</span>
                                  <span className="text-neutral-900">9.6%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-warning h-2 rounded-full" style={{ width: "9.6%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Others</span>
                                  <span className="text-neutral-900">34.0%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-neutral-500 h-2 rounded-full" style={{ width: "34%" }}></div>
                                </div>
                              </div>
                            </div>
                          </div>
                          
                          <div>
                            <h3 className="text-base font-medium text-neutral-900 mb-3">Market Cap Allocation</h3>
                            <div className="space-y-2">
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Large Cap</span>
                                  <span className="text-neutral-900">68.5%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-primary-500 h-2 rounded-full" style={{ width: "68.5%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Mid Cap</span>
                                  <span className="text-neutral-900">24.3%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-info h-2 rounded-full" style={{ width: "24.3%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="flex justify-between text-sm">
                                  <span className="font-medium text-neutral-700">Small Cap</span>
                                  <span className="text-neutral-900">7.2%</span>
                                </div>
                                <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-success h-2 rounded-full" style={{ width: "7.2%" }}></div>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </TabsContent>
                    
                    <TabsContent value="risk">
                      <div className="space-y-6">
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">Standard Deviation (1Y)</div>
                            <div className="text-lg font-semibold text-neutral-900">15.8%</div>
                            <div className="mt-1 text-xs text-neutral-500">Category Avg: 17.2%</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">Beta (1Y)</div>
                            <div className="text-lg font-semibold text-neutral-900">0.92</div>
                            <div className="mt-1 text-xs text-neutral-500">vs Benchmark</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">Sharpe Ratio (3Y)</div>
                            <div className="text-lg font-semibold text-neutral-900">1.24</div>
                            <div className="mt-1 text-xs text-neutral-500">Category Avg: 1.05</div>
                          </div>
                          
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="text-xs font-medium text-neutral-500">Maximum Drawdown</div>
                            <div className="text-lg font-semibold text-neutral-900">18.4%</div>
                            <div className="mt-1 text-xs text-neutral-500">Last 3 Years</div>
                          </div>
                        </div>
                        
                        <div>
                          <h3 className="text-base font-medium text-neutral-900 mb-3">Up/Down Capture Ratio</h3>
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                              <div>
                                <div className="text-sm font-medium text-neutral-700 mb-2">Up Capture (1Y)</div>
                                <div className="flex items-center">
                                  <div className="text-lg font-semibold text-neutral-900">105.2%</div>
                                  <div className="ml-2 text-xs text-success">+5.2% vs Benchmark</div>
                                </div>
                                <div className="mt-2 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-success h-2 rounded-full" style={{ width: "105.2%" }}></div>
                                </div>
                              </div>
                              
                              <div>
                                <div className="text-sm font-medium text-neutral-700 mb-2">Down Capture (1Y)</div>
                                <div className="flex items-center">
                                  <div className="text-lg font-semibold text-neutral-900">92.7%</div>
                                  <div className="ml-2 text-xs text-success">-7.3% vs Benchmark</div>
                                </div>
                                <div className="mt-2 w-full bg-neutral-200 rounded-full h-2">
                                  <div className="bg-primary-500 h-2 rounded-full" style={{ width: "92.7%" }}></div>
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                        
                        <div>
                          <h3 className="text-base font-medium text-neutral-900 mb-3">Risk Assessment</h3>
                          <div className="bg-neutral-50 p-4 rounded-lg">
                            <div className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-success bg-opacity-10 text-success mb-3">
                              Low Risk
                            </div>
                            <p className="text-sm text-neutral-700">
                              This fund has shown lower volatility compared to its benchmark and category peers,
                              with better downside protection during market corrections. The risk-adjusted returns
                              are favorable with a strong Sharpe ratio, indicating good returns for the level of risk taken.
                            </p>
                          </div>
                        </div>
                      </div>
                    </TabsContent>
                  </Tabs>
                </CardContent>
              </Card>
            ) : (
              <div className="flex items-center justify-center h-96 bg-neutral-50 rounded-lg">
                <div className="text-center">
                  <div className="material-icons text-4xl text-neutral-400 mb-2">search</div>
                  <h3 className="text-lg font-medium text-neutral-900">Select a Fund</h3>
                  <p className="text-sm text-neutral-500 mt-1">
                    Choose a fund from the list to view detailed analysis
                  </p>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
