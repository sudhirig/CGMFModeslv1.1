import React, { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Loader2, Search, Download, Calendar, TrendingUp } from 'lucide-react';

interface SchemeData {
  schemeCode: string;
  schemeName: string;
}

interface NAVData {
  date: string;
  nav: string;
}

interface HistoricalData {
  meta: {
    scheme_name: string;
    scheme_code: string;
    scheme_category: string;
    fund_house: string;
  };
  data: NAVData[];
}

export default function MFAPITest() {
  const [searchTerm, setSearchTerm] = useState('');
  const [schemes, setSchemes] = useState<SchemeData[]>([]);
  const [selectedScheme, setSelectedScheme] = useState<string>('');
  const [historicalData, setHistoricalData] = useState<HistoricalData | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [error, setError] = useState<string>('');

  const searchSchemes = async () => {
    if (!searchTerm.trim()) return;
    
    setSearchLoading(true);
    setError('');
    
    try {
      const response = await fetch(`https://api.mfapi.in/mf/search?q=${encodeURIComponent(searchTerm)}`);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: Failed to search schemes`);
      }
      
      const data = await response.json();
      setSchemes(data || []);
      
      if (!data || data.length === 0) {
        setError('No schemes found for the search term');
      }
    } catch (err: any) {
      setError(`Search failed: ${err.message}`);
      setSchemes([]);
    } finally {
      setSearchLoading(false);
    }
  };

  const fetchHistoricalData = async (schemeCode: string) => {
    setLoading(true);
    setError('');
    setHistoricalData(null);
    
    try {
      const response = await fetch(`https://api.mfapi.in/mf/${schemeCode}`);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: Failed to fetch historical data`);
      }
      
      const data = await response.json();
      setHistoricalData(data);
      
      if (!data || !data.data || data.data.length === 0) {
        setError('No historical data available for this scheme');
      }
    } catch (err: any) {
      setError(`Failed to fetch data: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const getDataStats = () => {
    if (!historicalData?.data) return null;
    
    const data = historicalData.data;
    const totalRecords = data.length;
    const latestDate = data[0]?.date;
    const oldestDate = data[data.length - 1]?.date;
    const latestNAV = data[0]?.nav;
    
    return { totalRecords, latestDate, oldestDate, latestNAV };
  };

  const stats = getDataStats();

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">MFAPI.in Test</h1>
          <p className="text-gray-600 mt-2">
            Test historical NAV data extraction using MFAPI.in API
          </p>
        </div>
      </div>

      {/* Search Section */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            Search Mutual Fund Schemes
          </CardTitle>
          <CardDescription>
            Enter fund name or keyword to search for schemes
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex gap-4">
            <div className="flex-1">
              <Label htmlFor="search">Search Term</Label>
              <Input
                id="search"
                placeholder="e.g., HDFC, SBI, Axis"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && searchSchemes()}
              />
            </div>
            <div className="flex items-end">
              <Button 
                onClick={searchSchemes} 
                disabled={searchLoading || !searchTerm.trim()}
              >
                {searchLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                ) : (
                  <Search className="h-4 w-4 mr-2" />
                )}
                Search
              </Button>
            </div>
          </div>

          {/* Search Results */}
          {schemes.length > 0 && (
            <div className="space-y-2">
              <Label>Found {schemes.length} schemes:</Label>
              <div className="max-h-60 overflow-y-auto space-y-2">
                {schemes.map((scheme) => (
                  <div
                    key={scheme.schemeCode}
                    className={`p-3 border rounded cursor-pointer hover:bg-gray-50 ${
                      selectedScheme === scheme.schemeCode ? 'border-blue-500 bg-blue-50' : 'border-gray-200'
                    }`}
                    onClick={() => setSelectedScheme(scheme.schemeCode)}
                  >
                    <div className="font-medium text-sm">{scheme.schemeName}</div>
                    <div className="text-xs text-gray-500">Code: {scheme.schemeCode}</div>
                  </div>
                ))}
              </div>
              
              {selectedScheme && (
                <Button 
                  onClick={() => fetchHistoricalData(selectedScheme)}
                  disabled={loading}
                  className="mt-4"
                >
                  {loading ? (
                    <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  ) : (
                    <Download className="h-4 w-4 mr-2" />
                  )}
                  Fetch Historical Data
                </Button>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Data Display */}
      {historicalData && (
        <div className="space-y-6">
          {/* Scheme Information */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                Scheme Information
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <Label className="text-sm font-medium text-gray-600">Scheme Name</Label>
                  <p className="text-lg font-semibold">{historicalData.meta.scheme_name}</p>
                </div>
                <div>
                  <Label className="text-sm font-medium text-gray-600">Scheme Code</Label>
                  <p className="font-mono">{historicalData.meta.scheme_code}</p>
                </div>
                <div>
                  <Label className="text-sm font-medium text-gray-600">Category</Label>
                  <Badge variant="secondary">{historicalData.meta.scheme_category}</Badge>
                </div>
                <div>
                  <Label className="text-sm font-medium text-gray-600">Fund House</Label>
                  <p>{historicalData.meta.fund_house}</p>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Data Statistics */}
          {stats && (
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Calendar className="h-5 w-5" />
                  Data Statistics
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="text-center">
                    <p className="text-2xl font-bold text-blue-600">{stats.totalRecords.toLocaleString()}</p>
                    <p className="text-sm text-gray-600">Total Records</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-bold text-green-600">₹{stats.latestNAV}</p>
                    <p className="text-sm text-gray-600">Latest NAV</p>
                  </div>
                  <div className="text-center">
                    <p className="text-lg font-semibold">{stats.latestDate}</p>
                    <p className="text-sm text-gray-600">Latest Date</p>
                  </div>
                  <div className="text-center">
                    <p className="text-lg font-semibold">{stats.oldestDate}</p>
                    <p className="text-sm text-gray-600">Oldest Date</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Sample Data Display */}
          <Card>
            <CardHeader>
              <CardTitle>Sample Historical NAV Data</CardTitle>
              <CardDescription>
                Showing latest 10 records (out of {historicalData.data.length} total)
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full border-collapse">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left p-2 font-medium">Date</th>
                      <th className="text-right p-2 font-medium">NAV (₹)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {historicalData.data.slice(0, 10).map((item, index) => (
                      <tr key={index} className="border-b">
                        <td className="p-2">{item.date}</td>
                        <td className="p-2 text-right font-mono">{item.nav}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              
              {historicalData.data.length > 10 && (
                <p className="text-sm text-gray-600 mt-4">
                  ... and {historicalData.data.length - 10} more records
                </p>
              )}
            </CardContent>
          </Card>

          {/* API Response Structure */}
          <Card>
            <CardHeader>
              <CardTitle>Raw API Response Structure</CardTitle>
              <CardDescription>
                Sample of the actual API response format
              </CardDescription>
            </CardHeader>
            <CardContent>
              <pre className="bg-gray-100 p-4 rounded text-sm overflow-x-auto">
{JSON.stringify({
  meta: historicalData.meta,
  data: historicalData.data.slice(0, 3)
}, null, 2)}
              </pre>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}