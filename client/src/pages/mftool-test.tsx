import React, { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Loader2, Search, Download, Calendar, TrendingUp, AlertTriangle } from 'lucide-react';

export default function MFToolTest() {
  const [schemeCode, setSchemeCode] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [testResults, setTestResults] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');

  const testMFToolAPI = async () => {
    setLoading(true);
    setError('');
    setTestResults(null);
    
    try {
      // Test the MFTool API endpoint we'll create
      const response = await fetch('/api/mftool/test', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          schemeCode,
          startDate,
          endDate
        }),
      });
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      setTestResults(data);
      
      if (!data.success) {
        setError(data.message || 'Failed to fetch data');
      }
    } catch (err: any) {
      setError(`Test failed: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  const testWithSampleData = () => {
    // Test with a known scheme code
    setSchemeCode('119551'); // HDFC Top 100 Fund
    setStartDate('01-01-2024');
    setEndDate('31-12-2024');
  };

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">MFTool Library Test</h1>
          <p className="text-gray-600 mt-2">
            Test historical NAV data extraction using MFTool Python library
          </p>
        </div>
      </div>

      {/* Information Card */}
      <Card className="border-blue-200 bg-blue-50">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-blue-800">
            <AlertTriangle className="h-5 w-5" />
            MFTool Library Requirements
          </CardTitle>
        </CardHeader>
        <CardContent className="text-blue-700">
          <p className="mb-2">
            This test requires the MFTool Python library to be installed and configured on the server.
          </p>
          <ul className="list-disc list-inside space-y-1 text-sm">
            <li>Python package: <code className="bg-blue-100 px-1 rounded">pip install mftool</code></li>
            <li>Server-side API endpoint to handle MFTool requests</li>
            <li>Date format: DD-MM-YYYY</li>
          </ul>
        </CardContent>
      </Card>

      {/* Test Configuration */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Search className="h-5 w-5" />
            Test Configuration
          </CardTitle>
          <CardDescription>
            Configure test parameters for MFTool API
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <Label htmlFor="schemeCode">Scheme Code</Label>
              <Input
                id="schemeCode"
                placeholder="e.g., 119551"
                value={schemeCode}
                onChange={(e) => setSchemeCode(e.target.value)}
              />
            </div>
            <div>
              <Label htmlFor="startDate">Start Date (DD-MM-YYYY)</Label>
              <Input
                id="startDate"
                placeholder="01-01-2024"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
            </div>
            <div>
              <Label htmlFor="endDate">End Date (DD-MM-YYYY)</Label>
              <Input
                id="endDate"
                placeholder="31-12-2024"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
              />
            </div>
          </div>

          <div className="flex gap-4">
            <Button 
              onClick={testMFToolAPI} 
              disabled={loading || !schemeCode}
            >
              {loading ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : (
                <Download className="h-4 w-4 mr-2" />
              )}
              Run MFTool Test
            </Button>
            
            <Button 
              variant="outline" 
              onClick={testWithSampleData}
              disabled={loading}
            >
              Use Sample Data
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Error Display */}
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {/* Test Results */}
      {testResults && (
        <div className="space-y-6">
          {/* Success/Failure Status */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                Test Results
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-2 mb-4">
                <Badge variant={testResults.success ? "default" : "destructive"}>
                  {testResults.success ? "Success" : "Failed"}
                </Badge>
                <span className="text-sm text-gray-600">
                  {testResults.message}
                </span>
              </div>

              {testResults.success && testResults.data && (
                <div className="space-y-4">
                  {/* Scheme Information */}
                  {testResults.data.meta && (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div>
                        <Label className="text-sm font-medium text-gray-600">Scheme Name</Label>
                        <p className="font-semibold">{testResults.data.meta.scheme_name}</p>
                      </div>
                      <div>
                        <Label className="text-sm font-medium text-gray-600">Fund House</Label>
                        <p>{testResults.data.meta.fund_house}</p>
                      </div>
                      <div>
                        <Label className="text-sm font-medium text-gray-600">Scheme Type</Label>
                        <Badge variant="secondary">{testResults.data.meta.scheme_type}</Badge>
                      </div>
                      <div>
                        <Label className="text-sm font-medium text-gray-600">Scheme Code</Label>
                        <p className="font-mono">{testResults.data.meta.scheme_code}</p>
                      </div>
                    </div>
                  )}

                  {/* Data Statistics */}
                  {testResults.statistics && (
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
                            <p className="text-2xl font-bold text-blue-600">
                              {testResults.statistics.totalRecords?.toLocaleString()}
                            </p>
                            <p className="text-sm text-gray-600">Total Records</p>
                          </div>
                          <div className="text-center">
                            <p className="text-2xl font-bold text-green-600">
                              ₹{testResults.statistics.latestNAV}
                            </p>
                            <p className="text-sm text-gray-600">Latest NAV</p>
                          </div>
                          <div className="text-center">
                            <p className="text-lg font-semibold">
                              {testResults.statistics.dateRange?.start}
                            </p>
                            <p className="text-sm text-gray-600">Start Date</p>
                          </div>
                          <div className="text-center">
                            <p className="text-lg font-semibold">
                              {testResults.statistics.dateRange?.end}
                            </p>
                            <p className="text-sm text-gray-600">End Date</p>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  {/* Sample Data */}
                  {testResults.data?.data && (
                    <Card>
                      <CardHeader>
                        <CardTitle>Sample NAV Data</CardTitle>
                        <CardDescription>
                          Showing sample records from MFTool library
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
                              {testResults.data.data.slice(0, 10).map((item: any, index: number) => (
                                <tr key={index} className="border-b">
                                  <td className="p-2">{item.date}</td>
                                  <td className="p-2 text-right font-mono">{item.nav}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </CardContent>
                    </Card>
                  )}

                  {/* Raw Response */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Raw MFTool Response</CardTitle>
                      <CardDescription>
                        Sample of the actual library response
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <pre className="bg-gray-100 p-4 rounded text-sm overflow-x-auto max-h-64">
                        {JSON.stringify(testResults, null, 2)}
                      </pre>
                    </CardContent>
                  </Card>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      )}

      {/* API Endpoint Information */}
      <Card className="border-gray-200">
        <CardHeader>
          <CardTitle>Required API Endpoint</CardTitle>
          <CardDescription>
            This test requires a server-side endpoint to be implemented
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="bg-gray-100 p-4 rounded">
            <p className="font-medium mb-2">POST /api/mftool/test</p>
            <p className="text-sm text-gray-600 mb-4">
              This endpoint should use the MFTool Python library to fetch historical NAV data.
            </p>
            <pre className="text-xs">
{`Expected Request Body:
{
  "schemeCode": "119551",
  "startDate": "01-01-2024",
  "endDate": "31-12-2024"
}

Expected Response:
{
  "success": true,
  "data": {
    "meta": { ... },
    "data": [ ... ]
  },
  "statistics": { ... }
}`}
            </pre>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}