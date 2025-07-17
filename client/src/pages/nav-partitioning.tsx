import React, { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { apiRequest } from '@/lib/queryClient';

export default function NavPartitioning() {
  const [testResult, setTestResult] = useState<any>(null);
  const [partitionResult, setPartitionResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState(0);

  const runTest = async () => {
    setLoading(true);
    setError(null);
    setProgress(20);
    
    try {
      const response = await apiRequest('/api/nav/test-partitioning', {
        method: 'POST',
      });
      
      setProgress(100);
      setTestResult(response);
    } catch (err: any) {
      setError(err.message || 'Test failed');
    } finally {
      setLoading(false);
    }
  };

  const runPartitioning = async () => {
    if (!confirm('This will create a partitioned copy of the NAV table. Continue?')) {
      return;
    }
    
    setLoading(true);
    setError(null);
    setProgress(10);
    
    try {
      // Simulate progress
      const progressInterval = setInterval(() => {
        setProgress(prev => Math.min(prev + 5, 90));
      }, 2000);
      
      const response = await apiRequest('/api/nav/partition-table', {
        method: 'POST',
      });
      
      clearInterval(progressInterval);
      setProgress(100);
      setPartitionResult(response);
    } catch (err: any) {
      setError(err.message || 'Partitioning failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container mx-auto p-6">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2">NAV Table Partitioning</h1>
        <p className="text-muted-foreground">
          Optimize the 2.3GB NAV table with 20M+ records using PostgreSQL partitioning
        </p>
      </div>

      <Tabs defaultValue="overview" className="space-y-4">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="test">Test Partitioning</TabsTrigger>
          <TabsTrigger value="migrate">Run Migration</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="grid gap-4">
            <Card>
              <CardHeader>
                <CardTitle>Current NAV Table Status</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div>
                    <p className="text-sm text-muted-foreground">Table Size</p>
                    <p className="text-2xl font-bold">2.3 GB</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Total Records</p>
                    <p className="text-2xl font-bold">20M+</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Date Range</p>
                    <p className="text-2xl font-bold">2006-2025</p>
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground">Unique Funds</p>
                    <p className="text-2xl font-bold">14,300</p>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Partitioning Benefits</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2">
                  <li className="flex items-start">
                    <Badge variant="outline" className="mr-2">Performance</Badge>
                    <span>Faster queries by scanning only relevant partitions</span>
                  </li>
                  <li className="flex items-start">
                    <Badge variant="outline" className="mr-2">Maintenance</Badge>
                    <span>Easier archival and deletion of old data</span>
                  </li>
                  <li className="flex items-start">
                    <Badge variant="outline" className="mr-2">Scalability</Badge>
                    <span>Better handling of future data growth</span>
                  </li>
                  <li className="flex items-start">
                    <Badge variant="outline" className="mr-2">Efficiency</Badge>
                    <span>Reduced memory usage for date-range queries</span>
                  </li>
                </ul>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="test">
          <Card>
            <CardHeader>
              <CardTitle>Test Partitioning</CardTitle>
              <CardDescription>
                Run a small-scale test to verify partitioning works correctly
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <Button 
                onClick={runTest} 
                disabled={loading}
                className="w-full sm:w-auto"
              >
                {loading ? 'Running Test...' : 'Run Partitioning Test'}
              </Button>

              {loading && (
                <Progress value={progress} className="w-full" />
              )}

              {error && (
                <Alert variant="destructive">
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}

              {testResult && (
                <div className="space-y-4">
                  <Alert variant={testResult.success ? "default" : "destructive"}>
                    <AlertDescription>
                      {testResult.success 
                        ? 'Test completed successfully!' 
                        : `Test failed: ${testResult.error}`}
                    </AlertDescription>
                  </Alert>

                  {testResult.success && (
                    <Card>
                      <CardHeader>
                        <CardTitle className="text-lg">Performance Results</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="grid grid-cols-3 gap-4 text-center">
                          <div>
                            <p className="text-sm text-muted-foreground">Original Query</p>
                            <p className="text-xl font-bold">{testResult.originalQueryTime}ms</p>
                          </div>
                          <div>
                            <p className="text-sm text-muted-foreground">Partitioned Query</p>
                            <p className="text-xl font-bold">{testResult.partitionedQueryTime}ms</p>
                          </div>
                          <div>
                            <p className="text-sm text-muted-foreground">Improvement</p>
                            <p className="text-xl font-bold text-green-600">
                              {testResult.improvement}%
                            </p>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="migrate">
          <Card>
            <CardHeader>
              <CardTitle>Run Full Migration</CardTitle>
              <CardDescription>
                Create partitioned table and migrate all 20M+ records
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <Alert>
                <AlertDescription>
                  <strong>Important:</strong> This process will:
                  <ul className="list-disc ml-5 mt-2">
                    <li>Create a new partitioned table structure</li>
                    <li>Copy all NAV data to the new structure</li>
                    <li>Take approximately 30-60 minutes</li>
                    <li>Require manual table swap after verification</li>
                  </ul>
                </AlertDescription>
              </Alert>

              <Button 
                onClick={runPartitioning} 
                disabled={loading}
                variant="destructive"
                className="w-full sm:w-auto"
              >
                {loading ? 'Running Migration...' : 'Start Full Migration'}
              </Button>

              {loading && (
                <div className="space-y-2">
                  <Progress value={progress} className="w-full" />
                  <p className="text-sm text-muted-foreground">
                    This may take 30-60 minutes. Do not close this page.
                  </p>
                </div>
              )}

              {partitionResult && (
                <div className="space-y-4">
                  <Alert variant={partitionResult.success ? "default" : "destructive"}>
                    <AlertDescription>
                      {partitionResult.success 
                        ? 'Migration completed successfully!' 
                        : `Migration failed: ${partitionResult.error}`}
                    </AlertDescription>
                  </Alert>

                  {partitionResult.success && (
                    <>
                      <Card>
                        <CardHeader>
                          <CardTitle className="text-lg">Migration Results</CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="grid grid-cols-2 gap-4">
                            <div>
                              <p className="text-sm text-muted-foreground">Original Rows</p>
                              <p className="text-xl font-bold">
                                {partitionResult.originalRows?.toLocaleString()}
                              </p>
                            </div>
                            <div>
                              <p className="text-sm text-muted-foreground">Migrated Rows</p>
                              <p className="text-xl font-bold">
                                {partitionResult.partitionedRows?.toLocaleString()}
                              </p>
                            </div>
                          </div>
                        </CardContent>
                      </Card>

                      <Alert>
                        <AlertDescription>
                          <strong>Next Steps:</strong>
                          <pre className="mt-2 p-2 bg-muted rounded text-xs">
{`ALTER TABLE nav_data RENAME TO nav_data_old;
ALTER TABLE nav_data_partitioned RENAME TO nav_data;`}
                          </pre>
                          Run these commands manually in the database to complete the migration.
                        </AlertDescription>
                      </Alert>
                    </>
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}