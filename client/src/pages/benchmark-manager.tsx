import { useState, useRef } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Download, Upload, AlertCircle, CheckCircle2, ExternalLink, FileSpreadsheet, RefreshCw } from "lucide-react";
import { apiRequest } from "@/lib/queryClient";
import { toast } from "@/hooks/use-toast";

export default function BenchmarkManager() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [benchmarkName, setBenchmarkName] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  const queryClient = useQueryClient();

  // Fetch current benchmarks
  const { data: benchmarkList, isLoading: isLoadingBenchmarks } = useQuery({
    queryKey: ['/api/benchmarks/list'],
  });

  // Fetch missing benchmarks
  const { data: missingBenchmarks, isLoading: isLoadingMissing } = useQuery({
    queryKey: ['/api/benchmarks/missing'],
  });

  // Fetch data sources
  const { data: dataSources } = useQuery({
    queryKey: ['/api/benchmarks/data-sources'],
  });
  
  // Fetch benchmark collection status
  const { data: collectionStatus } = useQuery({
    queryKey: ['/api/benchmark-collection/status'],
  });

  // Collect official data mutation
  const collectOfficialMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/benchmark-collection/collect-official');
    },
    onSuccess: (data) => {
      toast({
        title: "Success",
        description: data.message || `Collected ${data.results?.total || 0} benchmark data points`,
      });
      queryClient.invalidateQueries({ queryKey: ['/api/benchmarks'] });
      queryClient.invalidateQueries({ queryKey: ['/api/benchmark-collection'] });
    },
    onError: (error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to collect benchmark data",
        variant: "destructive",
      });
    },
  });
  
  // Import TRI data mutation
  const importTRIMutation = useMutation({
    mutationFn: async () => {
      return await apiRequest('POST', '/api/benchmark-collection/import-tri');
    },
    onSuccess: (data) => {
      toast({
        title: "Success",
        description: data.message || `Imported ${data.imported || 0} TRI data points`,
      });
      queryClient.invalidateQueries({ queryKey: ['/api/benchmarks'] });
      queryClient.invalidateQueries({ queryKey: ['/api/benchmark-collection'] });
    },
    onError: (error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to import TRI data",
        variant: "destructive",
      });
    },
  });

  // CSV upload mutation
  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!selectedFile || !benchmarkName) {
        throw new Error("Please select a file and enter benchmark name");
      }
      
      const formData = new FormData();
      formData.append('file', selectedFile);
      formData.append('benchmarkName', benchmarkName);
      
      const response = await fetch('/api/benchmarks/import/csv', {
        method: 'POST',
        body: formData,
      });
      
      if (!response.ok) {
        throw new Error('Upload failed');
      }
      
      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: "Success",
        description: `Imported ${data.recordsImported} records for ${benchmarkName}`,
      });
      setSelectedFile(null);
      setBenchmarkName("");
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      queryClient.invalidateQueries({ queryKey: ['/api/benchmarks'] });
    },
    onError: (error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to import data",
        variant: "destructive",
      });
    },
  });

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      setSelectedFile(file);
    }
  };

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="space-y-2">
        <h1 className="text-3xl font-bold">Benchmark Data Manager</h1>
        <p className="text-muted-foreground">
          Import and manage benchmark data from legitimate sources
        </p>
      </div>

      <Alert>
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>
          Only import data from official sources. AdvisorKhoj data requires their permission.
          Use NSE, BSE, or other official market data providers instead.
        </AlertDescription>
      </Alert>

      <Tabs defaultValue="collect" className="space-y-4">
        <TabsList className="grid w-full grid-cols-5">
          <TabsTrigger value="collect">Collect Data</TabsTrigger>
          <TabsTrigger value="sources">Data Sources</TabsTrigger>
          <TabsTrigger value="current">Current Benchmarks</TabsTrigger>
          <TabsTrigger value="missing">Missing Benchmarks</TabsTrigger>
          <TabsTrigger value="import">Manual Import</TabsTrigger>
        </TabsList>

        <TabsContent value="collect" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Automatic Benchmark Collection</CardTitle>
              <CardDescription>
                Collect benchmark data from official public sources
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-4 md:grid-cols-2">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">NSE Indices</CardTitle>
                    <CardDescription>
                      Collect NIFTY indices from National Stock Exchange
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Button
                      onClick={() => collectOfficialMutation.mutate()}
                      disabled={collectOfficialMutation.isPending}
                      className="w-full"
                    >
                      {collectOfficialMutation.isPending ? (
                        <>
                          <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                          Collecting...
                        </>
                      ) : (
                        <>
                          <Download className="mr-2 h-4 w-4" />
                          Collect NSE Data
                        </>
                      )}
                    </Button>
                    <p className="text-sm text-muted-foreground mt-2">
                      Includes: NIFTY 50, 100, 200, 500, MIDCAP, SMALLCAP
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">TRI Indices</CardTitle>
                    <CardDescription>
                      Generate Total Return Index versions
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Button
                      onClick={() => importTRIMutation.mutate()}
                      disabled={importTRIMutation.isPending}
                      className="w-full"
                      variant="secondary"
                    >
                      {importTRIMutation.isPending ? (
                        <>
                          <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                          Importing...
                        </>
                      ) : (
                        <>
                          <Upload className="mr-2 h-4 w-4" />
                          Import TRI Data
                        </>
                      )}
                    </Button>
                    <p className="text-sm text-muted-foreground mt-2">
                      Creates TRI versions with dividend reinvestment
                    </p>
                  </CardContent>
                </Card>
              </div>

              {collectionStatus && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Collection Status</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      <div className="flex justify-between">
                        <span>Total Benchmarks:</span>
                        <span className="font-medium">{collectionStatus.totalBenchmarks}</span>
                      </div>
                      <div className="flex justify-between">
                        <span>Benchmarks with Data:</span>
                        <span className="font-medium">{collectionStatus.benchmarksWithData}</span>
                      </div>
                      <div className="space-y-1">
                        <div className="flex justify-between">
                          <span>Coverage:</span>
                          <span className="font-medium">{collectionStatus.coveragePercent}%</span>
                        </div>
                        <Progress value={collectionStatus.coveragePercent} />
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sources" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Legitimate Data Sources</CardTitle>
              <CardDescription>
                Official sources for benchmark data
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 md:grid-cols-2">
                {dataSources?.sources && Object.entries(dataSources.sources).map(([key, source]: [string, any]) => (
                  <Card key={key}>
                    <CardHeader>
                      <div className="flex items-center justify-between">
                        <CardTitle className="text-lg">{source.name}</CardTitle>
                        <Badge variant={source.access === 'Free' || source.access === 'Free download' ? 'default' : 'secondary'}>
                          {source.access}
                        </Badge>
                      </div>
                    </CardHeader>
                    <CardContent className="space-y-2">
                      <div className="flex items-center gap-2">
                        <ExternalLink className="h-4 w-4" />
                        <a 
                          href={source.website} 
                          target="_blank" 
                          rel="noopener noreferrer"
                          className="text-sm text-blue-600 hover:underline"
                        >
                          {source.website}
                        </a>
                      </div>
                      {source.dataSection && (
                        <p className="text-sm text-muted-foreground">
                          <strong>Data Section:</strong> {source.dataSection}
                        </p>
                      )}
                      {source.indices && (
                        <div>
                          <p className="text-sm font-medium">Available Indices:</p>
                          <p className="text-sm text-muted-foreground">
                            {source.indices.join(', ')}
                          </p>
                        </div>
                      )}
                      {source.data && (
                        <p className="text-sm text-muted-foreground">
                          <strong>Data:</strong> {source.data}
                        </p>
                      )}
                    </CardContent>
                  </Card>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="current" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Current Benchmarks in System</CardTitle>
              <CardDescription>
                {benchmarkList?.total || 0} benchmarks being used by mutual funds
              </CardDescription>
            </CardHeader>
            <CardContent>
              {isLoadingBenchmarks ? (
                <div className="text-center py-4">Loading benchmarks...</div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Benchmark Name</TableHead>
                      <TableHead className="text-right">Fund Count</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {benchmarkList?.benchmarks?.map((benchmark: any) => (
                      <TableRow key={benchmark.benchmark_name}>
                        <TableCell className="font-medium">{benchmark.benchmark_name}</TableCell>
                        <TableCell className="text-right">{benchmark.fund_count}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="missing" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Missing Benchmark Data</CardTitle>
              <CardDescription>
                Benchmarks used by funds but not available in market indices table
              </CardDescription>
            </CardHeader>
            <CardContent>
              {isLoadingMissing ? (
                <div className="text-center py-4">Loading missing benchmarks...</div>
              ) : (
                <div className="space-y-4">
                  <Alert>
                    <AlertDescription>
                      These benchmarks need data import from official sources
                    </AlertDescription>
                  </Alert>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Benchmark Name</TableHead>
                        <TableHead className="text-right">Funds Using</TableHead>
                        <TableHead>Action</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {missingBenchmarks?.missingBenchmarks?.map((benchmark: any) => (
                        <TableRow key={benchmark.benchmark_name}>
                          <TableCell className="font-medium">{benchmark.benchmark_name}</TableCell>
                          <TableCell className="text-right">{benchmark.fund_count}</TableCell>
                          <TableCell>
                            <Badge variant="outline">Needs Import</Badge>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="import" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Import Benchmark Data</CardTitle>
              <CardDescription>
                Upload CSV files from official sources
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <Alert>
                <AlertDescription>
                  CSV format should include: Date, Close/Value columns
                </AlertDescription>
              </Alert>
              
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="benchmark-name">Benchmark Name</Label>
                  <Input
                    id="benchmark-name"
                    placeholder="e.g., NIFTY 500 TRI"
                    value={benchmarkName}
                    onChange={(e) => setBenchmarkName(e.target.value)}
                  />
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="csv-file">CSV File</Label>
                  <div className="flex items-center gap-2">
                    <Input
                      ref={fileInputRef}
                      id="csv-file"
                      type="file"
                      accept=".csv"
                      onChange={handleFileChange}
                    />
                    <Button
                      onClick={() => uploadMutation.mutate()}
                      disabled={!selectedFile || !benchmarkName || uploadMutation.isPending}
                    >
                      {uploadMutation.isPending ? (
                        <>Uploading...</>
                      ) : (
                        <>
                          <Upload className="mr-2 h-4 w-4" />
                          Import
                        </>
                      )}
                    </Button>
                  </div>
                </div>
                
                {selectedFile && (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <FileSpreadsheet className="h-4 w-4" />
                    {selectedFile.name} ({(selectedFile.size / 1024).toFixed(2)} KB)
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Import Instructions</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              <ol className="list-decimal list-inside space-y-2 text-sm">
                <li>Download historical data from official sources (NSE, BSE)</li>
                <li>Ensure CSV has Date and Close/Value columns</li>
                <li>Enter the exact benchmark name as used in the funds table</li>
                <li>Upload the CSV file</li>
              </ol>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}