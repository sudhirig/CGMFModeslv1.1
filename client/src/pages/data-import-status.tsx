import React, { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Progress } from "@/components/ui/progress";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import { format, formatDistanceToNow } from "date-fns";
import axios from "axios";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useQuery } from "@tanstack/react-query";
import { AlertCircle, CheckCircle, Clock, RefreshCw } from "lucide-react";

interface ETLRun {
  id: number;
  pipelineName: string;
  status: 'RUNNING' | 'COMPLETED' | 'FAILED';
  startTime: string;
  endTime: string | null;
  recordsProcessed: number | null;
  errorMessage: string | null;
  createdAt: string;
}

interface NavStats {
  funds_with_history: string;
  total_nav_records: string;
  earliest_date: string;
  latest_date: string;
}

interface HistoricalImportStatus {
  etlRun: ETLRun;
  navStats: NavStats;
  topFundsWithHistory: {
    id: number;
    fund_name: string;
    nav_count: string;
    earliest_date: string;
    latest_date: string;
  }[];
}

export default function DataImportStatus() {
  // State for managing the various status queries
  const [isStartingDaily, setIsStartingDaily] = useState(false);
  const [isStartingHistorical, setIsStartingHistorical] = useState(false);
  const [activeTab, setActiveTab] = useState("overview");

  // Query for getting the ETL status overview
  const { 
    data: etlRuns, 
    isLoading: etlLoading, 
    refetch: refetchEtl 
  } = useQuery<ETLRun[]>({
    queryKey: ['/api/etl/status'],
    refetchInterval: 5000, // Refresh every 5 seconds
  });

  // Query for getting the scheduled status
  const { 
    data: scheduledStatus, 
    isLoading: scheduledLoading, 
    refetch: refetchScheduled 
  } = useQuery<any>({
    queryKey: ['/api/amfi/scheduled-status'],
    refetchInterval: 5000,
  });

  // Query for getting the daily NAV update status
  const { 
    data: dailyNavStatus, 
    isLoading: dailyNavLoading, 
    refetch: refetchDailyNav 
  } = useQuery<any>({
    queryKey: ['/api/daily-nav/status'],
    refetchInterval: 5000,
  });

  // Query for getting the historical import status 
  const { 
    data: historicalImportStatus, 
    isLoading: historicalLoading, 
    refetch: refetchHistorical 
  } = useQuery<HistoricalImportStatus>({
    queryKey: ['/api/authentic-nav/status'],
    refetchInterval: 5000,
  });

  // Get the most recent run for each pipeline type
  const getDailyNavRun = () => {
    if (!etlRuns) return null;
    return etlRuns.find(run => run.pipelineName === "real_daily_nav_update");
  };

  const getHistoricalNavRun = () => {
    if (!etlRuns) return null;
    return etlRuns.find(run => run.pipelineName === "authentic_historical_import");
  };

  // Format timestamps for readability
  const formatTime = (dateString: string | null | undefined) => {
    if (!dateString) return "N/A";
    return format(new Date(dateString), "MMM d, yyyy h:mm a");
  };
  
  const formatTimeAgo = (dateString: string | null | undefined) => {
    if (!dateString) return "";
    return `(${formatDistanceToNow(new Date(dateString), { addSuffix: true })})`;
  };

  // Get total number of funds to process
  const [totalFundsToProcess, setTotalFundsToProcess] = useState(16766); // We have 16,766 funds total
  
  // Fetch the actual count on component mount
  useEffect(() => {
    axios.get('/api/funds/count')
      .then(response => {
        if (response.data && response.data.count) {
          setTotalFundsToProcess(response.data.count);
        }
      })
      .catch(error => {
        console.error("Error fetching fund count:", error);
      });
  }, []);
  
  // Calculate progress percentages
  const calculateProgress = (run: ETLRun | null | undefined) => {
    if (!run) return 0;
    if (run.status === "COMPLETED") return 100;
    if (run.status === "FAILED") return 0;
    
    // If we have the authentic historical import with a target of all funds
    if (run.pipelineName === "authentic_historical_import" && run.recordsProcessed) {
      return Math.min(99, (run.recordsProcessed / totalFundsToProcess) * 100);
    }
    
    // Generic progress calculation
    if (run.recordsProcessed) {
      return Math.min(95, (run.recordsProcessed / (run.recordsProcessed + 50)) * 100);
    }
    
    // Default progress when we don't have enough info
    return 10;
  };
  
  // Status indicator component
  const StatusIndicator = ({ status }: { status: string }) => {
    if (status === "RUNNING") {
      return (
        <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200 flex items-center gap-1">
          <RefreshCw className="h-3 w-3 animate-spin" />
          Running
        </Badge>
      );
    } else if (status === "COMPLETED") {
      return (
        <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200 flex items-center gap-1">
          <CheckCircle className="h-3 w-3" />
          Completed
        </Badge>
      );
    } else if (status === "FAILED") {
      return (
        <Badge variant="outline" className="bg-red-50 text-red-700 border-red-200 flex items-center gap-1">
          <AlertCircle className="h-3 w-3" />
          Failed
        </Badge>
      );
    } else {
      return (
        <Badge variant="outline" className="bg-gray-50 text-gray-700 border-gray-200 flex items-center gap-1">
          <Clock className="h-3 w-3" />
          Pending
        </Badge>
      );
    }
  };

  // Action handlers
  const handleStartDailyNavUpdate = async () => {
    try {
      setIsStartingDaily(true);
      await axios.post('/api/daily-nav/start');
      toast({
        title: "Daily NAV Update Started",
        description: "The daily NAV update process has been initiated.",
      });
      // Refresh status after starting
      refetchDailyNav();
      refetchEtl();
    } catch (error) {
      console.error("Error starting daily NAV update:", error);
      toast({
        title: "Error Starting Daily NAV Update",
        description: "Failed to start the daily NAV update process. Please try again.",
        variant: "destructive",
      });
    } finally {
      setIsStartingDaily(false);
    }
  };

  const handleStartHistoricalImport = async () => {
    try {
      setIsStartingHistorical(true);
      await axios.post('/api/authentic-nav/start');
      toast({
        title: "Historical NAV Import Started",
        description: "The historical NAV import process has been initiated.",
      });
      // Refresh status after starting
      refetchHistorical();
      refetchEtl();
    } catch (error) {
      console.error("Error starting historical NAV import:", error);
      toast({
        title: "Error Starting Historical Import",
        description: "Failed to start the historical NAV import process. Please try again.",
        variant: "destructive",
      });
    } finally {
      setIsStartingHistorical(false);
    }
  };

  const refreshAllStatus = () => {
    refetchEtl();
    refetchScheduled();
    refetchDailyNav();
    refetchHistorical();
  };

  // Is the historical import currently running?
  const isHistoricalRunning = historicalImportStatus?.etlRun?.status === "RUNNING";
  
  // Is the daily update currently running?
  const isDailyRunning = getDailyNavRun()?.status === "RUNNING";

  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-2xl font-bold text-neutral-900">Data Import Status</h1>
              <p className="mt-1 text-sm text-neutral-500">
                Monitor and manage authentic data import processes
              </p>
            </div>
            <div className="flex items-center space-x-3">
              <Button 
                variant="outline" 
                onClick={refreshAllStatus} 
                disabled={etlLoading}
                className="flex items-center gap-2"
              >
                <RefreshCw className="h-4 w-4" />
                Refresh
              </Button>
            </div>
          </div>
        </div>

        <Tabs defaultValue="overview" value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="mb-6">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="daily">Daily NAV Updates</TabsTrigger>
            <TabsTrigger value="historical">Historical NAV Import</TabsTrigger>
            <TabsTrigger value="data-quality">Data Quality</TabsTrigger>
          </TabsList>
          
          {/* Overview Tab */}
          <TabsContent value="overview">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Daily NAV Update Status Card */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex justify-between">
                    <span>Daily NAV Update</span>
                    {getDailyNavRun() && <StatusIndicator status={getDailyNavRun()?.status || ""} />}
                  </CardTitle>
                  <CardDescription>
                    Updates the latest NAV values for all funds from AMFI
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {etlLoading ? (
                    <div className="space-y-2">
                      <Skeleton className="h-4 w-full" />
                      <Skeleton className="h-4 w-3/4" />
                    </div>
                  ) : getDailyNavRun() ? (
                    <div className="space-y-4">
                      <div className="flex flex-col space-y-1">
                        <div className="text-sm font-medium">Last Run:</div>
                        <div className="text-sm text-neutral-500">
                          {formatTime(getDailyNavRun()?.startTime)} {formatTimeAgo(getDailyNavRun()?.startTime)}
                        </div>
                      </div>
                      {getDailyNavRun()?.recordsProcessed !== null && (
                        <div className="space-y-2">
                          <div className="flex justify-between text-sm">
                            <span>Progress</span>
                            <span>{getDailyNavRun()?.recordsProcessed || 0} NAV entries processed</span>
                          </div>
                          <Progress value={calculateProgress(getDailyNavRun())} className="h-2" />
                        </div>
                      )}
                      {getDailyNavRun()?.errorMessage && (
                        <div className="text-sm text-neutral-500">
                          <span className="font-medium">Status: </span>
                          {getDailyNavRun()?.errorMessage}
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="text-sm text-neutral-500">No recent daily NAV updates found.</div>
                  )}
                </CardContent>
                <CardFooter>
                  <Button 
                    onClick={handleStartDailyNavUpdate} 
                    disabled={isStartingDaily || isDailyRunning}
                    variant="secondary" 
                    className="w-full"
                  >
                    {isStartingDaily ? (
                      <>
                        <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                        Starting...
                      </>
                    ) : isDailyRunning ? (
                      "Update in Progress..."
                    ) : (
                      "Start Daily NAV Update"
                    )}
                  </Button>
                </CardFooter>
              </Card>

              {/* Historical NAV Import Status Card */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex justify-between">
                    <span>Historical NAV Import</span>
                    {historicalImportStatus?.etlRun && (
                      <StatusIndicator status={historicalImportStatus.etlRun.status} />
                    )}
                  </CardTitle>
                  <CardDescription>
                    Imports 36 months of historical NAV data from AMFI
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {historicalLoading ? (
                    <div className="space-y-2">
                      <Skeleton className="h-4 w-full" />
                      <Skeleton className="h-4 w-3/4" />
                    </div>
                  ) : historicalImportStatus?.etlRun ? (
                    <div className="space-y-4">
                      <div className="flex flex-col space-y-1">
                        <div className="text-sm font-medium">Last Run:</div>
                        <div className="text-sm text-neutral-500">
                          {formatTime(historicalImportStatus.etlRun.startTime)} {formatTimeAgo(historicalImportStatus.etlRun.startTime)}
                        </div>
                      </div>
                      {historicalImportStatus.etlRun.recordsProcessed !== null && (
                        <div className="space-y-2">
                          <div className="flex justify-between text-sm">
                            <span>Progress</span>
                            <span>{historicalImportStatus.etlRun.recordsProcessed || 0} / {totalFundsToProcess.toLocaleString()} funds processed</span>
                          </div>
                          <Progress value={calculateProgress(historicalImportStatus.etlRun)} className="h-2" />
                        </div>
                      )}
                      {historicalImportStatus.etlRun.errorMessage && (
                        <div className="text-sm text-neutral-500">
                          <span className="font-medium">Status: </span>
                          {historicalImportStatus.etlRun.errorMessage}
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="text-sm text-neutral-500">No historical NAV imports found.</div>
                  )}
                </CardContent>
                <CardFooter>
                  <Button 
                    onClick={handleStartHistoricalImport} 
                    disabled={isStartingHistorical || isHistoricalRunning}
                    variant="secondary" 
                    className="w-full"
                  >
                    {isStartingHistorical ? (
                      <>
                        <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                        Starting...
                      </>
                    ) : isHistoricalRunning ? (
                      "Import in Progress..."
                    ) : (
                      "Start Historical NAV Import"
                    )}
                  </Button>
                </CardFooter>
              </Card>

              {/* NAV Data Summary Card */}
              <Card>
                <CardHeader>
                  <CardTitle>NAV Data Summary</CardTitle>
                  <CardDescription>Current state of the NAV database</CardDescription>
                </CardHeader>
                <CardContent>
                  {historicalLoading ? (
                    <div className="space-y-2">
                      <Skeleton className="h-4 w-full" />
                      <Skeleton className="h-4 w-3/4" />
                    </div>
                  ) : historicalImportStatus?.navStats ? (
                    <div className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-sm font-medium">Total Funds</div>
                          <div className="text-2xl font-bold">{historicalImportStatus.navStats.funds_with_history}</div>
                        </div>
                        <div>
                          <div className="text-sm font-medium">Total NAV Records</div>
                          <div className="text-2xl font-bold">{historicalImportStatus.navStats.total_nav_records}</div>
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-sm font-medium">Earliest Data</div>
                          <div className="text-sm">
                            {historicalImportStatus.navStats.earliest_date ? 
                              format(new Date(historicalImportStatus.navStats.earliest_date), "MMM d, yyyy") : 
                              "N/A"}
                          </div>
                        </div>
                        <div>
                          <div className="text-sm font-medium">Latest Data</div>
                          <div className="text-sm">
                            {historicalImportStatus.navStats.latest_date ? 
                              format(new Date(historicalImportStatus.navStats.latest_date), "MMM d, yyyy") : 
                              "N/A"}
                          </div>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="text-sm text-neutral-500">No NAV data summary available.</div>
                  )}
                </CardContent>
              </Card>

              {/* Recent ETL Runs Card */}
              <Card>
                <CardHeader>
                  <CardTitle>Recent ETL Runs</CardTitle>
                  <CardDescription>Latest data processing operations</CardDescription>
                </CardHeader>
                <CardContent>
                  {etlLoading ? (
                    <div className="space-y-2">
                      <Skeleton className="h-4 w-full" />
                      <Skeleton className="h-4 w-3/4" />
                    </div>
                  ) : etlRuns && etlRuns.length > 0 ? (
                    <div className="space-y-2 max-h-[200px] overflow-y-auto">
                      {etlRuns.slice(0, 5).map((run) => (
                        <div key={run.id} className="flex justify-between items-center py-2 border-b last:border-b-0">
                          <div>
                            <div className="font-medium text-sm">{run.pipelineName}</div>
                            <div className="text-xs text-neutral-500">{formatTime(run.startTime)}</div>
                          </div>
                          <StatusIndicator status={run.status} />
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-sm text-neutral-500">No recent ETL runs found.</div>
                  )}
                </CardContent>
              </Card>
            </div>
          </TabsContent>
          
          {/* Daily NAV Updates Tab */}
          <TabsContent value="daily">
            <Card>
              <CardHeader>
                <CardTitle>Daily NAV Update Details</CardTitle>
                <CardDescription>Real-time AMFI data collection for latest NAVs</CardDescription>
              </CardHeader>
              <CardContent>
                {dailyNavLoading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-3/4" />
                  </div>
                ) : getDailyNavRun() ? (
                  <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <div>
                        <h3 className="text-sm font-medium mb-2">Process Information</h3>
                        <div className="space-y-2 text-sm">
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Process ID:</span>
                            <span className="font-medium">{getDailyNavRun()?.id}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Status:</span>
                            <StatusIndicator status={getDailyNavRun()?.status || ""} />
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Started:</span>
                            <span>{formatTime(getDailyNavRun()?.startTime)}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Completed:</span>
                            <span>{getDailyNavRun()?.endTime ? formatTime(getDailyNavRun()?.endTime) : "In Progress"}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Duration:</span>
                            <span>
                              {getDailyNavRun()?.startTime && 
                                (getDailyNavRun()?.endTime ? 
                                  formatDistanceToNow(
                                    new Date(getDailyNavRun()?.endTime), 
                                    {includeSeconds: true, addSuffix: false}
                                  ) : 
                                  formatDistanceToNow(
                                    new Date(getDailyNavRun()?.startTime), 
                                    {includeSeconds: true, addSuffix: false}
                                  ) + " (ongoing)"
                                )
                              }
                            </span>
                          </div>
                        </div>
                      </div>
                      
                      <div>
                        <h3 className="text-sm font-medium mb-2">Progress</h3>
                        {getDailyNavRun()?.recordsProcessed !== null && (
                          <div className="space-y-4">
                            <div className="space-y-2">
                              <div className="flex justify-between text-sm">
                                <span className="text-neutral-500">NAV Entries Processed:</span>
                                <span className="font-medium">{getDailyNavRun()?.recordsProcessed || 0}</span>
                              </div>
                              <Progress value={calculateProgress(getDailyNavRun())} className="h-2" />
                            </div>
                          </div>
                        )}
                        
                        {getDailyNavRun()?.errorMessage && (
                          <div className="mt-4">
                            <h3 className="text-sm font-medium mb-2">Status Message</h3>
                            <div className="p-3 bg-neutral-50 rounded border text-sm">
                              {getDailyNavRun()?.errorMessage}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                    
                    <div className="pt-4 border-t">
                      <h3 className="text-sm font-medium mb-3">Actions</h3>
                      <div className="flex space-x-3">
                        <Button 
                          onClick={handleStartDailyNavUpdate} 
                          disabled={isStartingDaily || isDailyRunning}
                        >
                          {isStartingDaily ? (
                            <>
                              <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                              Starting...
                            </>
                          ) : isDailyRunning ? (
                            "Update in Progress..."
                          ) : (
                            "Start Daily NAV Update"
                          )}
                        </Button>
                        <Button 
                          variant="outline" 
                          onClick={() => {
                            refetchDailyNav();
                            refetchEtl();
                          }}
                        >
                          Refresh Status
                        </Button>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="text-center py-6">
                    <p className="text-neutral-500 mb-4">No daily NAV update processes found.</p>
                    <Button 
                      onClick={handleStartDailyNavUpdate} 
                      disabled={isStartingDaily}
                    >
                      {isStartingDaily ? (
                        <>
                          <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                          Starting...
                        </>
                      ) : (
                        "Start Daily NAV Update"
                      )}
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
          
          {/* Historical NAV Import Tab */}
          <TabsContent value="historical">
            <Card>
              <CardHeader>
                <CardTitle>Historical NAV Import Details</CardTitle>
                <CardDescription>
                  Imports up to 36 months of historical NAV data from AMFI for mutual fund analysis
                </CardDescription>
              </CardHeader>
              <CardContent>
                {historicalLoading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-3/4" />
                  </div>
                ) : historicalImportStatus?.etlRun ? (
                  <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <div>
                        <h3 className="text-sm font-medium mb-2">Process Information</h3>
                        <div className="space-y-2 text-sm">
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Process ID:</span>
                            <span className="font-medium">{historicalImportStatus.etlRun.id}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Status:</span>
                            <StatusIndicator status={historicalImportStatus.etlRun.status} />
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Started:</span>
                            <span>{formatTime(historicalImportStatus.etlRun.startTime)}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-neutral-500">Duration:</span>
                            <span>
                              {historicalImportStatus.etlRun.startTime && 
                                (historicalImportStatus.etlRun.endTime ? 
                                  formatDistanceToNow(
                                    new Date(historicalImportStatus.etlRun.endTime), 
                                    {includeSeconds: true, addSuffix: false}
                                  ) : 
                                  formatDistanceToNow(
                                    new Date(historicalImportStatus.etlRun.startTime), 
                                    {includeSeconds: true, addSuffix: false}
                                  ) + " (ongoing)"
                                )
                              }
                            </span>
                          </div>
                        </div>
                      </div>
                      
                      <div>
                        <h3 className="text-sm font-medium mb-2">Progress</h3>
                        {historicalImportStatus.etlRun.recordsProcessed !== null && (
                          <div className="space-y-4">
                            <div className="space-y-2">
                              <div className="flex justify-between text-sm">
                                <span className="text-neutral-500">Funds Processed:</span>
                                <span className="font-medium">
                                  {historicalImportStatus.etlRun.recordsProcessed} / 1000 
                                  {historicalImportStatus.etlRun.status === "RUNNING" && 
                                    ` (${(historicalImportStatus.etlRun.recordsProcessed / 10).toFixed(1)}%)`}
                                </span>
                              </div>
                              <Progress 
                                value={calculateProgress(historicalImportStatus.etlRun)} 
                                className="h-2" 
                              />
                            </div>
                          </div>
                        )}
                        
                        {historicalImportStatus.etlRun.errorMessage && (
                          <div className="mt-4">
                            <h3 className="text-sm font-medium mb-2">Status Message</h3>
                            <div className="p-3 bg-neutral-50 rounded border text-sm">
                              {historicalImportStatus.etlRun.errorMessage}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                    
                    {historicalImportStatus.topFundsWithHistory && 
                     historicalImportStatus.topFundsWithHistory.length > 0 && (
                      <div>
                        <h3 className="text-sm font-medium mb-2">Sample Funds with NAV Data</h3>
                        <div className="overflow-x-auto">
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Fund Name</TableHead>
                                <TableHead className="text-right">NAV Count</TableHead>
                                <TableHead>Earliest Date</TableHead>
                                <TableHead>Latest Date</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {historicalImportStatus.topFundsWithHistory.map((fund) => (
                                <TableRow key={fund.id}>
                                  <TableCell className="font-medium max-w-[300px] truncate">{fund.fund_name}</TableCell>
                                  <TableCell className="text-right">{fund.nav_count}</TableCell>
                                  <TableCell>
                                    {fund.earliest_date ? format(new Date(fund.earliest_date), "MMM d, yyyy") : "N/A"}
                                  </TableCell>
                                  <TableCell>
                                    {fund.latest_date ? format(new Date(fund.latest_date), "MMM d, yyyy") : "N/A"}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      </div>
                    )}
                    
                    <div className="pt-4 border-t">
                      <h3 className="text-sm font-medium mb-3">Actions</h3>
                      <div className="flex space-x-3">
                        <Button 
                          onClick={handleStartHistoricalImport}
                          disabled={isStartingHistorical || isHistoricalRunning}
                        >
                          {isStartingHistorical ? (
                            <>
                              <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                              Starting...
                            </>
                          ) : isHistoricalRunning ? (
                            "Import in Progress..."
                          ) : (
                            "Start Historical NAV Import"
                          )}
                        </Button>
                        <Button 
                          variant="outline" 
                          onClick={() => {
                            refetchHistorical();
                            refetchEtl();
                          }}
                        >
                          Refresh Status
                        </Button>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="text-center py-6">
                    <p className="text-neutral-500 mb-4">No historical NAV import processes found.</p>
                    <Button 
                      onClick={handleStartHistoricalImport}
                      disabled={isStartingHistorical}
                    >
                      {isStartingHistorical ? (
                        <>
                          <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                          Starting...
                        </>
                      ) : (
                        "Start Historical NAV Import"
                      )}
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
          
          {/* Data Quality Tab */}
          <TabsContent value="data-quality">
            <Card>
              <CardHeader>
                <CardTitle>NAV Data Quality Assessment</CardTitle>
                <CardDescription>
                  Analysis of data completeness and reliability for investment decisions
                </CardDescription>
              </CardHeader>
              <CardContent>
                {historicalLoading ? (
                  <div className="space-y-2">
                    <Skeleton className="h-4 w-full" />
                    <Skeleton className="h-4 w-3/4" />
                  </div>
                ) : (
                  <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <div>
                        <h3 className="text-sm font-medium mb-3">Data Coverage Summary</h3>
                        <div className="space-y-4">
                          <div className="flex justify-between items-center p-3 bg-neutral-50 rounded border">
                            <div>
                              <div className="text-sm font-medium">Total Funds</div>
                              <div className="text-sm text-neutral-500">Funds with at least one NAV record</div>
                            </div>
                            <div className="text-xl font-bold">
                              {historicalImportStatus?.navStats?.funds_with_history || 0}
                            </div>
                          </div>
                          
                          <div className="flex justify-between items-center p-3 bg-neutral-50 rounded border">
                            <div>
                              <div className="text-sm font-medium">Total NAV Records</div>
                              <div className="text-sm text-neutral-500">All NAV data points in the database</div>
                            </div>
                            <div className="text-xl font-bold">
                              {historicalImportStatus?.navStats?.total_nav_records || 0}
                            </div>
                          </div>
                          
                          <div className="flex justify-between items-center p-3 bg-neutral-50 rounded border">
                            <div>
                              <div className="text-sm font-medium">Average NAV Points Per Fund</div>
                              <div className="text-sm text-neutral-500">Indicates data density</div>
                            </div>
                            <div className="text-xl font-bold">
                              {historicalImportStatus?.navStats?.funds_with_history && 
                               historicalImportStatus?.navStats?.total_nav_records ? 
                                (parseInt(historicalImportStatus.navStats.total_nav_records) / 
                                 parseInt(historicalImportStatus.navStats.funds_with_history)).toFixed(1) : 
                                "0"}
                            </div>
                          </div>
                        </div>
                      </div>
                      
                      <div>
                        <h3 className="text-sm font-medium mb-3">Data Timespan</h3>
                        <div className="space-y-4">
                          <div className="flex justify-between items-center p-3 bg-neutral-50 rounded border">
                            <div>
                              <div className="text-sm font-medium">Earliest Data Point</div>
                              <div className="text-sm text-neutral-500">Oldest NAV record in database</div>
                            </div>
                            <div className="text-md font-bold">
                              {historicalImportStatus?.navStats?.earliest_date ? 
                                format(new Date(historicalImportStatus.navStats.earliest_date), "MMM d, yyyy") : 
                                "N/A"}
                            </div>
                          </div>
                          
                          <div className="flex justify-between items-center p-3 bg-neutral-50 rounded border">
                            <div>
                              <div className="text-sm font-medium">Latest Data Point</div>
                              <div className="text-sm text-neutral-500">Most recent NAV record</div>
                            </div>
                            <div className="text-md font-bold">
                              {historicalImportStatus?.navStats?.latest_date ? 
                                format(new Date(historicalImportStatus.navStats.latest_date), "MMM d, yyyy") : 
                                "N/A"}
                            </div>
                          </div>
                          
                          <div className="flex justify-between items-center p-3 bg-neutral-50 rounded border">
                            <div>
                              <div className="text-sm font-medium">Historical Range</div>
                              <div className="text-sm text-neutral-500">Time span of available data</div>
                            </div>
                            <div className="text-md font-bold">
                              {historicalImportStatus?.navStats?.earliest_date && 
                               historicalImportStatus?.navStats?.latest_date ? 
                                formatDistanceToNow(
                                  new Date(historicalImportStatus.navStats.earliest_date), 
                                  {includeSeconds: false, addSuffix: false}
                                ) : 
                                "N/A"}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                    
                    <div className="pt-4 border-t">
                      <h3 className="text-sm font-medium mb-3">Data Quality Recommendations</h3>
                      <div className="space-y-3">
                        <div className="p-3 bg-blue-50 text-blue-700 rounded border border-blue-200">
                          <div className="font-medium mb-1">Historical Data Import</div>
                          <p className="text-sm">
                            For reliable quartile analysis and scoring, funds should have at least 36 months of
                            historical NAV data. Continue the historical import process until completion.
                          </p>
                        </div>
                        
                        <div className="p-3 bg-amber-50 text-amber-700 rounded border border-amber-200">
                          <div className="font-medium mb-1">Daily Updates</div>
                          <p className="text-sm">
                            Schedule daily NAV updates to keep the database current with the latest 
                            fund performance data from AMFI.
                          </p>
                        </div>
                        
                        <div className="p-3 bg-green-50 text-green-700 rounded border border-green-200">
                          <div className="font-medium mb-1">Quartile Rescoring</div>
                          <p className="text-sm">
                            After historical data import completes, trigger a quartile rescoring process
                            to recalculate fund rankings based on authentic historical performance.
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}