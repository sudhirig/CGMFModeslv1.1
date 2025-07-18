import React, { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useEtlStatus } from "@/hooks/use-etl-status";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { format } from "date-fns";
import { Badge } from "@/components/ui/badge";
import { toast } from "@/hooks/use-toast";
import axios from "axios";
import { Progress } from "@/components/ui/progress";
import { 
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export default function EtlPipeline() {
  const { 
    etlRuns, 
    refreshEtlStatus, 
    triggerDataCollection, 
    triggerFundDetailsCollection,
    scheduleFundDetailsCollection,
    scheduleBulkProcessing,
    stopBulkProcessing,
    isLoading, 
    isCollecting, 
    isCollectingDetails,
    isSchedulingBulk,
    fundDetailsStats,
    error 
  } = useEtlStatus();
  const [scheduledStatus, setScheduledStatus] = useState<{
    daily: { active: boolean; lastRun: any };
    historical: { active: boolean; lastRun: any };
    fundDetails: { active: boolean; lastRun: any };
    bulkProcessing: { active: boolean; lastRun: any };
  }>({
    daily: { active: false, lastRun: null },
    historical: { active: false, lastRun: null },
    fundDetails: { active: false, lastRun: null },
    bulkProcessing: { active: false, lastRun: null }
  });
  
  // Bulk processing configuration
  const [bulkConfig, setBulkConfig] = useState({
    batchSize: 100,
    batchCount: 5,
    intervalHours: 24
  });
  const [isScheduling, setIsScheduling] = useState(false);
  
  // Fetch the scheduled import status
  const fetchScheduledStatus = async () => {
    try {
      const response = await axios.get('/api/amfi/scheduled-status');
      if (response.data.success) {
        // Preserve bulkProcessing state which is managed client-side for now
        setScheduledStatus({
          ...response.data.scheduledImports,
          bulkProcessing: scheduledStatus?.bulkProcessing || { active: false, lastRun: null }
        });
      }
    } catch (error) {
      console.error('Error fetching scheduled status:', error);
    }
  };
  
  // Schedule a new import
  const scheduleImport = async (type: 'daily' | 'historical', interval: 'daily' | 'weekly') => {
    try {
      setIsScheduling(true);
      const response = await axios.get(`/api/amfi/schedule-import?type=${type}&interval=${interval}`);
      
      if (response.data.success) {
        toast({
          title: "Import Scheduled",
          description: `${type === 'daily' ? 'Daily' : 'Historical'} import scheduled with ${interval} frequency`,
          variant: "default",
        });
        
        // Refresh status after scheduling
        await fetchScheduledStatus();
        await refreshEtlStatus();
      }
    } catch (error) {
      console.error(`Error scheduling ${type} import:`, error);
      toast({
        title: "Scheduling Failed",
        description: `Failed to schedule ${type} import. Please try again.`,
        variant: "destructive",
      });
    } finally {
      setIsScheduling(false);
    }
  };
  
  // Stop a scheduled import
  const stopScheduledImport = async (type: 'daily' | 'historical' | 'all') => {
    try {
      setIsScheduling(true);
      const response = await axios.get(`/api/amfi/stop-scheduled-import?type=${type}`);
      
      if (response.data.success) {
        toast({
          title: "Import Stopped",
          description: `Stopped scheduled ${type} import`,
          variant: "default",
        });
        
        // Refresh status after stopping
        await fetchScheduledStatus();
        await refreshEtlStatus();
      }
    } catch (error) {
      console.error(`Error stopping ${type} import:`, error);
      toast({
        title: "Operation Failed",
        description: `Failed to stop ${type} import. Please try again.`,
        variant: "destructive",
      });
    } finally {
      setIsScheduling(false);
    }
  };
  
  // Fetch AMFI status data
  const [amfiStatus, setAmfiStatus] = useState<any>(null);
  const [isLoadingAmfiStatus, setIsLoadingAmfiStatus] = useState(false);
  
  const fetchAmfiStatus = async () => {
    try {
      setIsLoadingAmfiStatus(true);
      const response = await axios.get('/api/amfi/status');
      if (response.data.success) {
        setAmfiStatus(response.data);
      }
    } catch (error) {
      console.error('Error fetching AMFI status:', error);
    } finally {
      setIsLoadingAmfiStatus(false);
    }
  };
  
  // Initialize data on component mount and set up refresh intervals
  useEffect(() => {
    // Initial data fetch
    fetchScheduledStatus();
    refreshEtlStatus();
    fetchAmfiStatus();
    
    // Set up more frequent refresh intervals
    const statusInterval = setInterval(() => {
      fetchScheduledStatus();
      refreshEtlStatus();
    }, 5000); // Check every 5 seconds
    
    const amfiStatusInterval = setInterval(() => {
      fetchAmfiStatus();
    }, 10000); // Check every 10 seconds
    
    return () => {
      clearInterval(statusInterval);
      clearInterval(amfiStatusInterval);
    };
  }, []);
  
  const getStatusBadgeClass = (status: string) => {
    switch (status) {
      case "Completed":
        return "bg-green-100 text-green-800";
      case "In Progress":
      case "Starting":
        return "bg-blue-100 text-blue-800";
      case "Failed":
        return "bg-red-100 text-red-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };
  
  const getStatusDot = (status: string) => {
    switch (status) {
      case "Completed":
        return "bg-green-500";
      case "In Progress":
      case "Starting":
        return "bg-blue-500";
      case "Failed":
        return "bg-red-500";
      default:
        return "bg-gray-500";
    }
  };
  
  const handleTriggerCollection = async () => {
    await triggerDataCollection();
  };
  
  const handleTriggerFundDetailsCollection = async () => {
    try {
      toast({
        title: "Starting Fund Details Collection",
        description: "Collecting additional fund details like inception date, expense ratio, and more...",
        variant: "default",
      });
      
      await triggerFundDetailsCollection();
      
      toast({
        title: "Fund Details Collection Started",
        description: "The system is now collecting enhanced fund details in the background.",
        variant: "default",
      });
    } catch (error) {
      toast({
        title: "Collection Failed",
        description: "Failed to start fund details collection. Please try again.",
        variant: "destructive",
      });
    }
  };
  
  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-2xl font-bold text-neutral-900">ETL Pipeline Status</h1>
              <p className="mt-1 text-sm text-neutral-500">
                Manage and monitor data collection and processing tasks
              </p>
            </div>
            <div className="flex items-center space-x-2">
              <Button 
                variant="outline" 
                onClick={refreshEtlStatus} 
                disabled={isLoading}
                className="flex items-center"
              >
                <span className="material-icons text-sm mr-1">refresh</span>
                Refresh
              </Button>
              <Button 
                onClick={handleTriggerCollection} 
                disabled={isCollecting}
                className="flex items-center"
              >
                <span className="material-icons text-sm mr-1">sync</span>
                Collect NAV Data
              </Button>
              <Button 
                onClick={handleTriggerFundDetailsCollection} 
                disabled={isCollectingDetails}
                className="flex items-center"
                variant="outline"
              >
                <span className="material-icons text-sm mr-1">analytics</span>
                Collect Fund Details
              </Button>
            </div>
          </div>
        </div>
        
        {/* Data Source Status Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          <Card className="bg-white">
            <CardContent className="p-4">
              <div className="flex items-center">
                <div className="flex-shrink-0 h-10 w-10 rounded-md bg-success bg-opacity-20 flex items-center justify-center text-success">
                  <span className="material-icons">check_circle</span>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-neutral-900">AMFI Data</h3>
                  <p className="text-xs text-neutral-500">Last Updated: {format(new Date(), "MMM d, h:mm a")}</p>
                </div>
              </div>
              <div className="mt-3">
                <div className="text-xs text-neutral-500">Funds Processed</div>
                <div className="flex justify-between items-center">
                  <div className="text-base font-medium text-neutral-900">
                    {isLoadingAmfiStatus ? (
                      <Skeleton className="h-5 w-16" />
                    ) : (
                      amfiStatus?.fundCount?.toLocaleString() || "Loading..."
                    )}
                  </div>
                  <div className="text-xs text-success">100%</div>
                </div>
                <div className="mt-1 w-full bg-neutral-200 rounded-full h-1.5">
                  <div className="bg-success h-1.5 rounded-full" style={{ width: "100%" }}></div>
                </div>
              </div>
            </CardContent>
          </Card>
          
          <Card className="bg-white">
            <CardContent className="p-4">
              <div className="flex items-center">
                <div className="flex-shrink-0 h-10 w-10 rounded-md bg-warning bg-opacity-20 flex items-center justify-center text-warning">
                  <span className="material-icons">access_time</span>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-neutral-900">NSE Data</h3>
                  <p className="text-xs text-neutral-500">Last Updated: {format(new Date(), "MMM d, h:mm a")}</p>
                </div>
              </div>
              <div className="mt-3">
                <div className="text-xs text-neutral-500">Indices Processed</div>
                <div className="flex justify-between items-center">
                  <div className="text-base font-medium text-neutral-900">38 / 42</div>
                  <div className="text-xs text-warning">90%</div>
                </div>
                <div className="mt-1 w-full bg-neutral-200 rounded-full h-1.5">
                  <div className="bg-warning h-1.5 rounded-full" style={{ width: "90%" }}></div>
                </div>
              </div>
            </CardContent>
          </Card>
          
          <Card className="bg-white">
            <CardContent className="p-4">
              <div className="flex items-center">
                <div className="flex-shrink-0 h-10 w-10 rounded-md bg-success bg-opacity-20 flex items-center justify-center text-success">
                  <span className="material-icons">check_circle</span>
                </div>
                <div className="ml-3">
                  <h3 className="text-sm font-medium text-neutral-900">RBI Data</h3>
                  <p className="text-xs text-neutral-500">Last Updated: {format(new Date(), "MMM d, h:mm a")}</p>
                </div>
              </div>
              <div className="mt-3">
                <div className="text-xs text-neutral-500">Indicators Processed</div>
                <div className="flex justify-between items-center">
                  <div className="text-base font-medium text-neutral-900">12 / 12</div>
                  <div className="text-xs text-success">100%</div>
                </div>
                <div className="mt-1 w-full bg-neutral-200 rounded-full h-1.5">
                  <div className="bg-success h-1.5 rounded-full" style={{ width: "100%" }}></div>
                </div>
              </div>
            </CardContent>
          </Card>
          
          {/* Fund Details Collection Status Card */}
          <Card className="bg-white">
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center">
                  <div className={`flex-shrink-0 h-10 w-10 rounded-md ${isCollectingDetails 
                    ? "bg-blue-100 text-blue-600" 
                    : fundDetailsStats?.percentComplete === 100 
                      ? "bg-success bg-opacity-20 text-success" 
                      : "bg-amber-100 text-amber-600"
                    } flex items-center justify-center`}>
                    <span className="material-icons">
                      {isCollectingDetails 
                        ? "sync" 
                        : fundDetailsStats?.percentComplete === 100 
                          ? "check_circle" 
                          : "info"}
                    </span>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-neutral-900">Fund Details Collection</h3>
                    <p className="text-xs text-neutral-500">
                      Last Run: {fundDetailsStats?.etlStatus?.endTime 
                        ? format(new Date(fundDetailsStats.etlStatus.endTime), "MMM d, h:mm a")
                        : "Never"}
                    </p>
                  </div>
                </div>
                <Button 
                  variant="outline" 
                  size="sm"
                  disabled={isCollectingDetails}
                  onClick={() => triggerFundDetailsCollection()}
                >
                  {isCollectingDetails ? "Running..." : "Collect"}
                </Button>
              </div>
              
              <div className="mt-4">
                <div className="flex justify-between text-xs text-neutral-500 mb-1">
                  <span>Enhanced Details Collection</span>
                  <span className={
                    fundDetailsStats?.percentComplete < 30 
                      ? "text-red-500" 
                      : fundDetailsStats?.percentComplete < 70 
                        ? "text-amber-500" 
                        : "text-green-500"
                  }>
                    {fundDetailsStats?.percentComplete || 0}%
                  </span>
                </div>
                <Progress 
                  value={fundDetailsStats?.percentComplete || 0}
                  className="h-2"
                />
                
                <div className="grid grid-cols-3 gap-2 mt-3">
                  <div className="bg-neutral-50 p-2 rounded-md">
                    <div className="text-xs text-neutral-500">Total Funds</div>
                    <div className="text-base font-medium text-neutral-900">
                      {fundDetailsStats?.totalFunds?.toLocaleString() || 0}
                    </div>
                  </div>
                  <div className="bg-green-50 p-2 rounded-md">
                    <div className="text-xs text-green-600">Enhanced</div>
                    <div className="text-base font-medium text-green-800">
                      {fundDetailsStats?.enhancedFunds?.toLocaleString() || 0}
                    </div>
                  </div>
                  <div className="bg-amber-50 p-2 rounded-md">
                    <div className="text-xs text-amber-600">Pending</div>
                    <div className="text-base font-medium text-amber-800">
                      {fundDetailsStats?.pendingFunds?.toLocaleString() || 0}
                    </div>
                  </div>
                </div>
                
                {isCollectingDetails && (
                  <div className="mt-3 bg-blue-50 p-3 rounded-md">
                    <div className="flex items-center">
                      <span className="material-icons animate-spin text-blue-500 mr-2">sync</span>
                      <span className="text-sm text-blue-700">
                        Processing in batches of 50 funds... Please wait.
                      </span>
                    </div>
                  </div>
                )}
                
                {/* Bulk Processing Configuration */}
                <div className="mt-6 border-t pt-4">
                  <div className="flex justify-between items-center mb-3">
                    <h3 className="text-sm font-semibold text-neutral-800">
                      Automated Bulk Processing
                    </h3>
                    <Dialog>
                      <DialogTrigger asChild>
                        <Button variant="outline" size="sm">
                          Configure
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="sm:max-w-[425px]">
                        <DialogHeader>
                          <DialogTitle>Configure Bulk Processing</DialogTitle>
                          <DialogDescription>
                            Set up automated enhancement of fund details. This will process multiple batches of funds at regular intervals.
                          </DialogDescription>
                        </DialogHeader>
                        <div className="grid gap-4 py-4">
                          <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="batchSize" className="text-right">
                              Batch Size
                            </Label>
                            <Input
                              id="batchSize"
                              type="number"
                              value={bulkConfig.batchSize}
                              onChange={(e) => setBulkConfig({
                                ...bulkConfig,
                                batchSize: parseInt(e.target.value) || 100
                              })}
                              className="col-span-3"
                            />
                          </div>
                          <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="batchCount" className="text-right">
                              Batch Count
                            </Label>
                            <Input
                              id="batchCount"
                              type="number"
                              value={bulkConfig.batchCount}
                              onChange={(e) => setBulkConfig({
                                ...bulkConfig,
                                batchCount: parseInt(e.target.value) || 5
                              })}
                              className="col-span-3"
                            />
                          </div>
                          <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="intervalHours" className="text-right">
                              Interval (hrs)
                            </Label>
                            <Input
                              id="intervalHours"
                              type="number"
                              value={bulkConfig.intervalHours}
                              onChange={(e) => setBulkConfig({
                                ...bulkConfig,
                                intervalHours: parseInt(e.target.value) || 24
                              })}
                              className="col-span-3"
                            />
                          </div>
                        </div>
                        <DialogFooter>
                          <Button 
                            type="submit" 
                            onClick={() => {
                              scheduleBulkProcessing(bulkConfig);
                              toast({
                                title: "Bulk Processing Scheduled",
                                description: `Will process ${bulkConfig.batchSize * bulkConfig.batchCount} funds every ${bulkConfig.intervalHours} hours`,
                              });
                              
                              // Update local state
                              setScheduledStatus({
                                ...scheduledStatus,
                                bulkProcessing: {
                                  active: true,
                                  lastRun: new Date()
                                }
                              });
                            }}
                            disabled={isSchedulingBulk}
                          >
                            {isSchedulingBulk ? "Scheduling..." : "Schedule"}
                          </Button>
                        </DialogFooter>
                      </DialogContent>
                    </Dialog>
                  </div>
                  
                  <div className="bg-neutral-50 p-3 rounded-md mb-2">
                    <div className="grid grid-cols-3 gap-2 text-xs">
                      <div>
                        <span className="text-neutral-500">Funds per run:</span>
                        <span className="ml-1 font-medium">{bulkConfig.batchSize * bulkConfig.batchCount}</span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Frequency:</span>
                        <span className="ml-1 font-medium">Every {bulkConfig.intervalHours} hours</span>
                      </div>
                      <div>
                        <span className="text-neutral-500">Status:</span>
                        <span className={`ml-1 font-medium ${scheduledStatus?.bulkProcessing?.active ? "text-green-600" : "text-amber-600"}`}>
                          {scheduledStatus?.bulkProcessing?.active ? "Active" : "Inactive"}
                        </span>
                      </div>
                    </div>
                  </div>
                  
                  <div className="flex justify-between">
                    <Button 
                      variant="outline" 
                      size="sm" 
                      className="text-blue-600 border-blue-200 hover:bg-blue-50"
                      onClick={() => {
                        scheduleBulkProcessing(bulkConfig);
                        toast({
                          title: "Bulk Processing Started",
                          description: `Processing ${bulkConfig.batchSize * bulkConfig.batchCount} funds automatically`,
                        });
                        
                        // Update local state
                        setScheduledStatus({
                          ...scheduledStatus,
                          bulkProcessing: {
                            active: true,
                            lastRun: new Date()
                          }
                        });
                      }}
                      disabled={isSchedulingBulk || scheduledStatus?.bulkProcessing?.active}
                    >
                      Start Automated Processing
                    </Button>
                    
                    <Button 
                      variant="outline" 
                      size="sm"
                      className="text-red-600 border-red-200 hover:bg-red-50"
                      onClick={() => {
                        stopBulkProcessing();
                        toast({
                          title: "Bulk Processing Stopped",
                          description: "Automated fund details processing has been stopped",
                        });
                        
                        // Update local state
                        setScheduledStatus({
                          ...scheduledStatus,
                          bulkProcessing: {
                            active: false,
                            lastRun: scheduledStatus?.bulkProcessing?.lastRun || null
                          }
                        });
                      }}
                      disabled={!scheduledStatus?.bulkProcessing?.active}
                    >
                      Stop Processing
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
        
        {/* Pipeline Health Table */}
        <Card className="bg-white">
          <CardHeader>
            <CardTitle>Pipeline Health</CardTitle>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
              </div>
            ) : error ? (
              <div className="bg-red-50 p-4 rounded-md text-red-700">
                Error loading ETL status: {error}
              </div>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader className="bg-neutral-100">
                    <TableRow>
                      <TableHead>Process</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Last Run</TableHead>
                      <TableHead>Duration</TableHead>
                      <TableHead>Records</TableHead>
                      <TableHead className="text-right">Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {etlRuns?.map((run) => {
                      // Calculate duration
                      const startTime = new Date(run.startTime);
                      const endTime = run.endTime ? new Date(run.endTime) : new Date();
                      const durationMs = endTime.getTime() - startTime.getTime();
                      const durationMinutes = Math.floor(durationMs / 60000);
                      const durationSeconds = Math.floor((durationMs % 60000) / 1000);
                      const durationText = `${durationMinutes}m ${durationSeconds}s`;
                      
                      return (
                        <TableRow key={run.id}>
                          <TableCell className="font-medium">{run.pipelineName}</TableCell>
                          <TableCell>
                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getStatusBadgeClass(run.status)}`}>
                              <span className={`h-1.5 w-1.5 rounded-full ${getStatusDot(run.status)} mr-1.5`}></span>
                              {run.status}
                            </span>
                          </TableCell>
                          <TableCell>{format(new Date(run.startTime), "MMM d, h:mm a")}</TableCell>
                          <TableCell>{durationText}</TableCell>
                          <TableCell>{run.recordsProcessed || "-"}</TableCell>
                          <TableCell className="text-right">
                            <Button variant="link" size="sm" className="text-primary-600 hover:text-primary-900">
                              View Logs
                            </Button>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
        
        {/* ETL Pipeline Configuration */}
        <div className="mt-6">
          <Card>
            <CardHeader>
              <CardTitle>Automated Import Schedule</CardTitle>
              <CardDescription>Configure scheduled data imports to keep mutual fund data fresh</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-neutral-50 p-4 rounded-lg border">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-neutral-900">Daily NAV Updates</h3>
                    {scheduledStatus.daily.active && (
                      <Badge className="bg-green-100 text-green-800">Active</Badge>
                    )}
                    {!scheduledStatus.daily.active && (
                      <Badge variant="outline" className="bg-neutral-200 text-neutral-600">Inactive</Badge>
                    )}
                  </div>
                  <div className="text-xs text-neutral-600 mb-1">Source: AMFI Website</div>
                  <div className="text-xs text-neutral-600 mb-3">Frequency: Daily</div>
                  
                  {scheduledStatus.daily.lastRun && (
                    <div className="mb-3 text-xs text-neutral-600">
                      <div>Last Run: {new Date(scheduledStatus.daily.lastRun.startTime).toLocaleString()}</div>
                      <div>Status: {scheduledStatus.daily.lastRun.status}</div>
                      <div>Records: {scheduledStatus.daily.lastRun.recordsProcessed || 0}</div>
                    </div>
                  )}
                  
                  <div className="flex space-x-2 mt-3">
                    {!scheduledStatus.daily.active ? (
                      <Button 
                        variant="default" 
                        size="sm" 
                        className="flex-1"
                        disabled={isScheduling}
                        onClick={() => scheduleImport('daily', 'daily')}
                      >
                        {isScheduling ? 'Starting...' : 'Start Schedule'}
                      </Button>
                    ) : (
                      <Button 
                        variant="destructive" 
                        size="sm" 
                        className="flex-1"
                        disabled={isScheduling}
                        onClick={() => stopScheduledImport('daily')}
                      >
                        {isScheduling ? 'Stopping...' : 'Stop Schedule'}
                      </Button>
                    )}
                    <Button 
                      variant="outline" 
                      size="sm" 
                      className="flex items-center"
                      onClick={async () => {
                        try {
                          await triggerDataCollection();
                          toast({
                            title: "Import Started",
                            description: "Manual daily NAV import has been initiated",
                            variant: "default",
                          });
                        } catch (error) {
                          console.error('Error triggering import:', error);
                        }
                      }}
                      disabled={isCollecting}
                    >
                      <span className="material-icons text-sm mr-1">sync</span>
                      Run Now
                    </Button>
                  </div>
                </div>
                
                <div className="bg-neutral-50 p-4 rounded-lg border">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-neutral-900">36-Month Historical Import</h3>
                    {scheduledStatus.historical.active && (
                      <Badge className="bg-green-100 text-green-800">Active</Badge>
                    )}
                    {!scheduledStatus.historical.active && (
                      <Badge variant="outline" className="bg-neutral-200 text-neutral-600">Inactive</Badge>
                    )}
                  </div>
                  <div className="text-xs text-neutral-600 mb-1">Source: AMFI Historical Archive</div>
                  <div className="text-xs text-neutral-600 mb-3">Frequency: Weekly</div>
                  
                  {scheduledStatus.historical.lastRun && (
                    <div className="mb-3 text-xs text-neutral-600">
                      <div>Last Run: {new Date(scheduledStatus.historical.lastRun.startTime).toLocaleString()}</div>
                      <div>Status: {scheduledStatus.historical.lastRun.status}</div>
                      <div>Records: {scheduledStatus.historical.lastRun.recordsProcessed || 0}</div>
                    </div>
                  )}
                  
                  <div className="flex space-x-2 mt-3">
                    {!scheduledStatus.historical.active ? (
                      <Button 
                        variant="default" 
                        size="sm" 
                        className="flex-1"
                        disabled={isScheduling}
                        onClick={() => scheduleImport('historical', 'weekly')}
                      >
                        {isScheduling ? 'Starting...' : 'Start Schedule'}
                      </Button>
                    ) : (
                      <Button 
                        variant="destructive" 
                        size="sm" 
                        className="flex-1"
                        disabled={isScheduling}
                        onClick={() => stopScheduledImport('historical')}
                      >
                        {isScheduling ? 'Stopping...' : 'Stop Schedule'}
                      </Button>
                    )}
                    <Button 
                      variant="outline" 
                      size="sm" 
                      className="flex items-center"
                      onClick={async () => {
                        try {
                          // Start a manual historical import
                          const response = await axios.get('/api/amfi?historical=true');
                          if (response.data.success) {
                            toast({
                              title: "Import Started",
                              description: "Manual historical import has been initiated",
                              variant: "default",
                            });
                          }
                        } catch (error) {
                          console.error('Error triggering historical import:', error);
                          toast({
                            title: "Import Failed",
                            description: "Failed to start historical import",
                            variant: "destructive",
                          });
                        }
                      }}
                      disabled={isCollecting}
                    >
                      <span className="material-icons text-sm mr-1">sync</span>
                      Run Now
                    </Button>
                  </div>
                </div>
                
                {/* Fund Details Collection Card */}
                <div className="bg-neutral-50 p-4 rounded-lg border">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-sm font-medium text-neutral-900">Enhanced Fund Details</h3>
                    {/* Use optional chaining to safely access properties */}
                    {scheduledStatus?.fundDetails?.active ? (
                      <Badge className="bg-green-100 text-green-800">Active</Badge>
                    ) : (
                      <Badge variant="outline" className="bg-neutral-200 text-neutral-600">Inactive</Badge>
                    )}
                  </div>
                  <div className="text-xs text-neutral-600 mb-1">Source: AMFI Web Scraping</div>
                  <div className="text-xs text-neutral-600 mb-3">Frequency: Weekly</div>
                  
                  {/* Fund Details Progress */}
                  <div className="mb-4">
                    <div className="flex justify-between items-center mb-1">
                      <span className="text-xs text-neutral-600">Collection Progress</span>
                      <span className="text-xs font-medium">
                        {fundDetailsStats?.percentComplete || 0}%
                      </span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-blue-600 h-2 rounded-full" 
                        style={{ width: `${fundDetailsStats?.percentComplete || 0}%` }}
                      ></div>
                    </div>
                    <div className="flex justify-between mt-1 text-xs text-neutral-600">
                      <span>Enhanced: {(fundDetailsStats?.enhancedFunds || 0).toLocaleString()}</span>
                      <span>Pending: {(fundDetailsStats?.pendingFunds || 0).toLocaleString()}</span>
                    </div>
                    <div className="text-xs text-neutral-600 mt-1">
                      Total Funds: {(fundDetailsStats?.totalFunds || 0).toLocaleString()}
                    </div>
                  </div>
                  
                  {scheduledStatus?.fundDetails?.lastRun ? (
                    <div className="mb-3 text-xs text-neutral-600">
                      <div>Last Run: {new Date(scheduledStatus.fundDetails.lastRun.startTime).toLocaleString()}</div>
                      <div>Status: {scheduledStatus.fundDetails.lastRun.status}</div>
                      <div>Records: {scheduledStatus.fundDetails.lastRun.recordsProcessed || 0}</div>
                    </div>
                  ) : (
                    <div className="mb-3 text-xs text-neutral-600">
                      <div>No recent collection runs</div>
                    </div>
                  )}
                  
                  <div className="flex space-x-2 mt-3">
                    <Button 
                      variant="outline" 
                      size="sm" 
                      className="flex-1"
                      disabled={isCollectingDetails}
                      onClick={() => {
                        scheduleFundDetailsCollection(168); // Weekly schedule (168 hours)
                        toast({
                          title: "Fund Details Collection Scheduled",
                          description: "Weekly fund details collection has been scheduled",
                          variant: "default",
                        });
                      }}
                    >
                      Schedule Weekly
                    </Button>
                    <Button 
                      variant="default" 
                      size="sm" 
                      className="flex items-center"
                      onClick={handleTriggerFundDetailsCollection}
                      disabled={isCollectingDetails}
                    >
                      <span className="material-icons text-sm mr-1">analytics</span>
                      Run Now
                    </Button>
                  </div>
                </div>
              </div>
              
              <div className="mt-4">
                <Card className="bg-gray-50 border-dashed border-gray-300">
                  <CardContent className="p-4">
                    <div className="flex items-start">
                      <div className="flex-shrink-0 h-10 w-10 rounded-md bg-blue-100 flex items-center justify-center text-blue-600 mr-3">
                        <span className="material-icons">info</span>
                      </div>
                      <div>
                        <h3 className="text-sm font-medium text-neutral-900 mb-1">Scheduled Import Information</h3>
                        <ul className="text-xs text-neutral-600 space-y-1 list-disc pl-4">
                          <li>Daily NAV updates: Get the latest NAV values for all funds (runs once per day)</li>
                          <li>Historical Import: Updates 36 months of historical NAV data (runs weekly)</li>
                          <li>Both tasks will be tracked in the Pipeline Health table above</li>
                          <li>The database currently contains 16,766 funds with 15,216 NAV records</li>
                        </ul>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
              
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
