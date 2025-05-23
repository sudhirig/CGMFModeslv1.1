import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useEtlStatus } from "@/hooks/use-etl-status";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { format } from "date-fns";

export default function EtlPipeline() {
  const { etlRuns, refreshEtlStatus, triggerDataCollection, isLoading, isCollecting, error } = useEtlStatus();
  
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
                Collect Data
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
                  <div className="text-base font-medium text-neutral-900">2,345</div>
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
              <CardTitle>Schedule Configuration</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="bg-neutral-50 p-4 rounded-lg">
                  <h3 className="text-sm font-medium text-neutral-900 mb-2">AMFI Data Collection</h3>
                  <div className="text-xs text-neutral-500">Schedule: Daily at 9:00 AM</div>
                  <div className="mt-2 text-xs text-neutral-500">Source: AMFI Website</div>
                  <div className="mt-3">
                    <Button variant="outline" size="sm" className="w-full">
                      Edit Schedule
                    </Button>
                  </div>
                </div>
                
                <div className="bg-neutral-50 p-4 rounded-lg">
                  <h3 className="text-sm font-medium text-neutral-900 mb-2">NSE Data Collection</h3>
                  <div className="text-xs text-neutral-500">Schedule: Daily at 5:30 PM</div>
                  <div className="mt-2 text-xs text-neutral-500">Source: NSE APIs</div>
                  <div className="mt-3">
                    <Button variant="outline" size="sm" className="w-full">
                      Edit Schedule
                    </Button>
                  </div>
                </div>
                
                <div className="bg-neutral-50 p-4 rounded-lg">
                  <h3 className="text-sm font-medium text-neutral-900 mb-2">RBI Data Collection</h3>
                  <div className="text-xs text-neutral-500">Schedule: Weekly on Fridays</div>
                  <div className="mt-2 text-xs text-neutral-500">Source: RBI Data Portal</div>
                  <div className="mt-3">
                    <Button variant="outline" size="sm" className="w-full">
                      Edit Schedule
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
