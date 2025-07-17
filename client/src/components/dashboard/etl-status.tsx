import React from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useEtlStatus } from "@/hooks/use-etl-status";
import { Skeleton } from "@/components/ui/skeleton";
import { format } from "date-fns";

export default function EtlStatus() {
  const { etlRuns, refreshEtlStatus, triggerDataCollection, isLoading, isCollecting } = useEtlStatus();
  
  // Get the most recent ETL runs for each pipeline
  const amfiRun = etlRuns?.find(run => run.pipelineName === "AMFI Data Collection");
  const nseRun = etlRuns?.find(run => run.pipelineName === "NSE Data Collection");
  const rbiRun = etlRuns?.find(run => run.pipelineName === "RBI Data Collection");
  
  const formatTime = (date: Date | string | undefined) => {
    if (!date) return "N/A";
    return format(new Date(date), "MMM d, h:mm a");
  };
  
  const calculateProgress = (run: any) => {
    if (!run) return 0;
    if (run.status === "Completed") return 100;
    if (run.status === "Failed") return 0;
    return run.recordsProcessed ? Math.min(90, (run.recordsProcessed / (run.recordsProcessed + 10)) * 100) : 50;
  };
  
  return (
    <div className="px-4 sm:px-6 lg:px-8 mb-6">
      <Card>
        <CardContent className="p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-neutral-900">ETL Pipeline Status</h2>
            <div className="flex items-center">
              <Button 
                variant="outline" 
                className="flex items-center"
                onClick={refreshEtlStatus}
                disabled={isLoading}
              >
                <span className="material-icons text-sm mr-1">history</span>
                <span className="ml-1">View History</span>
              </Button>
            </div>
          </div>
          
          {isLoading ? (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
              <Skeleton className="h-32 w-full" />
              <Skeleton className="h-32 w-full" />
              <Skeleton className="h-32 w-full" />
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
              <div className="bg-neutral-50 rounded-lg p-4">
                <div className="flex items-center">
                  <div className={`flex-shrink-0 h-10 w-10 rounded-md ${
                    !amfiRun || amfiRun.status === "Failed"
                      ? "bg-danger bg-opacity-20 text-danger"
                      : amfiRun.status === "Completed"
                      ? "bg-success bg-opacity-20 text-success"
                      : "bg-warning bg-opacity-20 text-warning"
                  } flex items-center justify-center`}>
                    <span className="material-icons">
                      {!amfiRun || amfiRun.status === "Failed"
                        ? "error"
                        : amfiRun.status === "Completed"
                        ? "check_circle"
                        : "access_time"}
                    </span>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-neutral-900">AMFI Data</h3>
                    <p className="text-xs text-neutral-500">Last Updated: {formatTime(amfiRun?.endTime || amfiRun?.startTime)}</p>
                  </div>
                </div>
                <div className="mt-3">
                  <div className="text-xs text-neutral-500">Funds Processed</div>
                  <div className="flex justify-between items-center">
                    <div className="text-base font-medium text-neutral-900">
                      {amfiRun?.recordsProcessed || "0"}
                    </div>
                    <div className={`text-xs ${
                      !amfiRun || amfiRun.status === "Failed"
                        ? "text-danger"
                        : amfiRun.status === "Completed"
                        ? "text-success"
                        : "text-warning"
                    }`}>
                      {amfiRun?.status || "Not Started"}
                    </div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-1.5">
                    <div 
                      className={`h-1.5 rounded-full ${
                        !amfiRun || amfiRun.status === "Failed"
                          ? "bg-danger"
                          : amfiRun.status === "Completed"
                          ? "bg-success"
                          : "bg-warning"
                      }`}
                      style={{ width: `${calculateProgress(amfiRun)}%` }}
                    ></div>
                  </div>
                </div>
              </div>
              
              <div className="bg-neutral-50 rounded-lg p-4">
                <div className="flex items-center">
                  <div className={`flex-shrink-0 h-10 w-10 rounded-md ${
                    !nseRun || nseRun.status === "Failed"
                      ? "bg-danger bg-opacity-20 text-danger"
                      : nseRun.status === "Completed"
                      ? "bg-success bg-opacity-20 text-success"
                      : "bg-warning bg-opacity-20 text-warning"
                  } flex items-center justify-center`}>
                    <span className="material-icons">
                      {!nseRun || nseRun.status === "Failed"
                        ? "error"
                        : nseRun.status === "Completed"
                        ? "check_circle"
                        : "access_time"}
                    </span>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-neutral-900">NSE Data</h3>
                    <p className="text-xs text-neutral-500">Last Updated: {formatTime(nseRun?.endTime || nseRun?.startTime)}</p>
                  </div>
                </div>
                <div className="mt-3">
                  <div className="text-xs text-neutral-500">Indices Processed</div>
                  <div className="flex justify-between items-center">
                    <div className="text-base font-medium text-neutral-900">
                      {nseRun?.recordsProcessed ? `${nseRun.recordsProcessed}` : "0"}
                    </div>
                    <div className={`text-xs ${
                      !nseRun || nseRun.status === "Failed"
                        ? "text-danger"
                        : nseRun.status === "Completed"
                        ? "text-success"
                        : "text-warning"
                    }`}>
                      {nseRun?.status || "Not Started"}
                    </div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-1.5">
                    <div 
                      className={`h-1.5 rounded-full ${
                        !nseRun || nseRun.status === "Failed"
                          ? "bg-danger"
                          : nseRun.status === "Completed"
                          ? "bg-success"
                          : "bg-warning"
                      }`}
                      style={{ width: `${calculateProgress(nseRun)}%` }}
                    ></div>
                  </div>
                </div>
              </div>
              
              <div className="bg-neutral-50 rounded-lg p-4">
                <div className="flex items-center">
                  <div className={`flex-shrink-0 h-10 w-10 rounded-md ${
                    !rbiRun || rbiRun.status === "Failed"
                      ? "bg-danger bg-opacity-20 text-danger"
                      : rbiRun.status === "Completed"
                      ? "bg-success bg-opacity-20 text-success"
                      : "bg-warning bg-opacity-20 text-warning"
                  } flex items-center justify-center`}>
                    <span className="material-icons">
                      {!rbiRun || rbiRun.status === "Failed"
                        ? "error"
                        : rbiRun.status === "Completed"
                        ? "check_circle"
                        : "access_time"}
                    </span>
                  </div>
                  <div className="ml-3">
                    <h3 className="text-sm font-medium text-neutral-900">RBI Data</h3>
                    <p className="text-xs text-neutral-500">Last Updated: {formatTime(rbiRun?.endTime || rbiRun?.startTime)}</p>
                  </div>
                </div>
                <div className="mt-3">
                  <div className="text-xs text-neutral-500">Indicators Processed</div>
                  <div className="flex justify-between items-center">
                    <div className="text-base font-medium text-neutral-900">
                      {rbiRun?.recordsProcessed ? `${rbiRun.recordsProcessed}` : "0"}
                    </div>
                    <div className={`text-xs ${
                      !rbiRun || rbiRun.status === "Failed"
                        ? "text-danger"
                        : rbiRun.status === "Completed"
                        ? "text-success"
                        : "text-warning"
                    }`}>
                      {rbiRun?.status || "Not Started"}
                    </div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-1.5">
                    <div 
                      className={`h-1.5 rounded-full ${
                        !rbiRun || rbiRun.status === "Failed"
                          ? "bg-danger"
                          : rbiRun.status === "Completed"
                          ? "bg-success"
                          : "bg-warning"
                      }`}
                      style={{ width: `${calculateProgress(rbiRun)}%` }}
                    ></div>
                  </div>
                </div>
              </div>
            </div>
          )}
          
          <div className="bg-neutral-50 rounded-lg p-4">
            <h3 className="text-base font-medium text-neutral-900 mb-3">Pipeline Health</h3>
            {isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
                <Skeleton className="h-8 w-full" />
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-neutral-200">
                  <thead className="bg-neutral-100">
                    <tr>
                      <th scope="col" className="px-4 py-2 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Process</th>
                      <th scope="col" className="px-4 py-2 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Status</th>
                      <th scope="col" className="px-4 py-2 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Last Run</th>
                      <th scope="col" className="px-4 py-2 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Duration</th>
                      <th scope="col" className="px-4 py-2 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Records</th>
                      <th scope="col" className="px-4 py-2 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-neutral-200">
                    {etlRuns?.slice(0, 5).map((run) => {
                      // Calculate duration
                      const startTime = new Date(run.startTime);
                      const endTime = run.endTime ? new Date(run.endTime) : new Date();
                      const durationMs = endTime.getTime() - startTime.getTime();
                      const durationMinutes = Math.floor(durationMs / 60000);
                      const durationSeconds = Math.floor((durationMs % 60000) / 1000);
                      const durationText = `${durationMinutes}m ${durationSeconds}s`;
                      
                      return (
                        <tr key={run.id}>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-neutral-900">{run.pipelineName}</td>
                          <td className="px-4 py-2 whitespace-nowrap">
                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                              run.status === "Completed" 
                                ? "bg-green-100 text-green-800" 
                                : run.status === "Failed" 
                                ? "bg-red-100 text-red-800" 
                                : "bg-blue-100 text-blue-800"
                            }`}>
                              <span className={`h-1.5 w-1.5 rounded-full ${
                                run.status === "Completed" 
                                  ? "bg-green-500" 
                                  : run.status === "Failed" 
                                  ? "bg-red-500" 
                                  : "bg-blue-500"
                              } mr-1.5`}></span>
                              {run.status}
                            </span>
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-neutral-500">
                            {formatTime(run.startTime)}
                          </td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-neutral-500">{durationText}</td>
                          <td className="px-4 py-2 whitespace-nowrap text-sm text-neutral-500">{run.recordsProcessed || "-"}</td>
                          <td className="px-4 py-2 whitespace-nowrap text-right text-sm font-medium">
                            <Button variant="link" size="sm" className="text-primary-600 hover:text-primary-900">
                              View Logs
                            </Button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
