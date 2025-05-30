import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { Play, Square, RefreshCw, Database, TrendingUp } from "lucide-react";

interface ImportProgress {
  totalFundsProcessed: number;
  totalRecordsImported: number;
  currentBatch: number;
  lastProcessedFund: string;
  isRunning: boolean;
}

export default function HistoricalImportDashboard() {
  const queryClient = useQueryClient();
  
  const { data: importStatus, isLoading } = useQuery({
    queryKey: ["/api/historical-import/status"],
    refetchInterval: 5000, // Refresh every 5 seconds
  });

  const { data: navStats } = useQuery({
    queryKey: ["/api/amfi/status"],
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  const startImportMutation = useMutation({
    mutationFn: async () => {
      const response = await fetch("/api/historical-import/start", {
        method: "POST",
      });
      if (!response.ok) throw new Error("Failed to start import");
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/historical-import/status"] });
    },
  });

  const stopImportMutation = useMutation({
    mutationFn: async () => {
      const response = await fetch("/api/historical-import/stop", {
        method: "POST",
      });
      if (!response.ok) throw new Error("Failed to stop import");
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/historical-import/status"] });
    },
  });

  const progress = importStatus?.progress as ImportProgress;
  const isRunning = progress?.isRunning || false;

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat().format(num);
  };

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Historical Data Import</h1>
          <p className="text-muted-foreground mt-2">
            Background service continuously importing authentic NAV data from free APIs
          </p>
        </div>
        
        <div className="flex gap-2">
          <Button
            onClick={() => startImportMutation.mutate()}
            disabled={isRunning || startImportMutation.isPending}
            variant={isRunning ? "secondary" : "default"}
          >
            <Play className="w-4 h-4 mr-2" />
            {isRunning ? "Running" : "Start Import"}
          </Button>
          
          <Button
            onClick={() => stopImportMutation.mutate()}
            disabled={!isRunning || stopImportMutation.isPending}
            variant="outline"
          >
            <Square className="w-4 h-4 mr-2" />
            Stop
          </Button>
        </div>
      </div>

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium flex items-center">
              <Database className="w-4 h-4 mr-2" />
              Import Status
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Badge variant={isRunning ? "default" : "secondary"} className="mb-2">
              {isRunning ? "Running" : "Stopped"}
            </Badge>
            {progress?.currentBatch && (
              <p className="text-sm text-muted-foreground">
                Batch #{progress.currentBatch}
              </p>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Funds Processed</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {formatNumber(progress?.totalFundsProcessed || 0)}
            </div>
            <p className="text-xs text-muted-foreground">
              Total funds analyzed
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Records Imported</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-green-600">
              {formatNumber(progress?.totalRecordsImported || 0)}
            </div>
            <p className="text-xs text-muted-foreground">
              NAV records added
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium">Total NAV Data</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {formatNumber(navStats?.navCount || 0)}
            </div>
            <p className="text-xs text-muted-foreground">
              Total records in system
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Current Processing */}
      {progress?.lastProcessedFund && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <RefreshCw className={`w-4 h-4 mr-2 ${isRunning ? "animate-spin" : ""}`} />
              Current Processing
            </CardTitle>
          </CardHeader>
          <CardContent>
            <p className="font-medium">{progress.lastProcessedFund}</p>
            <p className="text-sm text-muted-foreground mt-1">
              Fetching historical NAV data from authentic sources
            </p>
          </CardContent>
        </Card>
      )}

      {/* System Overview */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center">
            <TrendingUp className="w-4 h-4 mr-2" />
            Data Coverage Overview
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-sm font-medium">Total Funds</p>
              <p className="text-2xl font-bold">
                {formatNumber(navStats?.fundCount || 0)}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium">Total NAV Records</p>
              <p className="text-2xl font-bold">
                {formatNumber(navStats?.navCount || 0)}
              </p>
            </div>
          </div>
          
          <div className="space-y-2">
            <div className="flex justify-between text-sm">
              <span>Data Coverage Progress</span>
              <span>Continuous Import Active</span>
            </div>
            <div className="text-xs text-muted-foreground">
              Background service automatically imports historical data 24/7 using free APIs
            </div>
          </div>
        </CardContent>
      </Card>

      {/* How It Works */}
      <Card>
        <CardHeader>
          <CardTitle>Background Import Process</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="space-y-2 text-sm">
            <p><strong>Continuous Operation:</strong> Runs 24/7 in the background, even when your laptop is shut down</p>
            <p><strong>Authentic Data:</strong> Fetches real NAV data from MFAPI.in and other free financial APIs</p>
            <p><strong>Parallel Processing:</strong> Processes 3 funds simultaneously with 6-month data chunks in parallel</p>
            <p><strong>Smart Processing:</strong> Prioritizes funds with insufficient data for quartile analysis</p>
            <p><strong>Optimized Speed:</strong> 0.5 second delays between requests, 15 seconds between batches</p>
            <p><strong>Extended Coverage:</strong> Imports up to 10 years of historical data per fund</p>
            <p><strong>Fault Tolerant:</strong> Continues processing even if individual fund data fails</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}