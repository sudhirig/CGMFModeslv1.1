import React from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Clock, CheckCircle, AlertTriangle, BarChart2 } from "lucide-react";

interface NavImportStatusProps {
  amfiStatus: {
    fundCount: number;
    navCount: number;
    navWithHistoryCount: number;
    isLoading: boolean;
    error: any;
  };
}

export function NavImportStatus({ amfiStatus }: NavImportStatusProps) {
  const { fundCount, navCount, navWithHistoryCount, isLoading, error } = amfiStatus;

  // Calculate progress percentages
  const phase1Progress = Math.round((navCount / fundCount) * 100) || 0;
  const phase2Progress = Math.round((navWithHistoryCount / fundCount) * 100) || 0;

  // Determine the current phase
  const phase1Complete = phase1Progress >= 100;
  const phase2Complete = phase2Progress >= 90; // Consider historical data complete at 90% since some funds may not have full history
  
  // Generate appropriate status messages
  const getStatusMessage = () => {
    if (error) {
      return "Error loading NAV data status";
    }
    
    if (isLoading) {
      return "Loading NAV data status...";
    }
    
    if (phase1Complete && phase2Complete) {
      return "NAV data import complete. Fund scoring and quartile analysis ready.";
    }
    
    if (phase1Complete) {
      return `Current NAV data complete. Historical data import in progress (${phase2Progress}% complete).`;
    }
    
    return `NAV data import in progress. Phase 1: ${phase1Progress}% complete`;
  };

  // Get appropriate icon
  const StatusIcon = () => {
    if (error) return <AlertTriangle className="h-5 w-5 text-amber-500" />;
    if (phase1Complete && phase2Complete) return <CheckCircle className="h-5 w-5 text-green-500" />;
    return <Clock className="h-5 w-5 text-blue-500" />;
  };

  if (isLoading) {
    return (
      <Card className="w-full mb-4">
        <CardContent className="pt-6">
          <div className="flex items-center mb-2">
            <Skeleton className="h-5 w-5 mr-2 rounded-full" />
            <Skeleton className="h-5 w-40" />
          </div>
          <Skeleton className="h-4 w-full mt-2" />
          <Skeleton className="h-4 w-3/4 mt-2" />
          <Skeleton className="h-5 w-full mt-4" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="w-full mb-4">
      <CardContent className="pt-6">
        <div className="flex items-center mb-4">
          <StatusIcon />
          <h3 className="text-lg font-medium ml-2">NAV Data Import Status</h3>
        </div>
        
        <Alert className={error ? "border-amber-200 bg-amber-50" : "border-blue-200 bg-blue-50"}>
          <AlertTitle className="flex items-center">
            {error ? (
              <AlertTriangle className="h-4 w-4 mr-2 text-amber-500" />
            ) : (
              <BarChart2 className="h-4 w-4 mr-2 text-blue-500" />
            )}
            Import Progress
          </AlertTitle>
          <AlertDescription>
            {getStatusMessage()}
          </AlertDescription>
        </Alert>
        
        <div className="mt-4">
          <div className="flex justify-between text-sm mb-1">
            <span>Phase 1: Current NAV Import</span>
            <span className="font-medium">{phase1Progress}%</span>
          </div>
          <Progress value={phase1Progress} className="h-2 mb-3" />
          
          <div className="flex justify-between text-sm mb-1">
            <span>Phase 2: Historical NAV Import</span>
            <span className="font-medium">{phase2Progress}%</span>
          </div>
          <Progress value={phase2Progress} className="h-2" />
        </div>
        
        <div className="mt-4 text-sm text-neutral-600">
          <div className="flex justify-between border-b pb-1 mb-1">
            <span>Total Funds:</span>
            <span className="font-medium">{fundCount.toLocaleString()}</span>
          </div>
          <div className="flex justify-between border-b pb-1 mb-1">
            <span>Funds with Current NAV:</span>
            <span className="font-medium">{navCount.toLocaleString()}</span>
          </div>
          <div className="flex justify-between">
            <span>Funds with Historical NAV:</span>
            <span className="font-medium">{navWithHistoryCount.toLocaleString()}</span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}