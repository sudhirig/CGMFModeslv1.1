import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Clock, Play, CheckCircle, AlertCircle, TrendingUp, Database, Calendar } from 'lucide-react';

interface SchedulerStatus {
  isRunning: boolean;
  lastFullRecalculation: string | null;
  scheduledTasks: string[];
}

interface FundEligibility {
  totalFunds: number;
  eligibleFunds: number;
  categories: Record<string, { total: number; eligible: number }>;
}

export default function AutomationDashboard() {
  const queryClient = useQueryClient();
  const [lastTrigger, setLastTrigger] = useState<string | null>(null);

  // Fetch scheduler status
  const { data: schedulerStatus, isLoading: statusLoading } = useQuery({
    queryKey: ['/api/scheduler/status'],
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  // Fetch fund eligibility stats
  const { data: eligibilityStats } = useQuery({
    queryKey: ['/api/funds/eligibility-stats'],
    refetchInterval: 60000, // Refresh every minute
  });

  // Fetch authentic quartile distribution (all synthetic data eliminated)
  const { data: quartileDistribution } = useQuery({
    queryKey: ['/api/authentic-quartile/distribution'],
    refetchInterval: 60000,
  });

  // Trigger mutations
  const triggerDailyMutation = useMutation({
    mutationFn: () => fetch('/api/scheduler/trigger-daily', { method: 'POST' }).then(r => r.json()),
    onSuccess: () => {
      setLastTrigger('Daily Check');
      queryClient.invalidateQueries({ queryKey: ['/api/scheduler/status'] });
    },
  });

  const triggerWeeklyMutation = useMutation({
    mutationFn: () => fetch('/api/scheduler/trigger-weekly', { method: 'POST' }).then(r => r.json()),
    onSuccess: () => {
      setLastTrigger('Weekly Recalculation');
      queryClient.invalidateQueries({ queryKey: ['/api/scheduler/status'] });
    },
  });

  const triggerMigrationMutation = useMutation({
    mutationFn: () => fetch('/api/scheduler/trigger-migration', { method: 'POST' }).then(r => r.json()),
    onSuccess: () => {
      setLastTrigger('Migration Tracking');
      queryClient.invalidateQueries({ queryKey: ['/api/scheduler/status'] });
    },
  });

  if (statusLoading) {
    return (
      <div className="container mx-auto p-6">
        <div className="flex items-center space-x-2 mb-6">
          <Clock className="h-6 w-6" />
          <div className="h-6 w-32 bg-gray-200 animate-pulse rounded"></div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[...Array(6)].map((_, i) => (
            <Card key={i}>
              <CardHeader className="space-y-2">
                <div className="h-4 w-24 bg-gray-200 animate-pulse rounded"></div>
                <div className="h-3 w-full bg-gray-200 animate-pulse rounded"></div>
              </CardHeader>
              <CardContent>
                <div className="h-8 w-16 bg-gray-200 animate-pulse rounded"></div>
              </CardContent>
            </Card>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-6">
      <div className="flex items-center space-x-2 mb-6">
        <Clock className="h-6 w-6 text-blue-600" />
        <h1 className="text-2xl font-bold">Automated Quartile System</h1>
        <Badge variant={schedulerStatus?.isRunning ? "default" : "secondary"}>
          {schedulerStatus?.isRunning ? 'Running' : 'Idle'}
        </Badge>
      </div>

      {lastTrigger && (
        <Alert className="mb-6">
          <CheckCircle className="h-4 w-4" />
          <AlertDescription>
            {lastTrigger} triggered successfully. Check the logs for details.
          </AlertDescription>
        </Alert>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
        
        {/* System Status */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Database className="h-5 w-5" />
              <span>System Status</span>
            </CardTitle>
            <CardDescription>Current automation state</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Status</span>
                <Badge variant={schedulerStatus?.isRunning ? "default" : "secondary"}>
                  {schedulerStatus?.isRunning ? 'Active' : 'Standby'}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Last Recalculation</span>
                <span className="text-sm font-medium">
                  {schedulerStatus?.lastFullRecalculation 
                    ? new Date(schedulerStatus.lastFullRecalculation).toLocaleDateString()
                    : 'Never'
                  }
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Scheduled Tasks</span>
                <span className="text-sm font-medium">
                  {schedulerStatus?.scheduledTasks?.length || 0} active
                </span>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Quartile Distribution */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <TrendingUp className="h-5 w-5" />
              <span>Quartile Distribution</span>
            </CardTitle>
            <CardDescription>Current fund classifications</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-sm text-green-600">Q1 (BUY)</span>
                <Badge variant="default" className="bg-green-100 text-green-800">
                  {quartileDistribution?.q1Count || 0}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-blue-600">Q2 (HOLD)</span>
                <Badge variant="secondary" className="bg-blue-100 text-blue-800">
                  {quartileDistribution?.q2Count || 0}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-yellow-600">Q3 (REVIEW)</span>
                <Badge variant="outline" className="bg-yellow-100 text-yellow-800">
                  {quartileDistribution?.q3Count || 0}
                </Badge>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-red-600">Q4 (SELL)</span>
                <Badge variant="destructive" className="bg-red-100 text-red-800">
                  {quartileDistribution?.q4Count || 0}
                </Badge>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Fund Eligibility */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <CheckCircle className="h-5 w-5" />
              <span>Fund Eligibility</span>
            </CardTitle>
            <CardDescription>Funds with sufficient data</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Total Funds</span>
                <span className="text-lg font-bold">{quartileDistribution?.totalCount || 0}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-600">Eligible for Analysis</span>
                <span className="text-lg font-bold text-green-600">
                  {(quartileDistribution?.q1Count || 0) + 
                   (quartileDistribution?.q2Count || 0) + 
                   (quartileDistribution?.q3Count || 0) + 
                   (quartileDistribution?.q4Count || 0)}
                </span>
              </div>
              <div className="w-full bg-gray-200 rounded-full h-2">
                <div 
                  className="bg-green-600 h-2 rounded-full" 
                  style={{ 
                    width: `${quartileDistribution?.totalCount ? 
                      (((quartileDistribution?.q1Count || 0) + 
                        (quartileDistribution?.q2Count || 0) + 
                        (quartileDistribution?.q3Count || 0) + 
                        (quartileDistribution?.q4Count || 0)) / quartileDistribution.totalCount) * 100 
                      : 0}%` 
                  }}
                ></div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Separator className="my-8" />

      {/* Manual Triggers */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Calendar className="h-5 w-5" />
              <span>Daily Check</span>
            </CardTitle>
            <CardDescription>
              Assess fund eligibility and detect new eligible funds
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button 
              onClick={() => triggerDailyMutation.mutate()}
              disabled={triggerDailyMutation.isPending}
              className="w-full"
            >
              <Play className="h-4 w-4 mr-2" />
              {triggerDailyMutation.isPending ? 'Running...' : 'Trigger Daily Check'}
            </Button>
            <p className="text-xs text-gray-500 mt-2">
              Scheduled: Daily at 6:00 AM UTC
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <TrendingUp className="h-5 w-5" />
              <span>Weekly Recalculation</span>
            </CardTitle>
            <CardDescription>
              Recalculate performance metrics and update quartile rankings
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button 
              onClick={() => triggerWeeklyMutation.mutate()}
              disabled={triggerWeeklyMutation.isPending}
              className="w-full"
            >
              <Play className="h-4 w-4 mr-2" />
              {triggerWeeklyMutation.isPending ? 'Running...' : 'Trigger Recalculation'}
            </Button>
            <p className="text-xs text-gray-500 mt-2">
              Scheduled: Sunday at 7:00 AM UTC
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <AlertCircle className="h-5 w-5" />
              <span>Migration Tracking</span>
            </CardTitle>
            <CardDescription>
              Track quartile changes and fund migrations
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button 
              onClick={() => triggerMigrationMutation.mutate()}
              disabled={triggerMigrationMutation.isPending}
              className="w-full"
            >
              <Play className="h-4 w-4 mr-2" />
              {triggerMigrationMutation.isPending ? 'Running...' : 'Track Migrations'}
            </Button>
            <p className="text-xs text-gray-500 mt-2">
              Scheduled: 1st of month at 8:00 AM UTC
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Schedule Information */}
      <Card className="mt-8">
        <CardHeader>
          <CardTitle>Automation Schedule</CardTitle>
          <CardDescription>
            The quartile system automatically maintains itself with these scheduled tasks
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="text-center p-4 bg-blue-50 rounded-lg">
              <Clock className="h-8 w-8 mx-auto mb-2 text-blue-600" />
              <h3 className="font-semibold">Daily at 6:00 AM UTC</h3>
              <p className="text-sm text-gray-600">Fund eligibility assessment</p>
            </div>
            <div className="text-center p-4 bg-green-50 rounded-lg">
              <TrendingUp className="h-8 w-8 mx-auto mb-2 text-green-600" />
              <h3 className="font-semibold">Weekly on Sunday 7:00 AM UTC</h3>
              <p className="text-sm text-gray-600">Complete quartile recalculation</p>
            </div>
            <div className="text-center p-4 bg-purple-50 rounded-lg">
              <Calendar className="h-8 w-8 mx-auto mb-2 text-purple-600" />
              <h3 className="font-semibold">Monthly on 1st at 8:00 AM UTC</h3>
              <p className="text-sm text-gray-600">Quartile migration tracking</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}