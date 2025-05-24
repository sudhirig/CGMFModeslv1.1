import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@/components/ui/card';
import { apiRequest } from '@/lib/queryClient';
import { useQuery } from '@tanstack/react-query';
import { Loader2 } from 'lucide-react';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';

export default function HistoricalDataImport() {
  const [isImporting, setIsImporting] = useState(false);
  const [importStatus, setImportStatus] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const { data: navStatus, isLoading: isLoadingStatus } = useQuery({
    queryKey: ['/api/amfi/status'],
    refetchInterval: 5000, // Refresh every 5 seconds
  });

  const startHistoricalImport = async () => {
    setIsImporting(true);
    setError(null);
    
    try {
      const response = await apiRequest('/api/historical-restart/start', {
        method: 'POST',
      });
      
      setImportStatus(response);
      console.log('Import started:', response);
    } catch (err) {
      console.error('Error starting historical import:', err);
      setError('Failed to start historical import. Please try again.');
    } finally {
      setIsImporting(false);
    }
  };

  const checkImportStatus = async () => {
    try {
      const response = await apiRequest('/api/historical-restart/status', {
        method: 'GET',
      });
      
      setImportStatus(response);
      console.log('Import status:', response);
    } catch (err) {
      console.error('Error checking status:', err);
      setError('Failed to check import status. Please try again.');
    }
  };

  return (
    <div className="container mx-auto py-10">
      <h1 className="text-3xl font-bold mb-6">Historical NAV Data Import</h1>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Current NAV Data Status</CardTitle>
            <CardDescription>
              Overview of the current NAV data in the system
            </CardDescription>
          </CardHeader>
          <CardContent>
            {isLoadingStatus ? (
              <div className="flex items-center justify-center p-6">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
              </div>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-2">
                  <div className="font-medium">Total Funds:</div>
                  <div>{navStatus?.fundCount || 0}</div>
                  
                  <div className="font-medium">Funds with NAV:</div>
                  <div>{navStatus?.navCount || 0}</div>
                  
                  <div className="font-medium">Earliest NAV Date:</div>
                  <div>{navStatus?.earliestNavDate || 'N/A'}</div>
                  
                  <div className="font-medium">Latest NAV Date:</div>
                  <div>{navStatus?.latestNavDate || 'N/A'}</div>
                </div>
              </div>
            )}
          </CardContent>
          <CardFooter>
            <p className="text-sm text-muted-foreground">
              {navStatus?.earliestNavDate === navStatus?.latestNavDate 
                ? 'No historical data available. Only single-day NAV data is present.' 
                : 'Historical NAV data is available for analysis.'}
            </p>
          </CardFooter>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle>Import Historical NAV Data</CardTitle>
            <CardDescription>
              Fetch real historical NAV data from AMFI for the past 36 months
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <p>
                This will import authentic historical NAV data for all funds, enabling:
              </p>
              <ul className="list-disc pl-6 space-y-1">
                <li>Accurate performance metrics calculation</li>
                <li>Proper quartile scoring based on real data</li>
                <li>Better investment recommendations</li>
              </ul>
              
              {error && (
                <Alert variant="destructive" className="mt-4">
                  <AlertTitle>Error</AlertTitle>
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              )}
              
              {importStatus && (
                <Alert className="mt-4">
                  <AlertTitle>Import Status</AlertTitle>
                  <AlertDescription>
                    <div className="mt-2">
                      <p>{importStatus.message}</p>
                      {importStatus.fundsToProcess && (
                        <p className="mt-1">Funds to process: {importStatus.fundsToProcess}</p>
                      )}
                      {importStatus.etlRunId && (
                        <p className="mt-1">ETL Run ID: {importStatus.etlRunId}</p>
                      )}
                    </div>
                  </AlertDescription>
                </Alert>
              )}
            </div>
          </CardContent>
          <CardFooter className="flex flex-col sm:flex-row gap-2">
            <Button 
              onClick={startHistoricalImport} 
              disabled={isImporting}
              className="w-full sm:w-auto"
            >
              {isImporting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Starting Import...
                </>
              ) : 'Start Historical Import'}
            </Button>
            
            <Button 
              onClick={checkImportStatus} 
              variant="outline"
              className="w-full sm:w-auto"
            >
              Check Status
            </Button>
          </CardFooter>
        </Card>
      </div>
    </div>
  );
}