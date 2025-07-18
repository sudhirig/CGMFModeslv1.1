import { Switch, Route } from "wouter";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/not-found";
import Dashboard from "@/pages/dashboard";
import FundAnalysis from "@/pages/fund-analysis";
import ProductionFundSearch from "@/pages/production-fund-search";
import PortfolioBuilder from "@/pages/portfolio-builder";
import EtlPipeline from "@/pages/etl-pipeline";
import Backtesting from "@/pages/backtesting";
import DatabaseExplorer from "@/pages/database-explorer";
import ElivateFramework from "@/pages/elivate-framework";
import QuartileAnalysis from "@/pages/quartile-analysis";
import HistoricalDataImport from "@/pages/historical-data-import";
import NavPartitioning from "@/pages/nav-partitioning";
import DataImportStatus from "@/pages/data-import-status";
import AutomationDashboard from "@/pages/automation-dashboard";
import HistoricalImportDashboard from "@/pages/historical-import-dashboard";
import ValidationDashboard from "@/pages/validation-dashboard";
import AdvancedAnalyticsPage from "@/pages/AdvancedAnalyticsPage";
import BenchmarkManager from "@/pages/benchmark-manager";
import BenchmarkRollingReturns from "@/pages/benchmark-rolling-returns";
import Sidebar from "@/components/layout/sidebar";
import Header from "@/components/layout/header";

function Router() {
  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <div className="flex flex-col flex-1 w-0 overflow-hidden">
        <Header />
        <main className="flex-1 relative z-0 overflow-y-auto focus:outline-none">
          <Switch>
            <Route path="/" component={Dashboard} />
            <Route path="/fund-search" component={ProductionFundSearch} />
            <Route path="/fund-analysis" component={FundAnalysis} />
            <Route path="/portfolio-builder" component={PortfolioBuilder} />
            <Route path="/etl-pipeline" component={EtlPipeline} />
            <Route path="/backtesting" component={Backtesting} />
            <Route path="/database-explorer" component={DatabaseExplorer} />
            <Route path="/elivate-framework" component={ElivateFramework} />
            <Route path="/quartile-analysis" component={QuartileAnalysis} />
            <Route path="/historical-data-import" component={HistoricalDataImport} />
            <Route path="/nav-partitioning" component={NavPartitioning} />
            <Route path="/data-import-status" component={DataImportStatus} />
            <Route path="/automation-dashboard" component={AutomationDashboard} />
            <Route path="/historical-import-dashboard" component={HistoricalImportDashboard} />
            <Route path="/validation-dashboard" component={ValidationDashboard} />
            <Route path="/validation" component={ValidationDashboard} />
            <Route path="/advanced-analytics" component={AdvancedAnalyticsPage} />
            <Route path="/benchmark-manager" component={BenchmarkManager} />
            <Route path="/benchmark-rolling-returns" component={BenchmarkRollingReturns} />
            <Route component={NotFound} />
          </Switch>
        </main>
      </div>
    </div>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router />
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
