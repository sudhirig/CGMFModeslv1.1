import React, { useState } from "react";
import { Link, useLocation } from "wouter";

export default function Sidebar() {
  const [location] = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);
  
  const isActive = (path: string) => {
    return location === path;
  };
  
  const linkClass = (path: string) => {
    return `flex items-center px-2 py-2 mt-1 text-sm font-medium rounded ${
      isActive(path)
        ? "text-primary-600 bg-primary-50"
        : "text-neutral-600 hover:bg-neutral-100"
    }`;
  };
  
  return (
    <div className={`${mobileOpen ? 'block' : 'hidden'} md:flex md:flex-shrink-0`}>
      <div className="flex flex-col w-64 bg-white border-r border-neutral-200">
        <div className="flex items-center justify-center h-16 px-4 border-b border-neutral-200">
          <h1 className="text-xl font-semibold text-neutral-900">Capitalglobal</h1>
        </div>
        <div className="flex flex-col flex-grow overflow-y-auto">
          <div className="flex flex-col flex-grow px-4 py-4">
            <div className="mb-6">
              <div className="text-xs font-semibold text-neutral-400 uppercase tracking-wider">
                Dashboard
              </div>
              <Link href="/" className={linkClass("/")}>
                <span className="material-icons text-xl mr-3">dashboard</span>
                <span>Overview</span>
              </Link>
              <Link href="/elivate-framework" className={linkClass("/elivate-framework")}>
                <span className="material-icons text-xl mr-3">trending_up</span>
                <span>ELIVATE Framework</span>
              </Link>
              <Link href="/fund-search" className={linkClass("/fund-search")}>
                <span className="material-icons text-xl mr-3">search</span>
                <span>Fund Search</span>
              </Link>
              <Link href="/fund-analysis" className={linkClass("/fund-analysis")}>
                <span className="material-icons text-xl mr-3">assessment</span>
                <span>Fund Analysis</span>
              </Link>
              <Link href="/quartile-analysis" className={linkClass("/quartile-analysis")}>
                <span className="material-icons text-xl mr-3">pie_chart</span>
                <span>Quartile Analysis</span>
              </Link>
              <Link href="/advanced-analytics" className={linkClass("/advanced-analytics")}>
                <span className="material-icons text-xl mr-3">science</span>
                <span>Advanced Analytics</span>
              </Link>
              <Link href="/portfolio-builder" className={linkClass("/portfolio-builder")}>
                <span className="material-icons text-xl mr-3">account_balance</span>
                <span>Portfolio Builder</span>
              </Link>
            </div>
            <div className="mb-6">
              <div className="text-xs font-semibold text-neutral-400 uppercase tracking-wider">
                Data Management
              </div>
              <Link href="/etl-pipeline" className={linkClass("/etl-pipeline")}>
                <span className="material-icons text-xl mr-3">sync</span>
                <span>ETL Pipeline</span>
              </Link>
              <Link href="/data-import-status" className={linkClass("/data-import-status")}>
                <span className="material-icons text-xl mr-3">analytics</span>
                <span>Data Import Status</span>
              </Link>
              <Link href="/historical-data-import" className={linkClass("/historical-data-import")}>
                <span className="material-icons text-xl mr-3">update</span>
                <span>Historical NAV Import</span>
              </Link>
              <Link href="/historical-import-dashboard" className={linkClass("/historical-import-dashboard")}>
                <span className="material-icons text-xl mr-3">cloud_sync</span>
                <span>Background Import</span>
              </Link>
              <Link href="/mfapi-test" className={linkClass("/mfapi-test")}>
                <span className="material-icons text-xl mr-3">api</span>
                <span>MFAPI.in Test</span>
              </Link>
              <Link href="/mftool-test" className={linkClass("/mftool-test")}>
                <span className="material-icons text-xl mr-3">build</span>
                <span>MFTool Test</span>
              </Link>
              <Link href="/database-explorer" className={linkClass("/database-explorer")}>
                <span className="material-icons text-xl mr-3">storage</span>
                <span>Database Explorer</span>
              </Link>
              <Link href="/benchmark-manager" className={linkClass("/benchmark-manager")}>
                <span className="material-icons text-xl mr-3">show_chart</span>
                <span>Benchmark Manager</span>
              </Link>
              <Link href="/benchmark-rolling-returns" className={linkClass("/benchmark-rolling-returns")}>
                <span className="material-icons text-xl mr-3">trending_up</span>
                <span>Benchmark Rolling Returns</span>
              </Link>
              <Link href="/nav-partitioning" className={linkClass("/nav-partitioning")}>
                <span className="material-icons text-xl mr-3">table_chart</span>
                <span>NAV Partitioning</span>
              </Link>
              <Link href="/backtesting" className={linkClass("/backtesting")}>
                <span className="material-icons text-xl mr-3">history</span>
                <span>Backtesting</span>
              </Link>
              <Link href="/validation-dashboard" className={linkClass("/validation-dashboard")}>
                <span className="material-icons text-xl mr-3">verified</span>
                <span>Validation Dashboard</span>
              </Link>
            </div>
            <div className="mb-6">
              <div className="text-xs font-semibold text-neutral-400 uppercase tracking-wider">
                Administration
              </div>
              <Link href="/settings" className={linkClass("/settings")}>
                <span className="material-icons text-xl mr-3">settings</span>
                <span>Settings</span>
              </Link>
              <Link href="/user-management" className={linkClass("/user-management")}>
                <span className="material-icons text-xl mr-3">verified_user</span>
                <span>User Management</span>
              </Link>
              <Link href="/system-health" className={linkClass("/system-health")}>
                <span className="material-icons text-xl mr-3">monitor_heart</span>
                <span>System Health</span>
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
