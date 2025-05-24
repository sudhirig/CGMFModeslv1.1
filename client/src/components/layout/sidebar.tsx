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
          <h1 className="text-xl font-semibold text-neutral-900">Spark Capital</h1>
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
              <Link href="/fund-analysis" className={linkClass("/fund-analysis")}>
                <span className="material-icons text-xl mr-3">assessment</span>
                <span>Fund Analysis</span>
              </Link>
              <Link href="/quartile-analysis" className={linkClass("/quartile-analysis")}>
                <span className="material-icons text-xl mr-3">pie_chart</span>
                <span>Quartile Analysis</span>
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
              <Link href="/database-explorer" className={linkClass("/database-explorer")}>
                <span className="material-icons text-xl mr-3">storage</span>
                <span>Database Explorer</span>
              </Link>
              <Link href="/backtesting" className={linkClass("/backtesting")}>
                <span className="material-icons text-xl mr-3">history</span>
                <span>Backtesting</span>
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
