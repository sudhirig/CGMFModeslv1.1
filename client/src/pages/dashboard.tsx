import React from "react";
import ElivateScoreCard from "@/components/dashboard/elivate-score-card";
import MarketOverview from "@/components/dashboard/market-overview";
import TopRatedFunds from "@/components/dashboard/top-rated-funds";
import ModelPortfolio from "@/components/dashboard/model-portfolio";
import EtlStatus from "@/components/dashboard/etl-status";

export default function Dashboard() {
  return (
    <div className="py-6">
      {/* Dashboard Header */}
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Dashboard Overview</h1>
          <p className="mt-1 text-sm text-neutral-500">
            A comprehensive view of the market status and fund performance
          </p>
        </div>
        
        {/* Date Range Selector */}
        <div className="flex justify-between items-center mb-6">
          <div className="flex items-center space-x-2">
            <button className="inline-flex items-center px-3 py-2 border border-neutral-300 shadow-sm text-sm leading-4 font-medium rounded-md text-neutral-700 bg-white hover:bg-neutral-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
              <span>Last 30 Days</span>
              <span className="material-icons ml-1 text-sm">arrow_drop_down</span>
            </button>
            <button className="inline-flex items-center px-3 py-2 border border-neutral-300 shadow-sm text-sm leading-4 font-medium rounded-md text-neutral-700 bg-white hover:bg-neutral-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
              <span className="material-icons text-sm">calendar_today</span>
              <span className="ml-1">Custom Range</span>
            </button>
          </div>
          <div className="flex items-center space-x-2">
            <button className="inline-flex items-center px-3 py-2 border border-primary-300 text-sm leading-4 font-medium rounded-md text-primary-700 bg-primary-50 hover:bg-primary-100 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
              <span className="material-icons text-sm">refresh</span>
              <span className="ml-1">Refresh Data</span>
            </button>
            <button className="inline-flex items-center px-3 py-2 border border-neutral-300 shadow-sm text-sm leading-4 font-medium rounded-md text-neutral-700 bg-white hover:bg-neutral-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
              <span className="material-icons text-sm">download</span>
              <span className="ml-1">Export</span>
            </button>
          </div>
        </div>
      </div>

      {/* Dashboard Components */}
      <ElivateScoreCard />
      <MarketOverview />
      <TopRatedFunds />
      <ModelPortfolio />
      <EtlStatus />
    </div>
  );
}
