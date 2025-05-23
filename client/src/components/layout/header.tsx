import React from "react";
import { useLocation } from "wouter";

export default function Header() {
  const [location] = useLocation();
  
  // Get the title based on current route
  const getTitle = () => {
    switch (location) {
      case "/":
        return "MF Selection Model";
      case "/fund-analysis":
        return "Fund Analysis";
      case "/portfolio-builder":
        return "Portfolio Builder";
      case "/etl-pipeline":
        return "ETL Pipeline";
      default:
        return "Spark Capital";
    }
  };
  
  // Get the breadcrumb path
  const getBreadcrumb = () => {
    const parts = location.split("/").filter(Boolean);
    if (parts.length === 0) return "Overview";
    return parts.map(part => part.split("-").map(word => 
      word.charAt(0).toUpperCase() + word.slice(1)
    ).join(" ")).join(" / ");
  };
  
  return (
    <div className="relative z-10 flex flex-shrink-0 h-16 bg-white shadow">
      <button 
        className="px-4 border-r border-neutral-200 text-neutral-500 focus:outline-none focus:bg-neutral-100 focus:text-neutral-600 md:hidden"
        onClick={() => {
          // This would toggle the sidebar in a full implementation
          const sidebar = document.querySelector('.md\\:flex-shrink-0');
          if (sidebar) {
            sidebar.classList.toggle('hidden');
            sidebar.classList.toggle('md:flex');
          }
        }}
      >
        <span className="material-icons">menu</span>
      </button>
      <div className="flex justify-between items-center w-full px-4">
        <div className="flex items-center">
          <div className="flex-shrink-0 mr-2">
            <h2 className="text-lg font-semibold text-neutral-900">{getTitle()}</h2>
          </div>
          {/* Breadcrumbs */}
          <div className="hidden md:flex ml-4 text-sm text-neutral-500">
            <span>Dashboard</span>
            <span className="mx-2">/</span>
            <span className="text-neutral-900">{getBreadcrumb()}</span>
          </div>
        </div>
        <div className="flex items-center">
          <button className="p-1 text-neutral-400 rounded-full hover:text-neutral-500 focus:outline-none focus:shadow-outline">
            <span className="material-icons">notifications</span>
          </button>
          <button className="p-1 ml-3 text-neutral-400 rounded-full hover:text-neutral-500 focus:outline-none focus:shadow-outline">
            <span className="material-icons">help_outline</span>
          </button>
          <div className="ml-3 relative">
            <div className="flex items-center">
              <div className="h-8 w-8 rounded-full bg-primary-500 flex items-center justify-center text-white">
                SC
              </div>
              <span className="ml-2 text-sm font-medium text-neutral-700 hidden lg:block">Analyst</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
