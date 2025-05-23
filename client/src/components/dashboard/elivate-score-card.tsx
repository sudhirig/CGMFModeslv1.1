import React from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import ElivateGauge from "@/components/charts/elivate-gauge";
import { useElivate } from "@/hooks/use-elivate";
import { format } from "date-fns";

export default function ElivateScoreCard() {
  const { elivateScore, isLoading, error } = useElivate();
  
  if (isLoading) {
    return (
      <div className="px-4 sm:px-6 lg:px-8 mb-6">
        <Card>
          <CardContent className="p-6">
            <div className="flex justify-between items-center mb-4">
              <Skeleton className="h-6 w-48" />
              <Skeleton className="h-4 w-32" />
            </div>
            <div className="flex flex-col md:flex-row md:items-center">
              <div className="md:w-1/4 mb-4 md:mb-0 md:pr-6">
                <div className="flex flex-col items-center">
                  <Skeleton className="h-36 w-36 rounded-full" />
                  <Skeleton className="h-5 w-24 mt-2" />
                </div>
              </div>
              <div className="md:w-3/4">
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
                  {[...Array(6)].map((_, i) => (
                    <Skeleton key={i} className="h-20 w-full" />
                  ))}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }
  
  if (error) {
    return (
      <div className="px-4 sm:px-6 lg:px-8 mb-6">
        <Card>
          <CardContent className="p-6">
            <div className="text-red-500">Error loading ELIVATE score: {error}</div>
          </CardContent>
        </Card>
      </div>
    );
  }
  
  return (
    <div className="px-4 sm:px-6 lg:px-8 mb-6">
      <Card>
        <CardContent className="p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-neutral-900">ELIVATE Framework Score</h2>
            <div className="flex items-center">
              <span className="text-sm font-medium text-neutral-500 mr-2">
                Last Updated: {elivateScore?.scoreDate ? format(new Date(elivateScore.scoreDate), "MMMM d, yyyy") : "N/A"}
              </span>
              <button className="p-1 rounded-full text-neutral-400 hover:text-neutral-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500">
                <span className="material-icons text-sm">info</span>
              </button>
            </div>
          </div>
          
          <div className="flex flex-col md:flex-row md:items-center">
            <div className="md:w-1/4 mb-4 md:mb-0 md:pr-6">
              <div className="flex flex-col items-center">
                <ElivateGauge score={elivateScore?.totalElivateScore || 0} />
                <div className="mt-2 text-center">
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    elivateScore?.marketStance === "BULLISH" 
                      ? "bg-green-100 text-green-800" 
                      : elivateScore?.marketStance === "BEARISH" 
                      ? "bg-red-100 text-red-800" 
                      : "bg-yellow-100 text-yellow-800"
                  }`}>
                    <span className="material-icons text-xs mr-1">
                      {elivateScore?.marketStance === "BULLISH" 
                        ? "arrow_upward" 
                        : elivateScore?.marketStance === "BEARISH" 
                        ? "arrow_downward" 
                        : "trending_flat"}
                    </span>
                    {elivateScore?.marketStance || "Neutral"} Stance
                  </span>
                </div>
              </div>
            </div>
            
            <div className="md:w-3/4">
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
                {/* ELIVATE Component Scores */}
                <div className="bg-neutral-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-neutral-500 uppercase">External Influence</div>
                  <div className="mt-1 flex justify-between items-center">
                    <div className="text-xl font-semibold text-neutral-900">{elivateScore?.externalInfluenceScore || 0}</div>
                    <div className="text-xs text-neutral-500">/ 20</div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                    <div 
                      className="bg-primary-500 h-2 rounded-full" 
                      style={{ width: `${((elivateScore?.externalInfluenceScore || 0) / 20) * 100}%` }}
                    ></div>
                  </div>
                </div>
                
                <div className="bg-neutral-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-neutral-500 uppercase">Local Story</div>
                  <div className="mt-1 flex justify-between items-center">
                    <div className="text-xl font-semibold text-neutral-900">{elivateScore?.localStoryScore || 0}</div>
                    <div className="text-xs text-neutral-500">/ 20</div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                    <div 
                      className="bg-primary-500 h-2 rounded-full" 
                      style={{ width: `${((elivateScore?.localStoryScore || 0) / 20) * 100}%` }}
                    ></div>
                  </div>
                </div>
                
                <div className="bg-neutral-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-neutral-500 uppercase">Inflation & Rates</div>
                  <div className="mt-1 flex justify-between items-center">
                    <div className="text-xl font-semibold text-neutral-900">{elivateScore?.inflationRatesScore || 0}</div>
                    <div className="text-xs text-neutral-500">/ 20</div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                    <div 
                      className="bg-primary-500 h-2 rounded-full" 
                      style={{ width: `${((elivateScore?.inflationRatesScore || 0) / 20) * 100}%` }}
                    ></div>
                  </div>
                </div>
                
                <div className="bg-neutral-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-neutral-500 uppercase">Valuation & Earnings</div>
                  <div className="mt-1 flex justify-between items-center">
                    <div className="text-xl font-semibold text-neutral-900">{elivateScore?.valuationEarningsScore || 0}</div>
                    <div className="text-xs text-neutral-500">/ 20</div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                    <div 
                      className="bg-primary-500 h-2 rounded-full" 
                      style={{ width: `${((elivateScore?.valuationEarningsScore || 0) / 20) * 100}%` }}
                    ></div>
                  </div>
                </div>
                
                <div className="bg-neutral-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-neutral-500 uppercase">Allocation of Capital</div>
                  <div className="mt-1 flex justify-between items-center">
                    <div className="text-xl font-semibold text-neutral-900">{elivateScore?.allocationCapitalScore || 0}</div>
                    <div className="text-xs text-neutral-500">/ 10</div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                    <div 
                      className="bg-primary-500 h-2 rounded-full" 
                      style={{ width: `${((elivateScore?.allocationCapitalScore || 0) / 10) * 100}%` }}
                    ></div>
                  </div>
                </div>
                
                <div className="bg-neutral-50 rounded-lg p-3">
                  <div className="text-xs font-medium text-neutral-500 uppercase">Trends & Sentiments</div>
                  <div className="mt-1 flex justify-between items-center">
                    <div className="text-xl font-semibold text-neutral-900">{elivateScore?.trendsSentimentsScore || 0}</div>
                    <div className="text-xs text-neutral-500">/ 10</div>
                  </div>
                  <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                    <div 
                      className="bg-primary-500 h-2 rounded-full" 
                      style={{ width: `${((elivateScore?.trendsSentimentsScore || 0) / 10) * 100}%` }}
                    ></div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
