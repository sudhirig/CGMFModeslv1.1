import React from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { TrendingUp, TrendingDown, Minus, Info, ExternalLink, Star } from "lucide-react";
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
      <Card className="bg-gradient-to-r from-blue-50 to-indigo-50 border-0 shadow-lg">
        <CardContent className="p-6">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-6">
            <div className="flex items-center space-x-3">
              <div className="p-2 bg-blue-100 rounded-lg">
                <Star className="w-6 h-6 text-blue-600" />
              </div>
              <div>
                <h2 className="text-2xl font-bold text-gray-900">ELIVATE Framework Score</h2>
                <p className="text-sm text-gray-600">Market intelligence and fund scoring methodology</p>
              </div>
            </div>
            <div className="flex items-center space-x-3 mt-4 sm:mt-0">
              <Badge variant="outline" className="text-blue-600 border-blue-200">
                <span className="text-xs font-medium">
                  Last Updated: {elivateScore?.scoreDate ? format(new Date(elivateScore.scoreDate), "MMM d, yyyy") : "N/A"}
                </span>
              </Badge>
              <Button variant="ghost" size="sm" className="text-blue-600 hover:text-blue-800">
                <Info className="w-4 h-4 mr-2" />
                About ELIVATE
              </Button>
              <Button variant="ghost" size="sm" className="text-blue-600 hover:text-blue-800">
                <ExternalLink className="w-4 h-4 mr-2" />
                View Details
              </Button>
            </div>
          </div>
          
          <div className="flex flex-col lg:flex-row lg:items-center">
            <div className="lg:w-1/4 mb-6 lg:mb-0 lg:pr-8">
              <div className="flex flex-col items-center">
                <div className="relative">
                  <ElivateGauge score={elivateScore?.totalElivateScore || 0} />
                  <div className="absolute -top-2 -right-2">
                    <div className="bg-white rounded-full p-1 shadow-sm">
                      <div className="w-4 h-4 bg-green-500 rounded-full animate-pulse"></div>
                    </div>
                  </div>
                </div>
                <div className="mt-4 text-center">
                  <Badge 
                    variant="secondary" 
                    className={`px-3 py-1 text-sm font-medium ${
                      elivateScore?.marketStance === "BULLISH" 
                        ? "bg-green-100 text-green-800 border-green-200" 
                        : elivateScore?.marketStance === "BEARISH" 
                        ? "bg-red-100 text-red-800 border-red-200" 
                        : "bg-yellow-100 text-yellow-800 border-yellow-200"
                    }`}>
                    {elivateScore?.marketStance === "BULLISH" && <TrendingUp className="w-4 h-4 mr-1" />}
                    {elivateScore?.marketStance === "BEARISH" && <TrendingDown className="w-4 h-4 mr-1" />}
                    {elivateScore?.marketStance === "NEUTRAL" && <Minus className="w-4 h-4 mr-1" />}
                    {elivateScore?.marketStance || "Neutral"} Stance
                  </Badge>
                </div>
              </div>
            </div>
            
            <div className="lg:w-3/4">
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {/* External Influence */}
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-sm font-medium text-gray-600">External Influence</h3>
                      <Badge variant="outline" className="text-xs">Global</Badge>
                    </div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-2xl font-bold text-gray-900">{elivateScore?.externalInfluenceScore || 0}</div>
                      <div className="text-sm text-gray-500">/ 20</div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-blue-500 to-blue-600 h-2 rounded-full transition-all duration-500" 
                        style={{ width: `${((elivateScore?.externalInfluenceScore || 0) / 20) * 100}%` }}
                      ></div>
                    </div>
                  </CardContent>
                </Card>
                
                {/* Local Story */}
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-sm font-medium text-gray-600">Local Story</h3>
                      <Badge variant="outline" className="text-xs">India</Badge>
                    </div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-2xl font-bold text-gray-900">{elivateScore?.localStoryScore || 0}</div>
                      <div className="text-sm text-gray-500">/ 20</div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-green-500 to-green-600 h-2 rounded-full transition-all duration-500" 
                        style={{ width: `${((elivateScore?.localStoryScore || 0) / 20) * 100}%` }}
                      ></div>
                    </div>
                  </CardContent>
                </Card>
                
                {/* Inflation & Rates */}
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-sm font-medium text-gray-600">Inflation & Rates</h3>
                      <Badge variant="outline" className="text-xs">RBI</Badge>
                    </div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-2xl font-bold text-gray-900">{elivateScore?.inflationRatesScore || 0}</div>
                      <div className="text-sm text-gray-500">/ 20</div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-purple-500 to-purple-600 h-2 rounded-full transition-all duration-500" 
                        style={{ width: `${((elivateScore?.inflationRatesScore || 0) / 20) * 100}%` }}
                      ></div>
                    </div>
                  </CardContent>
                </Card>
                
                {/* Valuation & Earnings */}
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-sm font-medium text-gray-600">Valuation & Earnings</h3>
                      <Badge variant="outline" className="text-xs">Market</Badge>
                    </div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-2xl font-bold text-gray-900">{elivateScore?.valuationEarningsScore || 0}</div>
                      <div className="text-sm text-gray-500">/ 20</div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-orange-500 to-orange-600 h-2 rounded-full transition-all duration-500" 
                        style={{ width: `${((elivateScore?.valuationEarningsScore || 0) / 20) * 100}%` }}
                      ></div>
                    </div>
                  </CardContent>
                </Card>
                
                {/* Allocation of Capital */}
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-sm font-medium text-gray-600">Allocation of Capital</h3>
                      <Badge variant="outline" className="text-xs">Flows</Badge>
                    </div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-2xl font-bold text-gray-900">{elivateScore?.allocationCapitalScore || 0}</div>
                      <div className="text-sm text-gray-500">/ 10</div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-teal-500 to-teal-600 h-2 rounded-full transition-all duration-500" 
                        style={{ width: `${((elivateScore?.allocationCapitalScore || 0) / 10) * 100}%` }}
                      ></div>
                    </div>
                  </CardContent>
                </Card>
                
                {/* Trends & Sentiments */}
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-shadow">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="text-sm font-medium text-gray-600">Trends & Sentiments</h3>
                      <Badge variant="outline" className="text-xs">VIX</Badge>
                    </div>
                    <div className="flex items-center justify-between mb-3">
                      <div className="text-2xl font-bold text-gray-900">{elivateScore?.trendsSentimentsScore || 0}</div>
                      <div className="text-sm text-gray-500">/ 10</div>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-gradient-to-r from-red-500 to-red-600 h-2 rounded-full transition-all duration-500" 
                        style={{ width: `${((elivateScore?.trendsSentimentsScore || 0) / 10) * 100}%` }}
                      ></div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
