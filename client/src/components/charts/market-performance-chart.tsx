import React, { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  Legend
} from "recharts";

interface MarketPerformanceChartProps {
  timeframe: string;
}

export default function MarketPerformanceChart({ timeframe }: MarketPerformanceChartProps) {
  const [chartData, setChartData] = useState<any[]>([]);
  
  // Fetch Nifty 50 data
  const { data: nifty50Data } = useQuery({
    queryKey: ["/api/market/index/NIFTY 50"],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
  
  // Fetch Nifty Midcap 100 data
  const { data: midcapData } = useQuery({
    queryKey: ["/api/market/index/NIFTY MIDCAP 100"],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
  
  // Fetch Nifty Smallcap 100 data
  const { data: smallcapData } = useQuery({
    queryKey: ["/api/market/index/NIFTY SMALLCAP 100"],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
  
  useEffect(() => {
    if (!nifty50Data && !midcapData && !smallcapData) return;
    
    // Create normalized chart data based on timeframe
    const normalizeData = () => {
      // If we don't have real data yet, generate demo data
      if (!nifty50Data || !midcapData || !smallcapData) {
        return generateDemoData();
      }
      
      // Determine the number of data points based on timeframe
      let numPoints = 30; // Default for daily
      if (timeframe === "weekly") numPoints = 52;
      if (timeframe === "monthly") numPoints = 24;
      if (timeframe === "yearly") numPoints = 5;
      
      // Get base value for normalization
      const niftyBase = nifty50Data[0]?.closeValue || 100;
      const midcapBase = midcapData[0]?.closeValue || 100;
      const smallcapBase = smallcapData[0]?.closeValue || 100;
      
      // Create data points
      const points = [];
      
      for (let i = 0; i < Math.min(numPoints, nifty50Data.length); i++) {
        const niftyPoint = nifty50Data[i];
        const midcapPoint = midcapData[i < midcapData.length ? i : midcapData.length - 1];
        const smallcapPoint = smallcapData[i < smallcapData.length ? i : smallcapData.length - 1];
        
        // Create normalized values (indexed to 100)
        const niftyNorm = (niftyPoint.closeValue / niftyBase) * 100;
        const midcapNorm = (midcapPoint.closeValue / midcapBase) * 100;
        const smallcapNorm = (smallcapPoint.closeValue / smallcapBase) * 100;
        
        points.push({
          date: new Date(niftyPoint.indexDate).toLocaleDateString('en-US', { 
            month: 'short',
            year: timeframe === 'yearly' ? 'numeric' : undefined,
            day: timeframe === 'daily' ? 'numeric' : undefined,
          }),
          nifty50: niftyNorm,
          midcap: midcapNorm,
          smallcap: smallcapNorm,
        });
      }
      
      return points.reverse(); // Reverse to show chronological order
    };
    
    setChartData(normalizeData());
  }, [nifty50Data, midcapData, smallcapData, timeframe]);
  
  // Generate demo data if actual data is not available
  const generateDemoData = () => {
    const demoData = [];
    const periods = timeframe === 'daily' ? 30 : 
                  timeframe === 'weekly' ? 52 : 
                  timeframe === 'monthly' ? 12 : 5;
    
    let nifty = 100;
    let midcap = 100;
    let smallcap = 100;
    
    for (let i = 0; i < periods; i++) {
      // Generate a date label based on timeframe
      let date;
      const today = new Date();
      
      if (timeframe === 'daily') {
        date = new Date(today);
        date.setDate(today.getDate() - (periods - i - 1));
        date = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      } else if (timeframe === 'weekly') {
        date = new Date(today);
        date.setDate(today.getDate() - (periods - i - 1) * 7);
        date = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      } else if (timeframe === 'monthly') {
        date = new Date(today);
        date.setMonth(today.getMonth() - (periods - i - 1));
        date = date.toLocaleDateString('en-US', { month: 'short' });
      } else {
        date = new Date(today);
        date.setFullYear(today.getFullYear() - (periods - i - 1));
        date = date.toLocaleDateString('en-US', { year: 'numeric' });
      }
      
      // Apply some randomness but maintain a trend
      const trendFactor = (i / periods) * 30; // Higher values towards the end
      
      nifty = nifty * (1 + (Math.random() * 0.04 - 0.015 + 0.001 * trendFactor));
      midcap = midcap * (1 + (Math.random() * 0.05 - 0.02 + 0.0015 * trendFactor));
      smallcap = smallcap * (1 + (Math.random() * 0.06 - 0.025 + 0.002 * trendFactor));
      
      demoData.push({
        date,
        nifty50: parseFloat(nifty.toFixed(2)),
        midcap: parseFloat(midcap.toFixed(2)),
        smallcap: parseFloat(smallcap.toFixed(2)),
      });
    }
    
    return demoData;
  };
  
  if (chartData.length === 0) {
    return (
      <div className="h-64 w-full bg-neutral-50 rounded-lg flex items-center justify-center">
        <span className="text-neutral-400">Loading market data...</span>
      </div>
    );
  }
  
  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart
        data={chartData}
        margin={{
          top: 5,
          right: 30,
          left: 20,
          bottom: 5,
        }}
      >
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis />
        <Tooltip
          formatter={(value: number) => [`${value.toFixed(2)}`, '']}
          labelFormatter={(label) => `Date: ${label}`}
        />
        <Legend />
        <Line 
          type="monotone" 
          dataKey="nifty50" 
          stroke="#2271fa" 
          activeDot={{ r: 8 }} 
          name="NIFTY 50"
          strokeWidth={2}
        />
        <Line 
          type="monotone" 
          dataKey="midcap" 
          stroke="#68aff4" 
          name="NIFTY MID CAP 100"
          strokeWidth={2}
        />
        <Line 
          type="monotone" 
          dataKey="smallcap" 
          stroke="#34D399" 
          name="NIFTY SMALL CAP 100"
          strokeWidth={2}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
