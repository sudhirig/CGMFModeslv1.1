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
    // Only process authentic data - NO SYNTHETIC DATA
    if (!nifty50Data || !midcapData || !smallcapData) {
      setChartData([]); // Clear chart data if no authentic data
      return;
    }
    
    // Create normalized chart data based on timeframe using AUTHENTIC DATA ONLY
    const normalizeData = () => {
      // Check data availability
      console.log('Market Data Check:', {
        nifty50: nifty50Data?.length || 0,
        midcap: midcapData?.length || 0,  
        smallcap: smallcapData?.length || 0,
        sampleData: nifty50Data?.[0]
      });
      
      // Determine the number of data points based on timeframe
      let numPoints = Math.min(30, nifty50Data.length); // Default for daily
      if (timeframe === "weekly") numPoints = Math.min(52, nifty50Data.length);
      if (timeframe === "monthly") numPoints = Math.min(24, nifty50Data.length);
      if (timeframe === "yearly") numPoints = Math.min(5, nifty50Data.length);
      
      // Get base value for normalization from authentic data - parse as float
      const niftyBase = parseFloat(nifty50Data[nifty50Data.length - 1]?.closeValue || nifty50Data[0]?.closeValue);
      const midcapBase = parseFloat(midcapData[midcapData.length - 1]?.closeValue || midcapData[0]?.closeValue);
      const smallcapBase = parseFloat(smallcapData[smallcapData.length - 1]?.closeValue || smallcapData[0]?.closeValue);
      
      if (!niftyBase || !midcapBase || !smallcapBase) {
        console.warn('Unable to normalize authentic data - missing base values');
        return [];
      }
      
      // Create data points from authentic data
      const points = [];
      
      for (let i = 0; i < numPoints; i++) {
        const niftyPoint = nifty50Data[i];
        const midcapPoint = midcapData[i < midcapData.length ? i : midcapData.length - 1];
        const smallcapPoint = smallcapData[i < smallcapData.length ? i : smallcapData.length - 1];
        
        if (!niftyPoint || !midcapPoint || !smallcapPoint) continue;
        
        // Create normalized values (indexed to 100) from authentic data - parse as float
        const niftyNorm = (parseFloat(niftyPoint.closeValue) / niftyBase) * 100;
        const midcapNorm = (parseFloat(midcapPoint.closeValue) / midcapBase) * 100;
        const smallcapNorm = (parseFloat(smallcapPoint.closeValue) / smallcapBase) * 100;
        
        points.push({
          date: new Date(niftyPoint.indexDate).toLocaleDateString('en-US', { 
            month: 'short',
            year: timeframe === 'yearly' ? 'numeric' : undefined,
            day: timeframe === 'daily' || timeframe === 'weekly' ? 'numeric' : undefined,
          }),
          nifty50: parseFloat(niftyNorm.toFixed(2)),
          midcap: parseFloat(midcapNorm.toFixed(2)),
          smallcap: parseFloat(smallcapNorm.toFixed(2)),
          // Store raw values for tooltip
          niftyRaw: parseFloat(niftyPoint.closeValue),
          midcapRaw: parseFloat(midcapPoint.closeValue),
          smallcapRaw: parseFloat(smallcapPoint.closeValue),
        });
      }
      
      return points.reverse(); // Reverse to show chronological order
    };
    
    setChartData(normalizeData());
  }, [nifty50Data, midcapData, smallcapData, timeframe]);
  
  // NO SYNTHETIC DATA GENERATION - Authentic data only
  
  if (chartData.length === 0) {
    return (
      <div className="h-64 w-full bg-neutral-50 rounded-lg flex items-center justify-center">
        <div className="text-center">
          <span className="text-neutral-600 font-medium">Waiting for authentic market data...</span>
          <p className="text-sm text-neutral-500 mt-2">No synthetic data used - only authentic sources</p>
        </div>
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
        <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
        <XAxis 
          dataKey="date" 
          tick={{ fontSize: 12 }}
          tickLine={{ stroke: '#666' }}
        />
        <YAxis 
          domain={['dataMin - 2', 'dataMax + 2']}
          tick={{ fontSize: 12 }}
          tickLine={{ stroke: '#666' }}
          tickFormatter={(value) => `${value.toFixed(0)}`}
          label={{ value: 'Indexed Value (Base = 100)', angle: -90, position: 'insideLeft', style: { fontSize: 12 } }}
        />
        <Tooltip
          formatter={(value: number, name: string) => {
            // Show both normalized and percentage change
            const baseValue = 100;
            const percentChange = ((value - baseValue) / baseValue * 100).toFixed(2);
            const sign = value >= baseValue ? '+' : '';
            return [`${value.toFixed(2)} (${sign}${percentChange}%)`, name];
          }}
          labelFormatter={(label) => `Date: ${label}`}
          contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #ccc' }}
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
