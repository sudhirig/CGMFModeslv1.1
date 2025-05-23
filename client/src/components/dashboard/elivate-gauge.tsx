import React from 'react';

interface ElivateGaugeProps {
  score: number;
  size?: number;
}

export default function ElivateGauge({ score, size = 120 }: ElivateGaugeProps) {
  // Calculate the angle based on the score (0-100)
  const angle = (score / 100) * 180;
  
  // Determine color based on score
  const getColor = () => {
    if (score < 30) return "#ef4444"; // Red
    if (score < 50) return "#f97316"; // Orange
    if (score < 70) return "#facc15"; // Yellow
    return "#22c55e"; // Green
  };
  
  // Calculate SVG arc path
  const generateArc = (radius: number, startAngle: number, endAngle: number) => {
    const start = polarToCartesian(radius, endAngle);
    const end = polarToCartesian(radius, startAngle);
    const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
    return [
      "M", start.x, start.y,
      "A", radius, radius, 0, largeArcFlag, 0, end.x, end.y
    ].join(" ");
  };
  
  // Convert polar coordinates to cartesian
  const polarToCartesian = (radius: number, angleInDegrees: number) => {
    const angleInRadians = (angleInDegrees - 90) * Math.PI / 180;
    return {
      x: radius + (radius * Math.cos(angleInRadians)),
      y: radius + (radius * Math.sin(angleInRadians))
    };
  };
  
  // Calculate dimensions
  const radius = size / 2;
  const strokeWidth = size * 0.09;
  const innerRadius = radius - strokeWidth / 2;
  
  return (
    <div className="relative" style={{ width: size, height: size / 2 }}>
      <svg width={size} height={size / 2} viewBox={`0 0 ${size} ${size / 2}`}>
        {/* Background arc */}
        <path
          d={generateArc(innerRadius, 0, 180)}
          fill="none"
          stroke="#e5e7eb"
          strokeWidth={strokeWidth}
          strokeLinecap="round"
        />
        
        {/* Foreground arc */}
        <path
          d={generateArc(innerRadius, 0, angle)}
          fill="none"
          stroke={getColor()}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
        />
        
        {/* Marker */}
        <circle
          cx={polarToCartesian(innerRadius, angle).x}
          cy={polarToCartesian(innerRadius, angle).y}
          r={strokeWidth * 0.8}
          fill="white"
          stroke={getColor()}
          strokeWidth="2"
        />
      </svg>
      
      {/* Score text */}
      <div className="absolute inset-0 flex flex-col items-center justify-end pb-2">
        <div className="text-2xl font-bold" style={{ color: getColor() }}>{score}</div>
        <div className="text-xs text-neutral-500">/ 100</div>
      </div>
    </div>
  );
}