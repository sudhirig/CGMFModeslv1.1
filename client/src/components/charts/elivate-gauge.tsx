import React from "react";

interface ElivateGaugeProps {
  score: number;
}

export default function ElivateGauge({ score }: ElivateGaugeProps) {
  // Calculate position for score
  const scorePercent = Math.min(100, Math.max(0, score));
  const dashArray = 100; // Circumference
  const dashOffset = dashArray - (dashArray * scorePercent) / 100;
  
  return (
    <div className="relative">
      <svg className="w-36 h-36" viewBox="0 0 36 36">
        {/* Gray background circle */}
        <circle 
          cx="18" 
          cy="18" 
          r="16" 
          fill="none" 
          stroke="#e2e8f0" 
          strokeWidth="2"
        />
        
        {/* Progress circle */}
        <circle 
          cx="18" 
          cy="18" 
          r="16" 
          fill="none" 
          stroke={getColorForScore(score)}
          strokeWidth="2" 
          strokeDasharray={`${dashArray}`} 
          strokeDashoffset={dashOffset} 
          transform="rotate(-90 18 18)"
        />
        
        {/* Score text (small) */}
        <text 
          x="18" 
          y="18" 
          textAnchor="middle" 
          dominantBaseline="central" 
          fontSize="0.6rem" 
          fill="#0f172a"
        >
          {score}/100
        </text>
      </svg>
      
      {/* Larger score display in the center */}
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="text-center">
          <div className="text-3xl font-bold text-neutral-900">{Math.round(score)}</div>
          <div className="text-sm text-neutral-500">ELIVATE Score</div>
        </div>
      </div>
    </div>
  );
}

// Helper function to get color based on score
function getColorForScore(score: number): string {
  if (score >= 75) return "#34D399"; // Green for bullish
  if (score >= 50) return "#FBBF24"; // Yellow for neutral
  return "#F87171"; // Red for bearish
}
