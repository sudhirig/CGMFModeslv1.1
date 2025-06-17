/**
 * Test Realistic Returns Validation
 * Verify that top and bottom quartiles now show meaningful performance differences
 */

import axios from 'axios';

async function testRealisticReturns() {
  console.log('Testing Realistic Return Calculations...\n');
  
  const testCases = [
    {
      name: 'Top Quartile (Q1)',
      config: { quartile: "Q1", maxFunds: "5", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
      expectedRange: { min: 5, max: 25 }  // Expected 5-25% annual returns for top funds
    },
    {
      name: 'Bottom Quartile (Q4)',
      config: { quartile: "Q4", maxFunds: "5", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" },
      expectedRange: { min: -15, max: 15 }  // Expected lower/negative returns for bottom funds
    },
    {
      name: 'STRONG_BUY vs SELL',
      tests: [
        { rec: "STRONG_BUY", maxFunds: "3" },
        { rec: "SELL", maxFunds: "3" }
      ]
    }
  ];

  const results = [];

  for (const test of testCases) {
    if (test.tests) {
      // Compare recommendation types
      console.log(`Comparing Recommendations:`);
      for (const subtest of test.tests) {
        try {
          const response = await axios.post('http://localhost:5000/api/comprehensive-backtest', {
            recommendation: subtest.rec,
            maxFunds: subtest.maxFunds,
            startDate: "2024-01-01",
            endDate: "2024-12-31",
            initialAmount: "100000",
            rebalancePeriod: "quarterly"
          });
          
          const data = response.data;
          const return_pct = data.performance.totalReturn;
          const scores = data.attribution.fundContributions.map(f => parseFloat(f.elivateScore));
          
          console.log(`  ${subtest.rec}: ${return_pct.toFixed(2)}% return | Scores: ${scores.join(', ')}`);
          results.push({ type: subtest.rec, return: return_pct, scores });
          
        } catch (error) {
          console.log(`  ${subtest.rec}: ERROR - ${error.message}`);
        }
      }
    } else {
      // Test quartiles
      try {
        console.log(`Testing: ${test.name}`);
        
        const response = await axios.post('http://localhost:5000/api/comprehensive-backtest', test.config);
        const data = response.data;
        
        const return_pct = data.performance.totalReturn;
        const scores = data.attribution.fundContributions.map(f => parseFloat(f.elivateScore));
        const avgScore = scores.reduce((a, b) => a + b, 0) / scores.length;
        
        const isRealistic = return_pct >= test.expectedRange.min && return_pct <= test.expectedRange.max;
        
        console.log(`  Return: ${return_pct.toFixed(2)}% | Avg Score: ${avgScore.toFixed(1)} | Realistic: ${isRealistic ? 'YES' : 'NO'}`);
        console.log(`  Fund Scores: ${scores.join(', ')}`);
        
        results.push({
          quartile: test.name,
          return: return_pct,
          avgScore,
          realistic: isRealistic,
          scores
        });
        
      } catch (error) {
        console.log(`  ERROR: ${error.message}`);
      }
    }
    console.log('');
  }
  
  // Analysis
  console.log('ANALYSIS:');
  const q1Result = results.find(r => r.quartile?.includes('Q1'));
  const q4Result = results.find(r => r.quartile?.includes('Q4'));
  
  if (q1Result && q4Result) {
    const returnDiff = q1Result.return - q4Result.return;
    const scoreDiff = q1Result.avgScore - q4Result.avgScore;
    
    console.log(`Return Difference (Q1 vs Q4): ${returnDiff.toFixed(2)}%`);
    console.log(`Score Difference (Q1 vs Q4): ${scoreDiff.toFixed(1)} points`);
    
    if (returnDiff > 2 && scoreDiff > 10) {
      console.log('✅ GOOD: Meaningful performance differentiation between quartiles');
    } else if (Math.abs(returnDiff) < 5) {
      console.log('⚠️  CONCERN: Similar returns across different quality tiers');
    } else {
      console.log('❌ ISSUE: Unexpected return patterns');
    }
  }
  
  const strongBuy = results.find(r => r.type === 'STRONG_BUY');
  const sell = results.find(r => r.type === 'SELL');
  
  if (strongBuy && sell) {
    const recReturnDiff = strongBuy.return - sell.return;
    console.log(`Recommendation Return Difference: ${recReturnDiff.toFixed(2)}%`);
    
    if (recReturnDiff > 3) {
      console.log('✅ GOOD: STRONG_BUY outperforming SELL recommendations');
    } else {
      console.log('⚠️  CONCERN: Insufficient differentiation between recommendation tiers');
    }
  }
}

testRealisticReturns().catch(console.error);