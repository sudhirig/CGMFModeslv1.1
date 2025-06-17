/**
 * Validate Backtesting Data Diversity
 * Ensures each backtesting type selects from different fund pools
 */

import axios from 'axios';

async function validateBacktestingDiversity() {
  console.log('🔍 Validating Backtesting Data Diversity...\n');
  
  const testCases = [
    {
      name: 'Q1 Quartile (Top 25%)',
      config: { quartile: "Q1", maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    },
    {
      name: 'Q3 Quartile (3rd 25%)',
      config: { quartile: "Q3", maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    },
    {
      name: 'Q4 Quartile (Bottom 25%)',
      config: { quartile: "Q4", maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    },
    {
      name: 'STRONG_BUY Recommendation',
      config: { recommendation: "STRONG_BUY", maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    },
    {
      name: 'HOLD Recommendation',
      config: { recommendation: "HOLD", maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    },
    {
      name: 'Score Range 85-95',
      config: { elivateScoreRange: { min: 85, max: 95 }, maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    },
    {
      name: 'Score Range 60-70',
      config: { elivateScoreRange: { min: 60, max: 70 }, maxFunds: "3", startDate: "2024-01-01", endDate: "2024-12-31", initialAmount: "100000", rebalancePeriod: "quarterly" }
    }
  ];

  const results = [];

  for (const test of testCases) {
    try {
      console.log(`Testing: ${test.name}`);
      
      const response = await axios.post('http://localhost:5000/api/comprehensive-backtest', test.config);
      const data = response.data;
      
      if (data.attribution?.fundContributions) {
        const funds = data.attribution.fundContributions;
        const scores = funds.map(f => parseFloat(f.elivateScore));
        const fundNames = funds.map(f => f.fundName.substring(0, 40) + '...');
        
        results.push({
          test: test.name,
          fundCount: funds.length,
          scoreRange: { min: Math.min(...scores), max: Math.max(...scores) },
          avgScore: (scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(2),
          performance: data.performance.totalReturn.toFixed(2),
          funds: fundNames
        });
        
        console.log(`  ✓ Funds: ${funds.length} | Score Range: ${Math.min(...scores)}-${Math.max(...scores)} | Avg: ${(scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(2)} | Return: ${data.performance.totalReturn.toFixed(2)}%`);
      } else {
        console.log(`  ❌ No fund data returned`);
      }
      
    } catch (error) {
      console.log(`  ❌ Test failed: ${error.message}`);
      results.push({
        test: test.name,
        error: error.message
      });
    }
  }
  
  console.log('\n📊 DIVERSITY VALIDATION SUMMARY');
  console.log('================================\n');
  
  results.forEach(result => {
    if (result.error) {
      console.log(`❌ ${result.test}: ${result.error}`);
    } else {
      console.log(`✅ ${result.test}:`);
      console.log(`   Score Range: ${result.scoreRange.min} - ${result.scoreRange.max} (Avg: ${result.avgScore})`);
      console.log(`   Performance: ${result.performance}%`);
      console.log(`   Sample Funds: ${result.funds.slice(0, 2).join(', ')}`);
      console.log('');
    }
  });
  
  // Check for proper diversity
  const scoreRanges = results.filter(r => !r.error).map(r => r.scoreRange);
  const minScores = scoreRanges.map(r => r.min);
  const maxScores = scoreRanges.map(r => r.max);
  
  const diversityScore = Math.max(...maxScores) - Math.min(...minScores);
  
  console.log(`🎯 DIVERSITY ANALYSIS:`);
  console.log(`   Score Spread: ${Math.min(...minScores).toFixed(1)} - ${Math.max(...maxScores).toFixed(1)} (Range: ${diversityScore.toFixed(1)})`);
  
  if (diversityScore > 30) {
    console.log('   ✅ EXCELLENT - Strong diversity across backtesting types');
  } else if (diversityScore > 15) {
    console.log('   ⚠️  MODERATE - Some diversity, room for improvement');
  } else {
    console.log('   ❌ POOR - Limited diversity, most tests using similar funds');
  }
}

validateBacktestingDiversity().catch(console.error);