/**
 * Simple Backtesting Test
 * Tests the optimized backtesting engine with minimal processing
 */

const { BacktestingEngine } = require('./server/services/backtesting-engine.ts');

async function testSimpleBacktest() {
  console.log('Starting simple backtesting test...');
  
  try {
    const engine = new BacktestingEngine();
    
    // Test portfolio generation first
    console.log('Testing portfolio generation...');
    const portfolio = await engine.generatePortfolio('Conservative');
    
    if (!portfolio) {
      console.error('Failed to generate portfolio');
      return;
    }
    
    console.log(`Generated portfolio with ${portfolio.allocations?.length || 0} allocations`);
    
    // Test with very short date range
    const startDate = new Date('2024-01-01');
    const endDate = new Date('2024-01-07'); // Just one week
    const initialAmount = 10000; // Smaller amount
    
    console.log('Running minimal backtest...');
    const results = await engine.runBacktest(
      portfolio,
      startDate,
      endDate,
      initialAmount,
      'quarterly'
    );
    
    console.log('Backtest results:', {
      portfolioId: results.portfolioId,
      totalReturn: results.totalReturn,
      finalAmount: results.finalAmount,
      dataPoints: results.returns?.length || 0
    });
    
  } catch (error) {
    console.error('Test failed:', error.message);
  }
}

testSimpleBacktest();