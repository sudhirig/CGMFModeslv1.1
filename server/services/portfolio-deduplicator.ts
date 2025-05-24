/**
 * This service handles portfolio fund deduplication
 * It ensures that each fund appears only once in a portfolio
 */

export function removeDuplicateFundsFromPortfolio(portfolio: any): any {
  if (!portfolio || !portfolio.allocations || !Array.isArray(portfolio.allocations)) {
    return portfolio;
  }
  
  // Create a map to track funds we've seen by name
  const seenFundNames = new Set<string>();
  const uniqueAllocations = [];
  
  // Log the original allocations for debugging
  console.log(`Original portfolio has ${portfolio.allocations.length} fund allocations`);
  
  // Process each allocation and keep only unique funds
  for (const allocation of portfolio.allocations) {
    if (!allocation.fund || !allocation.fund.fundName) {
      uniqueAllocations.push(allocation);
      continue;
    }
    
    const fundName = allocation.fund.fundName;
    
    if (!seenFundNames.has(fundName)) {
      // Keep this fund since we haven't seen it before
      seenFundNames.add(fundName);
      uniqueAllocations.push(allocation);
    } else {
      // Skip this fund since we've already included it
      console.log(`Removing duplicate fund: ${fundName}`);
    }
  }
  
  // Update the portfolio with deduplicated allocations
  portfolio.allocations = uniqueAllocations;
  console.log(`After deduplication: ${uniqueAllocations.length} unique funds in portfolio`);
  
  return portfolio;
}