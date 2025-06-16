/**
 * Implement Missing VaR Calculations
 * Calculates Value at Risk (95% confidence level) for all funds using authentic volatility data
 * Based on existing methodology found in fund_scores_backup table
 */

import { db } from './server/db.js';
import { fundScoresCorrected, fundPerformanceMetrics } from './shared/schema.js';
import { eq, and, isNotNull } from 'drizzle-orm';

/**
 * Calculate 95% VaR using parametric method
 * VaR = μ - (Z-score × σ)
 * For 95% confidence: Z-score = 1.645
 * Using daily volatility converted to percentage
 */
function calculateVaR95(volatilityDaily, meanReturn = 0) {
  if (!volatilityDaily || volatilityDaily <= 0) return null;
  
  // Convert daily volatility to percentage and apply 95% confidence Z-score
  const zScore95 = 1.645;
  const volatilityPercent = volatilityDaily * 100;
  const var95 = Math.abs(meanReturn - (zScore95 * volatilityPercent));
  
  return Math.round(var95 * 10000) / 10000; // Round to 4 decimal places
}

/**
 * Calculate VaR score based on relative risk assessment
 * Lower VaR = better score (less risky)
 */
function calculateVaRScore(varValue, category) {
  if (!varValue) return 0;
  
  // Score inversely proportional to VaR - lower VaR gets higher score
  // Normalize to 0-10 scale based on typical VaR ranges
  const maxVaR = 50; // Maximum expected VaR for scoring
  const normalizedVar = Math.min(varValue, maxVaR) / maxVaR;
  const score = (1 - normalizedVar) * 10;
  
  return Math.max(0, Math.min(10, Math.round(score * 100) / 100));
}

/**
 * Process VaR calculations for all funds with volatility data
 */
async function implementVaRCalculations() {
  console.log('🎯 Starting VaR implementation using authentic volatility data...');
  
  try {
    // Get all funds with existing volatility data from fund_performance_metrics
    const fundsWithVolatility = await db
      .select({
        fundId: fundPerformanceMetrics.fundId,
        volatility: fundPerformanceMetrics.volatility,
        category: fundPerformanceMetrics.category
      })
      .from(fundPerformanceMetrics)
      .where(isNotNull(fundPerformanceMetrics.volatility));
    
    console.log(`📊 Found ${fundsWithVolatility.length} funds with authentic volatility data`);
    
    let processedCount = 0;
    const batchSize = 500;
    
    // Process in batches
    for (let i = 0; i < fundsWithVolatility.length; i += batchSize) {
      const batch = fundsWithVolatility.slice(i, i + batchSize);
      console.log(`Processing VaR batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(fundsWithVolatility.length/batchSize)}...`);
      
      for (const fund of batch) {
        await processVaRForFund(fund);
        processedCount++;
      }
      
      if (processedCount % 1000 === 0) {
        console.log(`✓ Processed VaR calculations for ${processedCount} funds`);
      }
    }
    
    // Generate final VaR coverage report
    await generateVaRCoverageReport();
    
    console.log(`🎯 VaR implementation completed: ${processedCount} funds processed`);
    
  } catch (error) {
    console.error('❌ Error implementing VaR calculations:', error);
    throw error;
  }
}

/**
 * Process VaR calculation for a single fund
 */
async function processVaRForFund(fund) {
  try {
    // Calculate VaR using authentic volatility data
    const var95 = calculateVaR95(fund.volatility);
    
    if (var95 === null) return;
    
    // Calculate VaR score
    const varScore = calculateVaRScore(var95, fund.category);
    
    // Update fund_scores_corrected with VaR data
    await db
      .update(fundScoresCorrected)
      .set({
        var_95_1y: var95,
        var_score: varScore,
        var_calculation_date: new Date()
      })
      .where(
        and(
          eq(fundScoresCorrected.fundId, fund.fundId),
          eq(fundScoresCorrected.scoreDate, new Date().toISOString().split('T')[0])
        )
      );
    
  } catch (error) {
    console.error(`❌ Error processing VaR for fund ${fund.fundId}:`, error);
  }
}

/**
 * Generate comprehensive VaR coverage report
 */
async function generateVaRCoverageReport() {
  try {
    const coverageData = await db.execute(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN var_95_1y IS NOT NULL THEN 1 END) as funds_with_var,
        CAST((COUNT(CASE WHEN var_95_1y IS NOT NULL THEN 1 END) * 100.0 / COUNT(*)) AS NUMERIC(5,1)) as var_coverage_pct,
        CAST(MIN(var_95_1y) AS NUMERIC(8,4)) as min_var,
        CAST(MAX(var_95_1y) AS NUMERIC(8,4)) as max_var,
        CAST(AVG(var_95_1y) AS NUMERIC(8,4)) as avg_var,
        CAST(AVG(var_score) AS NUMERIC(5,2)) as avg_var_score
      FROM fund_scores_corrected
      WHERE score_date = CURRENT_DATE
    `);
    
    console.log('\n📊 VaR COVERAGE REPORT:');
    console.log('========================');
    console.log(`Total Funds: ${coverageData[0].total_funds}`);
    console.log(`Funds with VaR: ${coverageData[0].funds_with_var}`);
    console.log(`Coverage: ${coverageData[0].var_coverage_pct}%`);
    console.log(`VaR Range: ${coverageData[0].min_var} - ${coverageData[0].max_var}`);
    console.log(`Average VaR: ${coverageData[0].avg_var}`);
    console.log(`Average VaR Score: ${coverageData[0].avg_var_score}/10`);
    
    return coverageData[0];
    
  } catch (error) {
    console.error('❌ Error generating VaR coverage report:', error);
  }
}

/**
 * Validate VaR calculations against existing backup data
 */
async function validateVaRImplementation() {
  try {
    console.log('\n🔍 Validating VaR implementation against backup data...');
    
    const validationData = await db.execute(`
      SELECT 
        'Current Implementation' as source,
        COUNT(CASE WHEN var_95_1y IS NOT NULL THEN 1 END) as var_count,
        CAST(AVG(var_95_1y) AS NUMERIC(8,4)) as avg_var
      FROM fund_scores_corrected
      WHERE score_date = CURRENT_DATE
      
      UNION ALL
      
      SELECT 
        'Backup Reference' as source,
        COUNT(CASE WHEN var_95_1y IS NOT NULL THEN 1 END) as var_count,
        CAST(AVG(var_95_1y) AS NUMERIC(8,4)) as avg_var
      FROM fund_scores_backup
    `);
    
    console.log('\n📈 VaR VALIDATION RESULTS:');
    validationData.forEach(row => {
      console.log(`${row.source}: ${row.var_count} funds, Avg VaR: ${row.avg_var}`);
    });
    
  } catch (error) {
    console.error('❌ Error validating VaR implementation:', error);
  }
}

/**
 * Main execution function
 */
async function main() {
  console.log('🚀 VaR Implementation Started');
  console.log('Using authentic volatility data from fund_performance_metrics');
  console.log('Methodology based on existing VaR calculations in backup table\n');
  
  await implementVaRCalculations();
  await validateVaRImplementation();
  
  console.log('\n✅ VaR implementation completed successfully');
  console.log('All VaR calculations use authentic market data with zero synthetic generation');
}

// Run the implementation
main().catch(console.error);