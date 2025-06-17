/**
 * Phase 3: Sector Analysis Implementation
 * Implements comprehensive sector-based analytics with authentic data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

class SectorAnalysisEngine {
  
  /**
   * Phase 3.1: Implement Sector Classification
   */
  static async implementSectorClassification() {
    console.log('Phase 3.1: Implementing Sector Classification...');
    
    try {
      // Define authentic sector mappings based on fund categories
      const sectorMappings = [
        { category: 'Equity', sectors: ['Large Cap', 'Mid Cap', 'Small Cap', 'Multi Cap', 'Flexi Cap'] },
        { category: 'Debt', sectors: ['Government', 'Corporate Bond', 'Credit Risk', 'Banking & PSU'] },
        { category: 'Hybrid', sectors: ['Conservative', 'Balanced', 'Aggressive', 'Dynamic Asset Allocation'] },
        { category: 'Thematic', sectors: ['Technology', 'Healthcare', 'Infrastructure', 'Energy', 'Banking'] }
      ];

      // Create sector classification for existing funds
      let classified = 0;
      for (const mapping of sectorMappings) {
        for (const sector of mapping.sectors) {
          const updateResult = await pool.query(`
            UPDATE funds 
            SET sector = $1
            WHERE category ILIKE $2
            AND (subcategory ILIKE $3 OR fund_name ILIKE $3)
            AND sector IS NULL
          `, [sector, `%${mapping.category}%`, `%${sector}%`]);
          
          classified += updateResult.rowCount;
        }
      }

      console.log(`Phase 3.1 Complete: ${classified} funds classified by sector`);
      return { success: true, classified };
      
    } catch (error) {
      console.error('Phase 3.1 Error:', error);
      throw error;
    }
  }

  /**
   * Phase 3.2: Implement Sector Performance Analytics
   */
  static async implementSectorPerformanceAnalytics() {
    console.log('Phase 3.2: Implementing Sector Performance Analytics...');
    
    try {
      // Calculate sector-wise performance metrics
      const sectorPerformance = await pool.query(`
        WITH sector_stats AS (
          SELECT 
            f.sector,
            COUNT(DISTINCT f.id) as fund_count,
            AVG(fsc.total_score) as avg_elivate_score,
            AVG(fsc.return_1y_absolute) as avg_1y_return,
            AVG(fsc.volatility_1y_percent) as avg_volatility,
            AVG(fsc.sharpe_ratio) as avg_sharpe_ratio,
            STDDEV(fsc.total_score) as score_deviation
          FROM funds f
          LEFT JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
          WHERE f.sector IS NOT NULL
          AND fsc.total_score IS NOT NULL
          GROUP BY f.sector
          HAVING COUNT(DISTINCT f.id) >= 5
        )
        SELECT 
          sector,
          fund_count,
          ROUND(avg_elivate_score, 2) as avg_elivate_score,
          ROUND(avg_1y_return, 4) as avg_1y_return,
          ROUND(avg_volatility, 4) as avg_volatility,
          ROUND(avg_sharpe_ratio, 4) as avg_sharpe_ratio,
          ROUND(score_deviation, 2) as score_deviation
        FROM sector_stats
        ORDER BY avg_elivate_score DESC
      `);

      console.log('Sector Performance Analysis:');
      sectorPerformance.rows.forEach(sector => {
        console.log(`  ${sector.sector}: ${sector.fund_count} funds, Score: ${sector.avg_elivate_score}, Return: ${sector.avg_1y_return}%`);
      });

      // Store sector analytics in database
      for (const sector of sectorPerformance.rows) {
        await pool.query(`
          INSERT INTO sector_analytics (
            sector_name, fund_count, avg_elivate_score, avg_return_1y, 
            avg_volatility, avg_sharpe_ratio, score_deviation, analysis_date
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_DATE)
          ON CONFLICT (sector_name, analysis_date) 
          DO UPDATE SET 
            fund_count = EXCLUDED.fund_count,
            avg_elivate_score = EXCLUDED.avg_elivate_score,
            avg_return_1y = EXCLUDED.avg_return_1y,
            avg_volatility = EXCLUDED.avg_volatility,
            avg_sharpe_ratio = EXCLUDED.avg_sharpe_ratio,
            score_deviation = EXCLUDED.score_deviation
        `, [
          sector.sector, sector.fund_count, sector.avg_elivate_score,
          sector.avg_1y_return, sector.avg_volatility, sector.avg_sharpe_ratio,
          sector.score_deviation
        ]);
      }

      console.log(`Phase 3.2 Complete: ${sectorPerformance.rows.length} sector analytics calculated`);
      return { success: true, sectors: sectorPerformance.rows.length };
      
    } catch (error) {
      // Handle case where sector_analytics table doesn't exist
      if (error.message.includes('relation "sector_analytics" does not exist')) {
        console.log('Creating sector_analytics table...');
        await this.createSectorAnalyticsTable();
        return await this.implementSectorPerformanceAnalytics();
      }
      console.error('Phase 3.2 Error:', error);
      throw error;
    }
  }

  /**
   * Create sector analytics table if it doesn't exist
   */
  static async createSectorAnalyticsTable() {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sector_analytics (
        id SERIAL PRIMARY KEY,
        sector_name VARCHAR(100) NOT NULL,
        fund_count INTEGER,
        avg_elivate_score DECIMAL(5,2),
        avg_return_1y DECIMAL(8,4),
        avg_volatility DECIMAL(8,4),
        avg_sharpe_ratio DECIMAL(8,4),
        score_deviation DECIMAL(5,2),
        analysis_date DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(sector_name, analysis_date)
      )
    `);
  }

  /**
   * Phase 3.3: Implement Sector Correlation Analysis
   */
  static async implementSectorCorrelationAnalysis() {
    console.log('Phase 3.3: Implementing Sector Correlation Analysis...');
    
    try {
      // Get sectors with sufficient data
      const sectorsWithData = await pool.query(`
        SELECT DISTINCT f.sector
        FROM funds f
        JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
        WHERE f.sector IS NOT NULL
        AND fsc.return_1y_absolute IS NOT NULL
        GROUP BY f.sector
        HAVING COUNT(*) >= 10
      `);

      const sectors = sectorsWithData.rows.map(row => row.sector);
      console.log(`Analyzing correlations for ${sectors.length} sectors`);

      // Calculate pairwise correlations
      const correlations = [];
      for (let i = 0; i < sectors.length; i++) {
        for (let j = i + 1; j < sectors.length; j++) {
          const correlation = await this.calculateSectorCorrelation(sectors[i], sectors[j]);
          correlations.push({
            sector1: sectors[i],
            sector2: sectors[j],
            correlation: correlation
          });
        }
      }

      // Store correlation results
      for (const corr of correlations) {
        await pool.query(`
          INSERT INTO sector_correlations (
            sector1, sector2, correlation_coefficient, analysis_date
          ) VALUES ($1, $2, $3, CURRENT_DATE)
          ON CONFLICT (sector1, sector2, analysis_date)
          DO UPDATE SET correlation_coefficient = EXCLUDED.correlation_coefficient
        `, [corr.sector1, corr.sector2, corr.correlation]);
      }

      console.log('Top Sector Correlations:');
      correlations
        .sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation))
        .slice(0, 5)
        .forEach(corr => {
          console.log(`  ${corr.sector1} ↔ ${corr.sector2}: ${corr.correlation.toFixed(3)}`);
        });

      console.log(`Phase 3.3 Complete: ${correlations.length} sector correlations calculated`);
      return { success: true, correlations: correlations.length };
      
    } catch (error) {
      if (error.message.includes('relation "sector_correlations" does not exist')) {
        await this.createSectorCorrelationsTable();
        return await this.implementSectorCorrelationAnalysis();
      }
      console.error('Phase 3.3 Error:', error);
      throw error;
    }
  }

  /**
   * Calculate correlation between two sectors
   */
  static async calculateSectorCorrelation(sector1, sector2) {
    const data = await pool.query(`
      WITH sector1_returns AS (
        SELECT fsc.return_1y_absolute as return1
        FROM funds f
        JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
        WHERE f.sector = $1 AND fsc.return_1y_absolute IS NOT NULL
      ),
      sector2_returns AS (
        SELECT fsc.return_1y_absolute as return2
        FROM funds f
        JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
        WHERE f.sector = $2 AND fsc.return_1y_absolute IS NOT NULL
      )
      SELECT 
        AVG(s1.return1) as mean1,
        AVG(s2.return2) as mean2,
        COUNT(*) as n1,
        COUNT(*) as n2
      FROM sector1_returns s1, sector2_returns s2
    `, [sector1, sector2]);

    if (data.rows.length === 0) return 0;

    // Simplified correlation calculation (using average returns as proxy)
    const returns1 = await pool.query(`
      SELECT fsc.return_1y_absolute
      FROM funds f
      JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
      WHERE f.sector = $1 AND fsc.return_1y_absolute IS NOT NULL
    `, [sector1]);

    const returns2 = await pool.query(`
      SELECT fsc.return_1y_absolute
      FROM funds f
      JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
      WHERE f.sector = $2 AND fsc.return_1y_absolute IS NOT NULL
    `, [sector2]);

    if (returns1.rows.length < 5 || returns2.rows.length < 5) return 0;

    const r1 = returns1.rows.map(r => parseFloat(r.return_1y_absolute));
    const r2 = returns2.rows.map(r => parseFloat(r.return_1y_absolute));

    const mean1 = r1.reduce((sum, val) => sum + val, 0) / r1.length;
    const mean2 = r2.reduce((sum, val) => sum + val, 0) / r2.length;

    const minLength = Math.min(r1.length, r2.length);
    let numerator = 0;
    let denominator1 = 0;
    let denominator2 = 0;

    for (let i = 0; i < minLength; i++) {
      const dev1 = r1[i] - mean1;
      const dev2 = r2[i] - mean2;
      numerator += dev1 * dev2;
      denominator1 += dev1 * dev1;
      denominator2 += dev2 * dev2;
    }

    const denominator = Math.sqrt(denominator1 * denominator2);
    return denominator > 0 ? numerator / denominator : 0;
  }

  /**
   * Create sector correlations table
   */
  static async createSectorCorrelationsTable() {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sector_correlations (
        id SERIAL PRIMARY KEY,
        sector1 VARCHAR(100) NOT NULL,
        sector2 VARCHAR(100) NOT NULL,
        correlation_coefficient DECIMAL(6,4),
        analysis_date DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(sector1, sector2, analysis_date)
      )
    `);
  }

  /**
   * Phase 3 Validation
   */
  static async validatePhase3() {
    console.log('\nPhase 3 Validation: Testing Sector Analysis...');
    
    try {
      // Check sector classification coverage
      const classificationCheck = await pool.query(`
        SELECT 
          COUNT(*) as total_funds,
          COUNT(CASE WHEN sector IS NOT NULL THEN 1 END) as funds_with_sector,
          COUNT(DISTINCT sector) as unique_sectors
        FROM funds
        WHERE category IS NOT NULL
      `);

      const classification = classificationCheck.rows[0];
      console.log('Sector Classification Coverage:');
      console.log(`  Total funds: ${classification.total_funds}`);
      console.log(`  Funds with sector: ${classification.funds_with_sector}`);
      console.log(`  Unique sectors: ${classification.unique_sectors}`);

      // Check sector analytics
      const analyticsCheck = await pool.query(`
        SELECT COUNT(*) as sector_analytics_count
        FROM sector_analytics
        WHERE analysis_date = CURRENT_DATE
      `);

      const analytics = analyticsCheck.rows[0];
      console.log(`\nSector Analytics: ${analytics.sector_analytics_count} sectors analyzed`);

      // Check correlations
      const correlationsCheck = await pool.query(`
        SELECT COUNT(*) as correlations_count
        FROM sector_correlations
        WHERE analysis_date = CURRENT_DATE
      `);

      const correlations = correlationsCheck.rows[0];
      console.log(`Sector Correlations: ${correlations.correlations_count} correlations calculated`);

      // Sample sector data
      const sampleSectors = await pool.query(`
        SELECT sector_name, fund_count, avg_elivate_score, avg_return_1y
        FROM sector_analytics
        WHERE analysis_date = CURRENT_DATE
        ORDER BY avg_elivate_score DESC
        LIMIT 5
      `);

      console.log('\nTop Performing Sectors:');
      sampleSectors.rows.forEach((sector, idx) => {
        console.log(`  ${idx + 1}. ${sector.sector_name}: ${sector.fund_count} funds, Score: ${sector.avg_elivate_score}, Return: ${sector.avg_return_1y}%`);
      });

      const validationPassed = 
        parseInt(classification.funds_with_sector) > 1000 &&
        parseInt(classification.unique_sectors) >= 5 &&
        parseInt(analytics.sector_analytics_count) >= 3;

      console.log(`\nPhase 3 Status: ${validationPassed ? 'PASSED ✅' : 'NEEDS ATTENTION ⚠️'}`);
      return { 
        success: validationPassed, 
        classification, 
        analytics: analytics.sector_analytics_count,
        correlations: correlations.correlations_count
      };
      
    } catch (error) {
      console.error('Phase 3 Validation Error:', error);
      return { success: false, error: error.message };
    }
  }
}

// Execute Phase 3
async function executePhase3() {
  console.log('Starting Phase 3: Sector Analysis Implementation\n');
  
  try {
    // Phase 3.1: Sector Classification
    await SectorAnalysisEngine.implementSectorClassification();
    
    // Phase 3.2: Performance Analytics
    await SectorAnalysisEngine.implementSectorPerformanceAnalytics();
    
    // Phase 3.3: Correlation Analysis
    await SectorAnalysisEngine.implementSectorCorrelationAnalysis();
    
    // Validation
    const validation = await SectorAnalysisEngine.validatePhase3();
    
    if (validation.success) {
      console.log('\nPhase 3 Complete: Sector Analysis successfully implemented');
      console.log('Ready to proceed to Phase 4');
    } else {
      console.log('\nPhase 3 Failed: Issues detected');
      console.log('Issues:', validation.error || 'Validation criteria not met');
    }
    
    return validation;
    
  } catch (error) {
    console.error('Phase 3 Execution Error:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

executePhase3().catch(console.error);