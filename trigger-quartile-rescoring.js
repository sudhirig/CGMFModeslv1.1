// Script to monitor NAV data import and trigger quartile rescoring when complete
import pg from 'pg';
import axios from 'axios';

const { Pool } = pg;

async function monitorAndTriggerRescoring() {
  try {
    console.log('\n=== NAV DATA IMPORT MONITORING ===');
    console.log('Date/Time: ' + new Date().toLocaleString());
    console.log('==============================\n');
    
    // Connect to database
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    
    // Get the total number of funds that should have NAV data
    const fundsResult = await pool.query('SELECT COUNT(*) as total_funds FROM funds');
    const totalFunds = parseInt(fundsResult.rows[0].total_funds);
    console.log(`Total funds in database: ${totalFunds}`);
    
    // Check current NAV data status
    const navResult = await pool.query(`
      SELECT COUNT(DISTINCT fund_id) as funds_with_nav,
             COUNT(*) as total_nav_records
      FROM nav_data
    `);
    
    const fundsWithNav = parseInt(navResult.rows[0].funds_with_nav);
    const totalNavRecords = parseInt(navResult.rows[0].total_nav_records);
    
    console.log(`\n=== PHASE 1: CURRENT NAV IMPORT ===`);
    console.log(`Funds with NAV data: ${fundsWithNav} / ${totalFunds} (${Math.round(fundsWithNav/totalFunds*100)}%)`);
    console.log(`Total NAV records: ${totalNavRecords}`);
    
    // Get a sample of recently imported funds
    const recentNavsResult = await pool.query(`
      SELECT f.fund_name, n.nav_value, n.nav_date
      FROM nav_data n
      JOIN funds f ON n.fund_id = f.id
      ORDER BY n.created_at DESC
      LIMIT 5
    `);
    
    if (recentNavsResult.rows.length > 0) {
      console.log(`\nRecently imported NAVs:`);
      recentNavsResult.rows.forEach(row => {
        console.log(`- ${row.fund_name}: ${row.nav_value} (${row.nav_date})`);
      });
    }
    
    // Check the ETL status for the Historical NAV Import
    const etlResult = await pool.query(`
      SELECT id, status, start_time, end_time, records_processed, error_message
      FROM etl_pipeline_runs
      WHERE pipeline_name = 'Historical NAV Import'
      ORDER BY id DESC
      LIMIT 1
    `);
    
    console.log(`\n=== ETL PROCESS STATUS ===`);
    if (etlResult.rows.length > 0) {
      const etlStatus = etlResult.rows[0];
      console.log(`Historical NAV Import Status: ${etlStatus.status}`);
      console.log(`Started: ${etlStatus.start_time}`);
      console.log(`Completed: ${etlStatus.end_time || 'In progress'}`);
      console.log(`Records processed: ${etlStatus.records_processed}`);
      
      // Calculate runtime
      const startTime = new Date(etlStatus.start_time);
      const currentTime = new Date();
      const runtimeMs = currentTime - startTime;
      const runtimeHours = Math.floor(runtimeMs / (1000 * 60 * 60));
      const runtimeMinutes = Math.floor((runtimeMs % (1000 * 60 * 60)) / (1000 * 60));
      console.log(`Runtime: ${runtimeHours} hours, ${runtimeMinutes} minutes`);
      
      if (etlStatus.error_message) {
        console.log(`Latest message: ${etlStatus.error_message}`);
      }
      
      // Check for funds with multiple NAV entries (historical data)
      const multipleEntriesResult = await pool.query(`
        SELECT COUNT(*) as funds_with_history
        FROM (
          SELECT fund_id, COUNT(*) as record_count
          FROM nav_data
          GROUP BY fund_id
          HAVING COUNT(*) > 1
        ) as subquery
      `);
      
      const fundsWithHistory = parseInt(multipleEntriesResult.rows[0].funds_with_history);
      
      console.log(`\n=== PHASE 2: HISTORICAL NAV IMPORT ===`);
      console.log(`Funds with historical NAV data: ${fundsWithHistory} / ${fundsWithNav} (${Math.round(fundsWithHistory/fundsWithNav*100) || 0}%)`);
      
      // Get details on funds with the most historical data
      if (fundsWithHistory > 0) {
        const historyDetailsResult = await pool.query(`
          SELECT f.id, f.fund_name, f.amc_name, COUNT(*) as record_count,
                 MIN(n.nav_date) as earliest_date,
                 MAX(n.nav_date) as latest_date,
                 COUNT(DISTINCT n.nav_value) as distinct_values,
                 MIN(n.nav_value) as min_value,
                 MAX(n.nav_value) as max_value,
                 (MAX(n.nav_value) - MIN(n.nav_value)) / MIN(n.nav_value) * 100 as percent_variation
          FROM nav_data n
          JOIN funds f ON n.fund_id = f.id
          GROUP BY f.id, f.fund_name, f.amc_name
          HAVING COUNT(*) > 1
          ORDER BY COUNT(*) DESC
          LIMIT 3
        `);
        
        console.log(`\nFunds with most historical data:`);
        historyDetailsResult.rows.forEach(row => {
          console.log(`- ${row.fund_name} (${row.amc_name})`);
          console.log(`  Records: ${row.record_count}, Date range: ${row.earliest_date} to ${row.latest_date}`);
          console.log(`  NAV range: ${row.min_value} to ${row.max_value} (${row.percent_variation.toFixed(2)}% variation)`);
          console.log(`  Distinct values: ${row.distinct_values}`);
        });
        
        // Get a sample of historical NAV data for a single fund
        if (historyDetailsResult.rows.length > 0) {
          const sampleFundId = historyDetailsResult.rows[0].id;
          const historyValuesResult = await pool.query(`
            SELECT nav_date, nav_value
            FROM nav_data
            WHERE fund_id = $1
            ORDER BY nav_date DESC
            LIMIT 5
          `, [sampleFundId]);
          
          console.log(`\nSample historical NAV values for ${historyDetailsResult.rows[0].fund_name}:`);
          historyValuesResult.rows.forEach(row => {
            console.log(`- ${row.nav_date}: ${row.nav_value}`);
          });
        }
      }
      
      // Calculate avg records per fund
      const avgRecordsPerFund = totalNavRecords / (fundsWithNav || 1);
      console.log(`\nAverage NAV records per fund: ${avgRecordsPerFund.toFixed(2)}`);
      
      // Check if NAV import is complete or close to complete
      // We consider it complete when:
      // 1. The ETL status is COMPLETED, or
      // 2. We have NAV data for at least 90% of funds AND
      //    The average records per fund is at least 12 (1 year of data)
      const isImportComplete = 
        etlStatus.status === 'COMPLETED' || 
        (fundsWithNav / totalFunds >= 0.9 && avgRecordsPerFund >= 12);
      
      console.log(`\n=== QUARTILE SCORING STATUS ===`);
      // Check existing fund scores
      const scoreResult = await pool.query(`
        SELECT COUNT(*) as total_scores,
               COUNT(DISTINCT fund_id) as scored_funds
        FROM fund_scores
      `);
      
      const totalScores = parseInt(scoreResult.rows[0].total_scores);
      const scoredFunds = parseInt(scoreResult.rows[0].scored_funds);
      
      console.log(`Funds with quartile scores: ${scoredFunds} / ${totalFunds} (${Math.round(scoredFunds/totalFunds*100) || 0}%)`);
      console.log(`Total score records: ${totalScores}`);
      
      if (isImportComplete) {
        console.log('\n=== ACTION REQUIRED ===');
        console.log('✓ NAV data import appears to be complete or nearly complete.');
        console.log('✓ Triggering full quartile rescoring...');
        
        try {
          // Trigger the quartile scoring
          const response = await axios.post('http://localhost:5000/api/quartile/start-scoring', {
            force: true,
            batchSize: 500,
            complete: true
          });
          
          console.log('✓ Quartile rescoring triggered successfully!');
          console.log('Response:', response.data);
        } catch (error) {
          console.error('✗ Error triggering quartile rescoring:', error.message);
        }
      } else {
        console.log('\n=== NEXT STEPS ===');
        console.log('→ NAV data import is still in progress.');
        console.log('→ Current progress: Phase 1 is ' + Math.round(fundsWithNav/totalFunds*100) + '% complete.');
        
        if (fundsWithHistory > 0) {
          console.log('→ Phase 2 has started: ' + fundsWithHistory + ' funds have historical data.');
        } else {
          console.log('→ Phase 2 has not started yet. Will begin after Phase 1 completes.');
        }
        
        console.log('→ Run this script again later to check progress and trigger quartile rescoring when complete.');
      }
    } else {
      console.log('No Historical NAV Import ETL run found.');
    }
    
    console.log('\n==============================');
    
    // Close the pool
    await pool.end();
    
  } catch (error) {
    console.error('Error monitoring NAV data import:', error);
  }
}

// Run the monitoring process
monitorAndTriggerRescoring();