// Script to monitor NAV data import and trigger quartile rescoring when complete
import pg from 'pg';
import axios from 'axios';

const { Pool } = pg;

async function monitorAndTriggerRescoring() {
  try {
    console.log('Starting NAV data import monitoring...');
    
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
    
    console.log(`Funds with NAV data: ${fundsWithNav} / ${totalFunds} (${Math.round(fundsWithNav/totalFunds*100)}%)`);
    console.log(`Total NAV records: ${totalNavRecords}`);
    
    // Check the ETL status for the Historical NAV Import
    const etlResult = await pool.query(`
      SELECT id, status, start_time, end_time, records_processed
      FROM etl_pipeline_runs
      WHERE pipeline_name = 'Historical NAV Import'
      ORDER BY id DESC
      LIMIT 1
    `);
    
    if (etlResult.rows.length > 0) {
      const etlStatus = etlResult.rows[0];
      console.log(`Historical NAV Import Status: ${etlStatus.status}`);
      console.log(`Started: ${etlStatus.start_time}`);
      console.log(`Completed: ${etlStatus.end_time || 'In progress'}`);
      console.log(`Records processed: ${etlStatus.records_processed}`);
      
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
      console.log(`Funds with historical NAV data: ${fundsWithHistory} / ${fundsWithNav}`);
      
      // Calculate avg records per fund
      const avgRecordsPerFund = totalNavRecords / (fundsWithNav || 1);
      console.log(`Average NAV records per fund: ${avgRecordsPerFund.toFixed(2)}`);
      
      // Check if NAV import is complete or close to complete
      // We consider it complete when:
      // 1. The ETL status is COMPLETED, or
      // 2. We have NAV data for at least 90% of funds AND
      //    The average records per fund is at least 12 (1 year of data)
      const isImportComplete = 
        etlStatus.status === 'COMPLETED' || 
        (fundsWithNav / totalFunds >= 0.9 && avgRecordsPerFund >= 12);
      
      if (isImportComplete) {
        console.log('NAV data import appears to be complete or nearly complete.');
        console.log('Triggering full quartile rescoring...');
        
        try {
          // Trigger the quartile scoring
          const response = await axios.post('http://localhost:5000/api/quartile/start-scoring', {
            force: true,
            batchSize: 500,
            complete: true
          });
          
          console.log('Quartile rescoring triggered successfully!');
          console.log('Response:', response.data);
        } catch (error) {
          console.error('Error triggering quartile rescoring:', error.message);
        }
      } else {
        console.log('NAV data import is still in progress.');
        console.log('Run this script again later to check progress and trigger quartile rescoring when complete.');
      }
    } else {
      console.log('No Historical NAV Import ETL run found.');
    }
    
    // Close the pool
    await pool.end();
    
  } catch (error) {
    console.error('Error monitoring NAV data import:', error);
  }
}

// Run the monitoring process
monitorAndTriggerRescoring();