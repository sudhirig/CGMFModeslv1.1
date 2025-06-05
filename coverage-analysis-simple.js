/**
 * Simple MFAPI.in Coverage Analysis
 */

import pg from 'pg';
const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function getMFAPITotalCount() {
  try {
    const response = await fetch('https://api.mfapi.in/mf');
    const funds = await response.json();
    return funds.length;
  } catch (error) {
    console.error('Error fetching MFAPI total:', error.message);
    return null;
  }
}

async function analyzeCoverage() {
  console.log('🔍 MFAPI.in Coverage Analysis\n');
  
  try {
    // Get MFAPI.in total
    const mfapiTotal = await getMFAPITotalCount();
    console.log(`📊 MFAPI.in Total Funds: ${mfapiTotal?.toLocaleString() || 'Unable to fetch'}`);
    
    // Get your database stats
    const dbQuery = await pool.query(`
      SELECT 
        COUNT(DISTINCT f.id) as total_funds_in_db,
        COUNT(DISTINCT nd.fund_id) as funds_with_nav_data,
        COUNT(*) as total_nav_records,
        MIN(nd.nav_date) as earliest_date,
        MAX(nd.nav_date) as latest_date,
        COUNT(DISTINCT nd.nav_date) as unique_trading_days
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
    `);
    
    const stats = dbQuery.rows[0];
    
    console.log('\n📈 Your Database:');
    console.log(`  Total Funds: ${parseInt(stats.total_funds_in_db).toLocaleString()}`);
    console.log(`  Funds with NAV Data: ${parseInt(stats.funds_with_nav_data).toLocaleString()}`);
    console.log(`  Total NAV Records: ${parseInt(stats.total_nav_records).toLocaleString()}`);
    console.log(`  Date Range: ${stats.earliest_date} to ${stats.latest_date}`);
    console.log(`  Trading Days Covered: ${parseInt(stats.unique_trading_days).toLocaleString()}`);
    
    if (mfapiTotal) {
      const fundCoverage = (parseInt(stats.funds_with_nav_data) / mfapiTotal * 100);
      console.log(`\n📊 Coverage Analysis:`);
      console.log(`  Your Funds vs MFAPI Total: ${fundCoverage.toFixed(1)}%`);
      console.log(`  Missing from MFAPI: ${(mfapiTotal - parseInt(stats.funds_with_nav_data)).toLocaleString()} funds`);
    }
    
    // Analyze data quality
    const qualityQuery = await pool.query(`
      SELECT 
        CASE 
          WHEN COUNT(*) >= 1260 THEN '5+ years'
          WHEN COUNT(*) >= 756 THEN '3-5 years'
          WHEN COUNT(*) >= 252 THEN '1-3 years'
          WHEN COUNT(*) >= 63 THEN '3-12 months'
          ELSE 'Less than 3 months'
        END as data_depth,
        COUNT(DISTINCT fund_id) as fund_count
      FROM nav_data 
      GROUP BY fund_id
    `);
    
    console.log('\n📋 Your Data Quality:');
    const depthCounts = {};
    qualityQuery.rows.forEach(row => {
      depthCounts[row.data_depth] = parseInt(row.fund_count);
    });
    
    Object.entries(depthCounts).forEach(([depth, count]) => {
      console.log(`  ${depth}: ${count.toLocaleString()} funds`);
    });
    
    console.log('\n🎯 Summary:');
    console.log(`  MFAPI.in has ${mfapiTotal?.toLocaleString() || 'unknown'} total funds`);
    console.log(`  You have imported NAV data for ${parseInt(stats.funds_with_nav_data).toLocaleString()} funds`);
    console.log(`  Your coverage: ${mfapiTotal ? (parseInt(stats.funds_with_nav_data) / mfapiTotal * 100).toFixed(1) + '%' : 'unknown'}`);
    console.log(`  You have ${parseInt(stats.total_nav_records).toLocaleString()} authentic NAV records`);
    
  } catch (error) {
    console.error('Analysis error:', error.message);
  } finally {
    await pool.end();
  }
}

analyzeCoverage();