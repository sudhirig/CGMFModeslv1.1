/**
 * Import NAV data from AMFI's official source for funds without historical data
 * Uses the authoritative AMFI NAVAll.txt file
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function importFromAMFISource() {
  try {
    console.log('Starting import from AMFI official NAV source...');
    
    // Get AMFI NAV data
    console.log('Fetching current NAV data from AMFI...');
    const response = await axios.get('https://www.amfiindia.com/spages/NAVAll.txt', {
      timeout: 30000
    });
    
    if (!response.data) {
      throw new Error('No data received from AMFI');
    }
    
    console.log('Processing AMFI NAV data...');
    const lines = response.data.split('\n');
    console.log(`AMFI data contains ${lines.length} lines`);
    
    // Parse AMFI data format
    const navRecords = [];
    let currentCategory = '';
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      if (!line || line.includes('Scheme Code;ISIN')) continue;
      
      // Category headers
      if (line.includes('Open Ended Schemes') || line.includes('Close Ended Schemes') || line.includes('Interval Fund Schemes')) {
        currentCategory = line;
        continue;
      }
      
      // Parse NAV data lines
      const parts = line.split(';');
      if (parts.length >= 4) {
        const schemeCode = parts[0]?.trim();
        const schemeName = parts[3]?.trim();
        const navValue = parts[4]?.trim();
        const navDate = parts[5]?.trim();
        
        if (schemeCode && schemeName && navValue && navDate) {
          const nav = parseFloat(navValue);
          if (!isNaN(nav) && nav > 0) {
            navRecords.push({
              schemeCode,
              schemeName,
              navValue: nav,
              navDate,
              category: currentCategory
            });
          }
        }
      }
    }
    
    console.log(`Parsed ${navRecords.length} valid NAV records from AMFI`);
    
    // Match with our funds that need data
    const fundsNeedingData = await pool.query(`
      SELECT f.id, f.scheme_code, f.fund_name
      FROM funds f
      LEFT JOIN nav_data nd ON f.id = nd.fund_id
      WHERE nd.fund_id IS NULL
      AND f.scheme_code IS NOT NULL
      ORDER BY f.id
      LIMIT 100
    `);
    
    console.log(`Found ${fundsNeedingData.rows.length} funds needing NAV data`);
    
    let matchedCount = 0;
    let importedCount = 0;
    
    for (const fund of fundsNeedingData.rows) {
      // Find matching AMFI record
      const amfiRecord = navRecords.find(record => 
        record.schemeCode === fund.scheme_code
      );
      
      if (amfiRecord) {
        matchedCount++;
        
        try {
          // Convert AMFI date format (DD-MMM-YYYY) to YYYY-MM-DD
          const formattedDate = formatAMFIDate(amfiRecord.navDate);
          
          if (formattedDate) {
            await pool.query(`
              INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (fund_id, nav_date) DO NOTHING
            `, [
              fund.id,
              formattedDate,
              amfiRecord.navValue,
              new Date()
            ]);
            
            importedCount++;
            console.log(`✓ Imported NAV for ${fund.fund_name}: ${amfiRecord.navValue} on ${formattedDate}`);
          }
        } catch (error) {
          console.error(`Error importing NAV for fund ${fund.id}:`, error.message);
        }
      }
    }
    
    console.log(`\n=== AMFI Import Summary ===`);
    console.log(`Funds processed: ${fundsNeedingData.rows.length}`);
    console.log(`Scheme codes matched: ${matchedCount}`);
    console.log(`NAV records imported: ${importedCount}`);
    console.log(`Remaining funds needing data: ${fundsNeedingData.rows.length - matchedCount}`);
    
    // Now try historical import for newly matched funds using MFAPI
    if (importedCount > 0) {
      console.log('\nAttempting historical import for newly matched funds...');
      await importHistoricalForMatchedFunds();
    }
    
  } catch (error) {
    console.error('Error in AMFI import:', error);
  } finally {
    await pool.end();
  }
}

function formatAMFIDate(amfiDate) {
  try {
    // AMFI format: DD-MMM-YYYY (e.g., "31-May-2025")
    const parts = amfiDate.split('-');
    if (parts.length !== 3) return null;
    
    const day = parts[0].padStart(2, '0');
    const monthAbbr = parts[1];
    const year = parts[2];
    
    const monthMap = {
      'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
      'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    };
    
    const month = monthMap[monthAbbr];
    if (!month) return null;
    
    return `${year}-${month}-${day}`;
  } catch (error) {
    return null;
  }
}

async function importHistoricalForMatchedFunds() {
  try {
    // Get funds that now have some NAV data but might benefit from more historical data
    const recentlyMatched = await pool.query(`
      SELECT f.id, f.scheme_code, f.fund_name, COUNT(nd.nav_value) as nav_count
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE f.scheme_code IS NOT NULL
      GROUP BY f.id, f.scheme_code, f.fund_name
      HAVING COUNT(nd.nav_value) < 30
      ORDER BY nav_count DESC
      LIMIT 20
    `);
    
    console.log(`Attempting historical import for ${recentlyMatched.rows.length} funds with limited data`);
    
    for (const fund of recentlyMatched.rows) {
      try {
        console.log(`Checking MFAPI historical data for ${fund.fund_name}...`);
        
        const url = `https://api.mfapi.in/mf/${fund.scheme_code}`;
        const response = await axios.get(url, { timeout: 10000 });
        
        if (response.data && response.data.data && response.data.data.length > 0) {
          console.log(`Found ${response.data.data.length} historical records for ${fund.scheme_code}`);
          
          // Import recent historical data (last 3 months)
          const threeMonthsAgo = new Date();
          threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
          
          let imported = 0;
          for (const record of response.data.data.slice(0, 90)) { // Last 90 records
            try {
              // Convert DD-MM-YYYY to YYYY-MM-DD
              const dateParts = record.date.split('-');
              const formattedDate = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`;
              
              await pool.query(`
                INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (fund_id, nav_date) DO NOTHING
              `, [
                fund.id,
                formattedDate,
                parseFloat(record.nav),
                new Date()
              ]);
              imported++;
            } catch (insertError) {
              continue;
            }
          }
          
          if (imported > 0) {
            console.log(`✓ Imported ${imported} historical records for ${fund.fund_name}`);
          }
        }
        
        await new Promise(resolve => setTimeout(resolve, 1000));
        
      } catch (error) {
        console.log(`No additional historical data for ${fund.fund_name}`);
      }
    }
    
  } catch (error) {
    console.error('Error in historical import:', error);
  }
}

importFromAMFISource();