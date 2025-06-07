/**
 * Complete NAV import solution using verified AMFI connectivity
 * Import current NAV data for all funds without data, then attempt historical import
 */

import pkg from 'pg';
const { Pool } = pkg;
import axios from 'axios';

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function completeNavImportSolution() {
  try {
    console.log('Starting complete NAV import solution...\n');
    
    // Step 1: Import current NAV data from AMFI for all funds
    console.log('=== Step 1: Import Current NAV Data from AMFI ===');
    const currentNavImported = await importCurrentNavFromAMFI();
    
    // Step 2: Attempt historical import for funds that now have current data
    if (currentNavImported > 0) {
      console.log('\n=== Step 2: Attempt Historical Import ===');
      await attemptHistoricalImport();
    }
    
    // Step 3: Report final status
    console.log('\n=== Final Status Report ===');
    await reportFinalStatus();
    
  } catch (error) {
    console.error('Error in complete NAV import solution:', error);
  } finally {
    await pool.end();
  }
}

async function importCurrentNavFromAMFI() {
  console.log('Fetching AMFI NAV data...');
  
  const response = await axios.get('https://www.amfiindia.com/spages/NAVAll.txt', {
    timeout: 30000
  });
  
  const lines = response.data.split('\n');
  console.log(`Processing ${lines.length} lines from AMFI...`);
  
  // Parse AMFI data into a map for quick lookup
  const amfiNavMap = new Map();
  
  for (const line of lines) {
    if (!line.includes(';')) continue;
    
    const parts = line.split(';');
    if (parts.length >= 6) {
      const schemeCode = parts[0]?.trim();
      const schemeName = parts[3]?.trim();
      const navValue = parts[4]?.trim();
      const navDate = parts[5]?.trim();
      
      if (schemeCode && navValue && navDate) {
        const nav = parseFloat(navValue);
        if (!isNaN(nav) && nav > 0) {
          amfiNavMap.set(schemeCode, {
            nav: nav,
            date: formatAMFIDate(navDate),
            name: schemeName
          });
        }
      }
    }
  }
  
  console.log(`Parsed ${amfiNavMap.size} NAV records from AMFI`);
  
  // Get funds needing NAV data
  const fundsNeedingData = await pool.query(`
    SELECT f.id, f.scheme_code, f.fund_name
    FROM funds f
    LEFT JOIN nav_data nd ON f.id = nd.fund_id
    WHERE nd.fund_id IS NULL
    AND f.scheme_code IS NOT NULL
  `);
  
  console.log(`Found ${fundsNeedingData.rows.length} funds needing NAV data`);
  
  let importedCount = 0;
  let matchedCount = 0;
  
  for (const fund of fundsNeedingData.rows) {
    const amfiData = amfiNavMap.get(fund.scheme_code);
    
    if (amfiData && amfiData.date) {
      matchedCount++;
      
      try {
        await pool.query(`
          INSERT INTO nav_data (fund_id, nav_date, nav_value, created_at)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (fund_id, nav_date) DO NOTHING
        `, [
          fund.id,
          amfiData.date,
          amfiData.nav,
          new Date()
        ]);
        
        importedCount++;
        
        if (importedCount <= 10) {
          console.log(`✓ Imported NAV for ${fund.fund_name}: ${amfiData.nav} on ${amfiData.date}`);
        } else if (importedCount % 100 === 0) {
          console.log(`✓ Progress: ${importedCount} funds imported...`);
        }
        
      } catch (error) {
        console.error(`Error importing NAV for fund ${fund.id}:`, error.message);
      }
    }
  }
  
  console.log(`\nAMFI Import Summary:`);
  console.log(`- Funds processed: ${fundsNeedingData.rows.length}`);
  console.log(`- Scheme codes matched: ${matchedCount}`);
  console.log(`- NAV records imported: ${importedCount}`);
  
  return importedCount;
}

async function attemptHistoricalImport() {
  console.log('Attempting historical import for newly imported funds...');
  
  // Get funds that now have some NAV data but could benefit from more
  const candidates = await pool.query(`
    SELECT f.id, f.scheme_code, f.fund_name, COUNT(nd.nav_value) as nav_count
    FROM funds f
    JOIN nav_data nd ON f.id = nd.fund_id
    WHERE f.scheme_code IS NOT NULL
    GROUP BY f.id, f.scheme_code, f.fund_name
    HAVING COUNT(nd.nav_value) < 30
    ORDER BY f.id
    LIMIT 50
  `);
  
  console.log(`Testing historical import for ${candidates.rows.length} funds...`);
  
  let historicalImported = 0;
  
  for (const fund of candidates.rows) {
    try {
      const url = `https://api.mfapi.in/mf/${fund.scheme_code}`;
      const response = await axios.get(url, { 
        timeout: 8000,
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; Fund-Analysis/1.0)'
        }
      });
      
      if (response.data && response.data.data && response.data.data.length > 0) {
        console.log(`✓ Found ${response.data.data.length} historical records for ${fund.fund_name}`);
        
        // Import last 6 months of data
        let imported = 0;
        for (const record of response.data.data.slice(0, 180)) {
          try {
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
          historicalImported++;
          console.log(`  Imported ${imported} historical records`);
        }
      }
      
      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
      
    } catch (error) {
      // No historical data available for this fund
      continue;
    }
  }
  
  console.log(`Historical import completed for ${historicalImported} funds`);
}

async function reportFinalStatus() {
  const stats = await pool.query(`
    SELECT 
      'Total Funds' as category,
      COUNT(*) as count,
      'All funds in database' as description
    FROM funds
    UNION ALL
    SELECT 
      'Funds with NAV Data' as category,
      COUNT(DISTINCT f.id) as count,
      'Funds that have NAV records' as description
    FROM funds f
    JOIN nav_data nd ON f.id = nd.fund_id
    UNION ALL
    SELECT 
      'Funds Ready for Scoring' as category,
      COUNT(*) as count,
      'Funds with 30+ NAV records ready for scoring' as description
    FROM (
      SELECT f.id
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.fund_id IS NULL
      GROUP BY f.id
      HAVING COUNT(nd.nav_value) >= 30
    ) ready_funds
    UNION ALL
    SELECT 
      'Currently Scored Funds' as category,
      COUNT(DISTINCT fund_id) as count,
      'Funds with complete scoring' as description
    FROM fund_scores
  `);
  
  console.log('System Status After Import:');
  stats.rows.forEach(stat => {
    console.log(`- ${stat.category}: ${stat.count} (${stat.description})`);
  });
}

function formatAMFIDate(amfiDate) {
  try {
    // AMFI format: DD-MMM-YYYY
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
    return month ? `${year}-${month}-${day}` : null;
  } catch (error) {
    return null;
  }
}

completeNavImportSolution();