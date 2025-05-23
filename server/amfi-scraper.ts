// Advanced AMFI Data Scraper to fetch real mutual fund data
import axios from 'axios';
import { executeRawQuery } from './db';

// AMFI provides mutual fund data through this URL
// This contains actual mutual fund names, codes, NAVs, etc.
const AMFI_NAV_ALL_URL = 'https://www.amfiindia.com/spages/NAVAll.txt';

interface ParsedFund {
  schemeCode: string;
  isinDivPayout: string | null;
  isinDivReinvest: string | null;
  fundName: string;
  amcName: string;
  category: string;
  subcategory: string | null;
  navValue: number;
  navDate: string;
}

/**
 * Function to fetch and parse mutual fund data from AMFI
 */
export async function fetchAMFIMutualFundData() {
  try {
    console.log('Starting AMFI data fetch...');
    
    // Fetch the NAV data file
    const response = await axios.get(AMFI_NAV_ALL_URL);
    const data = response.data;
    
    if (!data) {
      console.error('Failed to fetch data from AMFI');
      return { success: false, message: 'No data received from AMFI' };
    }
    
    console.log('Data received, parsing...');
    
    const lines = data.split('\n');
    
    let currentAMC = '';
    let currentSchemeType = '';
    const funds: ParsedFund[] = [];
    
    // Parse the text file line by line
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      // Skip empty lines
      if (!line) continue;
      
      // If line starts with a number, it's a fund entry
      if (/^\d/.test(line)) {
        const parts = line.split(';');
        
        if (parts.length >= 5) {
          const schemeCode = parts[0].trim();
          const isinDivPayout = parts[1].trim() || null;
          const isinDivReinvest = parts[2].trim() || null;
          const fundName = parts[3].trim();
          const navValue = parseFloat(parts[4].trim()) || 0;
          const navDate = parts[5]?.trim() || new Date().toISOString().split('T')[0];
          
          // Categorize fund based on AMC and scheme type
          const { category, subcategory } = categorizeFund(currentSchemeType, fundName);
          
          funds.push({
            schemeCode,
            isinDivPayout,
            isinDivReinvest,
            fundName,
            amcName: currentAMC,
            category,
            subcategory,
            navValue,
            navDate
          });
          
          if (funds.length % 100 === 0) {
            console.log(`Parsed ${funds.length} funds so far...`);
          }
        }
      } else if (line.includes('Mutual Fund')) {
        // This is an AMC name line
        currentAMC = line.replace('Mutual Fund', '').trim();
      } else if (/^[A-Za-z]/.test(line) && !line.includes(';')) {
        // This is a scheme type line (open-ended, close-ended, etc.)
        currentSchemeType = line.trim();
      }
    }
    
    console.log(`Parsing complete. Found ${funds.length} mutual funds.`);
    
    // Insert funds into database
    const result = await importFundsToDatabase(funds);
    
    return { 
      success: true, 
      message: `Successfully imported ${result.importedCount} funds out of ${funds.length}`,
      counts: {
        totalFunds: funds.length,
        importedFunds: result.importedCount
      }
    };
    
  } catch (error) {
    console.error('Error fetching AMFI data:', error);
    return { success: false, message: error.message || 'Unknown error occurred' };
  }
}

/**
 * Import the parsed funds to the database
 */
async function importFundsToDatabase(funds: ParsedFund[]) {
  console.log(`Importing ${funds.length} funds to database...`);
  
  let importedCount = 0;
  let errorCount = 0;
  
  try {
    // Truncate existing tables to start fresh - only if this is a complete rebuild
    // Don't do this in production unless you're sure!
    // await executeRawQuery('TRUNCATE TABLE funds, nav_data CASCADE');
    
    // Process funds in batches to avoid memory issues
    const batchSize = 100;
    for (let i = 0; i < funds.length; i += batchSize) {
      const batch = funds.slice(i, i + batchSize);
      
      for (const fund of batch) {
        try {
          // 1. Insert the fund
          const fundResult = await executeRawQuery(
            `INSERT INTO funds (
              scheme_code, isin_div_payout, isin_div_reinvest, fund_name, 
              amc_name, category, subcategory, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (scheme_code) DO UPDATE SET
              fund_name = EXCLUDED.fund_name,
              amc_name = EXCLUDED.amc_name,
              category = EXCLUDED.category,
              subcategory = EXCLUDED.subcategory,
              status = EXCLUDED.status
            RETURNING id`,
            [
              fund.schemeCode,
              fund.isinDivPayout,
              fund.isinDivReinvest,
              fund.fundName,
              fund.amcName,
              fund.category,
              fund.subcategory,
              'ACTIVE',
              new Date()
            ]
          );
          
          if (fundResult.rows && fundResult.rows.length > 0) {
            const fundId = fundResult.rows[0].id;
            
            // 2. Insert the NAV data
            await executeRawQuery(
              `INSERT INTO nav_data (
                fund_id, nav_date, nav_value, created_at
              ) VALUES ($1, $2, $3, $4)
              ON CONFLICT (fund_id, nav_date) DO UPDATE SET
                nav_value = EXCLUDED.nav_value`,
              [
                fundId,
                fund.navDate,
                fund.navValue,
                new Date()
              ]
            );
            
            importedCount++;
          }
        } catch (err) {
          console.error(`Error importing fund ${fund.schemeCode}:`, err);
          errorCount++;
        }
      }
      
      console.log(`Imported batch ${i/batchSize + 1}, progress: ${importedCount}/${funds.length}`);
    }
    
    console.log(`Import complete. Successfully imported ${importedCount} funds. Errors: ${errorCount}`);
    
    return { importedCount, errorCount };
  } catch (error) {
    console.error('Database import error:', error);
    throw error;
  }
}

/**
 * Categorize funds based on scheme type and name
 */
function categorizeFund(schemeType: string, fundName: string): { category: string, subcategory: string | null } {
  const nameLower = fundName.toLowerCase();
  let category = 'Other';
  let subcategory = null;
  
  // Check if it's an equity fund
  if (
    schemeType.includes('Equity Scheme') || 
    nameLower.includes('equity') || 
    nameLower.includes('nifty') || 
    nameLower.includes('sensex') ||
    nameLower.includes('growth') || 
    nameLower.includes('dividend') ||
    nameLower.includes('large cap') || 
    nameLower.includes('mid cap') || 
    nameLower.includes('small cap')
  ) {
    category = 'Equity';
    
    // Determine subcategory
    if (nameLower.includes('large cap')) {
      subcategory = 'Large Cap';
    } else if (nameLower.includes('mid cap')) {
      subcategory = 'Mid Cap';
    } else if (nameLower.includes('small cap')) {
      subcategory = 'Small Cap';
    } else if (nameLower.includes('multi cap') || nameLower.includes('multicap')) {
      subcategory = 'Multi Cap';
    } else if (nameLower.includes('elss') || nameLower.includes('tax saver')) {
      subcategory = 'ELSS';
    } else if (nameLower.includes('index') || nameLower.includes('nifty') || nameLower.includes('sensex')) {
      subcategory = 'Index';
    } else if (nameLower.includes('value')) {
      subcategory = 'Value';
    } else if (nameLower.includes('flexi cap') || nameLower.includes('flexicap')) {
      subcategory = 'Flexi Cap';
    } else if (nameLower.includes('focused')) {
      subcategory = 'Focused';
    } else if (nameLower.includes('sectoral') || nameLower.includes('thematic')) {
      subcategory = 'Sectoral/Thematic';
    }
  } 
  // Check if it's a debt fund
  else if (
    schemeType.includes('Debt Scheme') || 
    nameLower.includes('debt') || 
    nameLower.includes('income') || 
    nameLower.includes('bond') ||
    nameLower.includes('liquid') || 
    nameLower.includes('overnight') || 
    nameLower.includes('ultra short') ||
    nameLower.includes('credit risk') ||
    nameLower.includes('gilt')
  ) {
    category = 'Debt';
    
    // Determine subcategory
    if (nameLower.includes('liquid')) {
      subcategory = 'Liquid';
    } else if (nameLower.includes('overnight')) {
      subcategory = 'Overnight';
    } else if (nameLower.includes('ultra short')) {
      subcategory = 'Ultra Short Duration';
    } else if (nameLower.includes('short duration') || nameLower.includes('short term')) {
      subcategory = 'Short Duration';
    } else if (nameLower.includes('medium duration') || nameLower.includes('medium term')) {
      subcategory = 'Medium Duration';
    } else if (nameLower.includes('long duration') || nameLower.includes('long term')) {
      subcategory = 'Long Duration';
    } else if (nameLower.includes('corporate bond')) {
      subcategory = 'Corporate Bond';
    } else if (nameLower.includes('credit risk')) {
      subcategory = 'Credit Risk';
    } else if (nameLower.includes('banking') && nameLower.includes('psu')) {
      subcategory = 'Banking and PSU';
    } else if (nameLower.includes('gilt')) {
      subcategory = 'Gilt';
    } else if (nameLower.includes('dynamic bond')) {
      subcategory = 'Dynamic Bond';
    }
  } 
  // Check if it's a hybrid fund
  else if (
    schemeType.includes('Hybrid Scheme') || 
    nameLower.includes('hybrid') || 
    nameLower.includes('balanced') ||
    nameLower.includes('equity & debt') || 
    nameLower.includes('conservative') || 
    nameLower.includes('aggressive')
  ) {
    category = 'Hybrid';
    
    // Determine subcategory
    if (nameLower.includes('balanced advantage') || nameLower.includes('dynamic asset')) {
      subcategory = 'Balanced Advantage';
    } else if (nameLower.includes('aggressive') || (nameLower.includes('equity') && nameLower.includes('debt'))) {
      subcategory = 'Aggressive';
    } else if (nameLower.includes('conservative')) {
      subcategory = 'Conservative';
    } else if (nameLower.includes('multi asset')) {
      subcategory = 'Multi Asset';
    } else {
      subcategory = 'Balanced';
    }
  }
  
  return { category, subcategory };
}