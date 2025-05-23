// Advanced AMFI Data Scraper to fetch real mutual fund data
import axios from 'axios';
import { executeRawQuery } from './db';

// Real AMFI URL for production use
const AMFI_NAV_ALL_URL = 'https://www.amfiindia.com/spages/NAVAll.txt';

// Since external API calls might be limited in this environment,
// we'll generate a comprehensive mutual fund dataset

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
 * Generate and import a comprehensive set of mutual fund data
 */
export async function fetchAMFIMutualFundData() {
  try {
    console.log('Starting real AMFI mutual fund data import...');
    
    // Fetch real mutual fund data from AMFI
    console.log(`Fetching data from ${AMFI_NAV_ALL_URL}...`);
    const response = await axios.get(AMFI_NAV_ALL_URL);
    
    if (!response.data) {
      throw new Error('No data received from AMFI');
    }
    
    // Parse the raw text content from AMFI
    const navText = response.data;
    const funds: ParsedFund[] = [];
    
    console.log('Parsing AMFI data...');
    
    // Split by lines
    const lines = navText.split('\n');
    
    let currentAMC = '';
    let currentSchemeType = '';
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      if (line.length === 0) continue;
      
      // Check if line contains AMC name
      if (line.includes('Mutual Fund') && !line.includes(';')) {
        currentAMC = line;
        continue;
      }
      
      // Check if line contains scheme type
      if (line.includes('Schemes') && !line.includes(';')) {
        currentSchemeType = line;
        continue;
      }
      
      // Parse fund data line (semicolon separated)
      if (line.includes(';')) {
        const parts = line.split(';');
        
        if (parts.length >= 5) {
          const schemeCode = parts[0].trim();
          const fundName = parts[3].trim();
          const isinDivPayout = parts[1].trim() || null;
          const isinDivReinvest = parts[2].trim() || null;
          
          // Convert NAV string to number, handling commas and invalid values
          let navValueStr = parts[4].trim().replace(/,/g, '');
          const navValue = navValueStr && !isNaN(parseFloat(navValueStr)) 
            ? parseFloat(navValueStr) 
            : 0;
          
          // Get date from the next part if available
          const navDate = parts.length >= 6 ? parts[5].trim() : new Date().toISOString().split('T')[0];
          
          // Categorize the fund
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
          
          if (funds.length % 250 === 0) {
            console.log(`Parsed ${funds.length} funds so far...`);
          }
        }
      }
    }
    
    console.log(`Parsing complete. Found ${funds.length} mutual funds from AMFI.`);
    
    // Insert funds into database
    const result = await importFundsToDatabase(funds);
    
    return { 
      success: true, 
      message: `Successfully imported ${result.importedCount} mutual funds from AMFI`,
      counts: {
        totalFunds: funds.length,
        importedFunds: result.importedCount
      }
    };
    
  } catch (error: any) {
    console.error('Error fetching AMFI mutual fund data:', error);
    return { 
      success: false, 
      message: error.message || 'Unknown error occurred during AMFI data fetch',
      error: String(error)
    };
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
    // Clear existing data to ensure a fresh start
    console.log("Clearing existing fund data...");
    try {
      await executeRawQuery('DELETE FROM nav_data');
      await executeRawQuery('DELETE FROM funds');
      console.log("Successfully cleared existing data");
    } catch (clearError) {
      console.error("Error clearing existing data:", clearError);
      // Continue anyway - we'll use the ON CONFLICT clauses to handle duplicates
    }
    
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