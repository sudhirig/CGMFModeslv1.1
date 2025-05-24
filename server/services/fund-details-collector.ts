import axios from 'axios';
import * as cheerio from 'cheerio';
import { storage } from '../storage';
import { executeRawQuery } from '../db';

// Constants
const AMFI_FUND_DETAILS_BASE_URL = 'https://www.amfiindia.com/mutual-fund-details';
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36';

/**
 * Response type for enhanced fund details
 */
interface EnhancedFundDetails {
  schemeCode: string;
  fundName: string;
  inceptionDate?: Date | null;
  expenseRatio?: number | null;
  exitLoad?: string | null;
  benchmarkName?: string | null;
  minimumInvestment?: number | null;
  fundManager?: string | null;
  lockInPeriod?: number | null;
  amcURL?: string | null;
  fundDataSource?: string;
}

/**
 * Collects enhanced fund details beyond what basic NAV data provides
 */
export class FundDetailsCollector {
  private static instance: FundDetailsCollector;
  
  private constructor() {
    // Default constructor
  }
  
  public static getInstance(): FundDetailsCollector {
    if (!FundDetailsCollector.instance) {
      FundDetailsCollector.instance = new FundDetailsCollector();
    }
    return FundDetailsCollector.instance;
  }
  
  /**
   * Collect additional details for all funds or a specific set of funds
   * @param fundIds Optional specific fund IDs to collect details for
   */
  public async collectFundDetails(fundIds?: number[]): Promise<{ success: boolean, message: string, count: number }> {
    try {
      console.log('Starting collection of enhanced fund details...');
      
      // Log ETL operation start
      await storage.createETLRun({
        pipelineName: 'fund_details_collection',
        status: 'RUNNING',
        startTime: new Date()
      });
      
      // Get funds to process - either the specific funds requested or all funds
      const fundsToProcess = fundIds 
        ? await Promise.all(fundIds.map(id => storage.getFund(id)))
        : await storage.getAllFunds();
      
      // Filter out undefined funds
      const validFunds = fundsToProcess.filter(f => f !== undefined) as any[];
      
      console.log(`Found ${validFunds.length} funds to collect enhanced details for`);
      
      // Process in batches to avoid overwhelming the source
      const batchSize = 10;
      let successCount = 0;
      
      for (let i = 0; i < validFunds.length; i += batchSize) {
        const batch = validFunds.slice(i, i + batchSize);
        console.log(`Processing batch ${i / batchSize + 1}, funds ${i + 1}-${i + batch.length}`);
        
        // Process each fund in the batch concurrently
        const promises = batch.map(fund => this.processFund(fund));
        const results = await Promise.allSettled(promises);
        
        // Count successful updates
        results.forEach(result => {
          if (result.status === 'fulfilled' && result.value.success) {
            successCount++;
          }
        });
        
        // Add a small delay between batches to be nice to the server
        if (i + batchSize < validFunds.length) {
          await new Promise(resolve => setTimeout(resolve, 2000));
        }
      }
      
      // Log ETL operation completion
      await storage.createETLRun({
        pipelineName: 'fund_details_collection',
        status: 'COMPLETED',
        startTime: new Date(),
        endTime: new Date(),
        recordsProcessed: validFunds.length,
        errorMessage: `Successfully collected enhanced details for ${successCount} out of ${validFunds.length} funds`
      });
      
      return {
        success: true,
        message: `Successfully collected enhanced details for ${successCount} out of ${validFunds.length} funds`,
        count: successCount
      };
    } catch (error: any) {
      console.error('Error collecting fund details:', error);
      
      // Log ETL operation failure
      await storage.createETLRun({
        pipelineName: 'fund_details_collection',
        status: 'FAILED',
        startTime: new Date(),
        endTime: new Date(),
        errorMessage: `Error: ${error.message || 'Unknown error'}`
      });
      
      return {
        success: false,
        message: `Failed to collect fund details: ${error.message}`,
        count: 0
      };
    }
  }
  
  /**
   * Process an individual fund to collect additional details
   * @param fund The fund to process
   */
  private async processFund(fund: any): Promise<{ success: boolean; fund?: any }> {
    try {
      console.log(`Collecting details for ${fund.fundName} (${fund.schemeCode})`);
      
      // Try multiple sources to get the most complete data
      const detailsFromAMFI = await this.fetchDetailsFromAMFI(fund.schemeCode, fund.fundName);
      const detailsFromAMC = await this.fetchDetailsFromAMC(fund.schemeCode, fund.amcName, fund.fundName);
      
      // Merge details, preferring AMFI data but filling gaps with AMC data
      const enhancedDetails: any = {
        inceptionDate: detailsFromAMFI.inceptionDate || detailsFromAMC.inceptionDate,
        expenseRatio: detailsFromAMFI.expenseRatio || detailsFromAMC.expenseRatio,
        exitLoad: detailsFromAMFI.exitLoad || detailsFromAMC.exitLoad,
        benchmarkName: detailsFromAMFI.benchmarkName || detailsFromAMC.benchmarkName,
        minimumInvestment: detailsFromAMFI.minimumInvestment || detailsFromAMC.minimumInvestment,
        fundManager: detailsFromAMFI.fundManager || detailsFromAMC.fundManager,
        lockInPeriod: detailsFromAMFI.lockInPeriod || detailsFromAMC.lockInPeriod
      };
      
      // Remove undefined fields
      Object.keys(enhancedDetails).forEach(key => {
        if (enhancedDetails[key] === undefined) {
          delete enhancedDetails[key];
        }
      });
      
      // If we have any details, update the fund
      if (Object.keys(enhancedDetails).length > 0) {
        const updatedFund = await storage.updateFund(fund.id, enhancedDetails);
        console.log(`Updated fund ${fund.id} with enhanced details`);
        return { success: true, fund: updatedFund };
      } else {
        console.log(`No additional details found for fund ${fund.id}`);
        return { success: false };
      }
    } catch (error) {
      console.error(`Error processing fund ${fund.id}:`, error);
      return { success: false };
    }
  }
  
  /**
   * Fetch fund details from AMFI website
   * This uses web scraping to extract data from the fund details page
   */
  private async fetchDetailsFromAMFI(schemeCode: string, fundName: string): Promise<EnhancedFundDetails> {
    try {
      // Create a base result with identification info
      const result: EnhancedFundDetails = {
        schemeCode,
        fundName,
        fundDataSource: 'AMFI'
      };
      
      // Construct the URL for the fund's details page
      const url = `${AMFI_FUND_DETAILS_BASE_URL}?symbol=${schemeCode}`;
      
      // Fetch the page with a proper user agent
      const response = await axios.get(url, {
        headers: {
          'User-Agent': USER_AGENT
        },
        timeout: 10000 // 10 second timeout
      });
      
      if (!response.data) {
        return result;
      }
      
      // Parse the HTML
      const $ = cheerio.load(response.data);
      
      // Extract details from the page - the exact CSS selectors may need adjustment
      // based on the actual structure of the AMFI website
      
      // Inception Date
      const inceptionDateText = $('.fund-details .inception-date').text().trim();
      if (inceptionDateText) {
        const dateParts = inceptionDateText.split('/');
        if (dateParts.length === 3) {
          // Format is usually DD/MM/YYYY
          result.inceptionDate = new Date(`${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`);
        }
      }
      
      // Expense Ratio
      const expenseRatioText = $('.fund-details .expense-ratio').text().trim().replace('%', '');
      if (expenseRatioText && !isNaN(parseFloat(expenseRatioText))) {
        result.expenseRatio = parseFloat(expenseRatioText);
      }
      
      // Exit Load
      const exitLoadText = $('.fund-details .exit-load').text().trim();
      if (exitLoadText) {
        result.exitLoad = exitLoadText;
      }
      
      // Benchmark
      const benchmarkText = $('.fund-details .benchmark').text().trim();
      if (benchmarkText) {
        result.benchmarkName = benchmarkText;
      }
      
      // Minimum Investment
      const minInvestmentText = $('.fund-details .min-investment').text().trim().replace(/[₹,]/g, '');
      if (minInvestmentText && !isNaN(parseInt(minInvestmentText))) {
        result.minimumInvestment = parseInt(minInvestmentText);
      }
      
      // Fund Manager
      const fundManagerText = $('.fund-details .fund-manager').text().trim();
      if (fundManagerText) {
        result.fundManager = fundManagerText;
      }
      
      // Lock-in Period
      const lockInText = $('.fund-details .lock-in').text().trim().replace(/[^0-9]/g, '');
      if (lockInText && !isNaN(parseInt(lockInText))) {
        result.lockInPeriod = parseInt(lockInText);
      }
      
      return result;
    } catch (error) {
      console.warn(`Error fetching AMFI details for scheme ${schemeCode}:`, error);
      return {
        schemeCode,
        fundName,
        fundDataSource: 'AMFI_ERROR'
      };
    }
  }
  
  /**
   * Fetch fund details from the AMC's website
   * This is a fallback to get data that might not be available on AMFI
   */
  private async fetchDetailsFromAMC(schemeCode: string, amcName: string, fundName: string): Promise<EnhancedFundDetails> {
    // This is a placeholder for potential future implementation
    // We'd need to create specific scrapers for each AMC's website format
    return {
      schemeCode,
      fundName,
      fundDataSource: 'AMC'
    };
  }
  
  /**
   * Start a scheduled fund details collection job
   * @param intervalHours How often to run the collection (in hours)
   */
  public startScheduledDetailsFetch(intervalHours: number = 168): void { // Default weekly
    console.log(`Starting scheduled fund details collection every ${intervalHours} hours`);
    
    // Convert hours to milliseconds
    const intervalMs = intervalHours * 60 * 60 * 1000;
    
    // Set up the interval
    setInterval(async () => {
      console.log('Running scheduled fund details collection...');
      try {
        const result = await this.collectFundDetails();
        console.log('Scheduled fund details collection completed:', result);
      } catch (error) {
        console.error('Error in scheduled fund details collection:', error);
      }
    }, intervalMs);
  }
}

// Export a singleton instance
export const fundDetailsCollector = FundDetailsCollector.getInstance();