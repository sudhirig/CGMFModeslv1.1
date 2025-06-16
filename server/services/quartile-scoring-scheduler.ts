import { fundScoringEngine } from './batch-quartile-scoring';
import { storage } from '../storage';
import { InsertEtlPipelineRun } from '../../shared/schema';

export class QuartileScheduler {
  private isRunning: boolean = false;
  
  constructor() {
    // Nothing to initialize
  }
  
  startScheduler(intervalDays: number = 7) {
    console.log(`⚠️ Quartile scoring scheduler DISABLED to prevent database corruption`);
    console.log(`Manual triggers available via API endpoints for controlled execution`);
  }
  
  private async runBatchProcess() {
    if (this.isRunning) {
      console.log('A quartile scoring job is already running, skipping this run');
      return;
    }
    
    let etlRunId: number | null = null;
    
    try {
      this.isRunning = true;
      
      // Log the start of the ETL operation
      const etlRun: InsertEtlPipelineRun = {
        pipelineName: 'Quartile Scoring',
        status: 'RUNNING',
        startTime: new Date(),
        recordsProcessed: 0,
        errorMessage: 'Starting quartile scoring process'
      };
      
      const etlResult = await storage.createETLRun(etlRun);
      etlRunId = etlResult.id;
      
      console.log(`Starting quartile scoring batch process (ETL run ID: ${etlRunId})`);
      
      // Process funds in batches of 500 until all are processed
      let totalProcessed = 0;
      let batchCount = 0;
      let batchResult;
      const batchSize = 500;
      
      do {
        batchCount++;
        console.log(`Processing batch ${batchCount}`);
        
        batchResult = await fundScoringEngine.batchScoreFunds(batchSize);
        totalProcessed += batchResult.processed;
        
        // Update the ETL status
        if (etlRunId) {
          await storage.updateETLRun(etlRunId, {
            status: 'RUNNING',
            recordsProcessed: totalProcessed,
            errorMessage: `Processed ${totalProcessed} funds so far`
          });
        }
        
        // Add a small delay between batches
        if (batchResult.processed > 0) {
          await new Promise(resolve => setTimeout(resolve, 5000));
        }
      } while (batchResult.processed === batchSize); // Continue if we processed a full batch
      
      // After all individual funds are scored, assign quartiles across categories
      await fundScoringEngine.assignQuartiles();
      
      // Update the ETL status to completed
      if (etlRunId) {
        await storage.updateETLRun(etlRunId, {
          status: 'COMPLETED',
          endTime: new Date(),
          recordsProcessed: totalProcessed,
          errorMessage: `Successfully scored and assigned quartiles for ${totalProcessed} funds`
        });
      }
      
      console.log(`Completed quartile scoring. Processed ${totalProcessed} funds.`);
    } catch (error: any) {
      console.error('Error in batch quartile scoring:', error);
      
      // Log the error
      if (etlRunId) {
        await storage.updateETLRun(etlRunId, {
          status: 'ERROR',
          endTime: new Date(),
          errorMessage: `Error in quartile scoring: ${error.message || 'Unknown error'}`
        });
      }
    } finally {
      this.isRunning = false;
    }
  }
  
  // Method to trigger a manual run
  async triggerManualRun(category?: string) {
    if (this.isRunning) {
      return {
        success: false,
        message: 'A quartile scoring job is already running'
      };
    }
    
    // Start a new run in the background
    setTimeout(() => this.runBatchProcess(), 0);
    
    return {
      success: true,
      message: 'Manual quartile scoring job started'
    };
  }
}

export const quartileScheduler = new QuartileScheduler();