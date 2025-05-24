// Script to run the metrics migration using ES modules
import { exec } from 'child_process';

console.log('Compiling and running the detailed metrics migration...');
console.log('This will add transparency metrics to fund_scores table.');

// Execute the migration with tsx (TypeScript executor)
exec('npx tsx server/migrations/add-detailed-metrics.ts', (error, stdout, stderr) => {
  if (error) {
    console.error('Migration execution failed:', error);
    return;
  }
  
  if (stderr) {
    console.error('Migration stderr:', stderr);
  }
  
  console.log('Migration output:');
  console.log(stdout);
  console.log('Migration completed. Fund scores now have detailed metrics for complete transparency.');
});