// Script to apply the detailed metrics migration
const runMigration = require('./server/migrations/add-detailed-metrics').default;

async function applyMetricsMigration() {
  try {
    console.log('Applying detailed metrics migration to fund_scores table...');
    await runMigration();
    console.log('Migration completed successfully!');
    console.log('The fund_scores table now has detailed metrics for complete transparency.');
  } catch (error) {
    console.error('Error applying migration:', error);
  }
}

// Run the migration
applyMetricsMigration();