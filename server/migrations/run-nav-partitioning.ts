import { pool } from '../db';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function runNavPartitioning() {
  const client = await pool.connect();
  
  try {
    console.log('🔧 Starting NAV data partitioning migration...');
    
    // Read the SQL migration file
    const migrationSQL = fs.readFileSync(
      path.join(__dirname, 'partition-nav-data.sql'),
      'utf8'
    );
    
    // Split the SQL into individual statements
    const statements = migrationSQL
      .split(/;\s*$(?=(?:[^']*'[^']*')*[^']*$)/m)
      .filter(stmt => stmt.trim().length > 0);
    
    // Execute each statement
    for (let i = 0; i < statements.length; i++) {
      const statement = statements[i].trim();
      
      // Skip comment-only statements
      if (statement.startsWith('--') && !statement.includes('\n')) {
        continue;
      }
      
      console.log(`Executing statement ${i + 1}/${statements.length}...`);
      
      try {
        await client.query(statement);
      } catch (error) {
        console.error(`Error executing statement ${i + 1}:`, error.message);
        // Continue with other statements if one fails
      }
    }
    
    // Verify the migration
    const verificationResult = await client.query(`
      SELECT 
        'nav_data' as table_name,
        COUNT(*) as row_count,
        pg_size_pretty(pg_total_relation_size('nav_data')) as size
      FROM nav_data
      UNION ALL
      SELECT 
        'nav_data_partitioned' as table_name,
        COUNT(*) as row_count,
        pg_size_pretty(pg_total_relation_size('nav_data_partitioned')) as size
      FROM nav_data_partitioned
    `);
    
    console.log('📊 Migration Results:');
    verificationResult.rows.forEach(row => {
      console.log(`${row.table_name}: ${row.row_count} rows, ${row.size}`);
    });
    
    // Check if row counts match
    const originalCount = verificationResult.rows.find(r => r.table_name === 'nav_data')?.row_count || 0;
    const partitionedCount = verificationResult.rows.find(r => r.table_name === 'nav_data_partitioned')?.row_count || 0;
    
    if (originalCount === partitionedCount && partitionedCount > 0) {
      console.log('✅ Migration successful! Row counts match.');
      console.log('⚠️  To complete the migration, manually run:');
      console.log('    ALTER TABLE nav_data RENAME TO nav_data_old;');
      console.log('    ALTER TABLE nav_data_partitioned RENAME TO nav_data;');
    } else {
      console.log('❌ Migration incomplete. Row counts do not match.');
    }
    
    // Show partition information
    const partitionInfo = await client.query(`
      SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
      FROM pg_tables 
      WHERE tablename LIKE 'nav_data_y%'
      ORDER BY tablename
      LIMIT 10
    `);
    
    console.log('\n📁 Sample partitions created:');
    partitionInfo.rows.forEach(row => {
      console.log(`  ${row.tablename}: ${row.size}`);
    });
    
    return { success: true, originalCount, partitionedCount };
    
  } catch (error) {
    console.error('❌ Migration failed:', error);
    return { success: false, error: error.message };
  } finally {
    client.release();
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runNavPartitioning()
    .then(result => {
      console.log('Migration completed:', result);
      process.exit(result.success ? 0 : 1);
    })
    .catch(error => {
      console.error('Migration error:', error);
      process.exit(1);
    });
}