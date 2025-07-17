import { pool } from '../db';

export async function testNavPartitioning() {
  const client = await pool.connect();
  
  try {
    console.log('🧪 Testing NAV partitioning setup...');
    
    // Step 1: Create a test partitioned table
    await client.query(`
      CREATE TABLE IF NOT EXISTS nav_data_test_partitioned (
        fund_id INTEGER,
        nav_date DATE NOT NULL,
        nav_value DECIMAL(12,4) NOT NULL,
        nav_change DECIMAL(12,4),
        nav_change_pct DECIMAL(8,4),
        aum_cr DECIMAL(15,2),
        created_at TIMESTAMP DEFAULT NOW()
      ) PARTITION BY RANGE (nav_date)
    `);
    
    console.log('✅ Created test partitioned table');
    
    // Step 2: Create a few test partitions
    const testPartitions = [
      { year: 2025, month: 1 },
      { year: 2025, month: 2 },
      { year: 2025, month: 3 }
    ];
    
    for (const { year, month } of testPartitions) {
      const partitionName = `nav_data_test_y${year}m${String(month).padStart(2, '0')}`;
      const startDate = `${year}-${String(month).padStart(2, '0')}-01`;
      const endDate = `${year}-${String(month + 1).padStart(2, '0')}-01`;
      
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${partitionName} 
        PARTITION OF nav_data_test_partitioned 
        FOR VALUES FROM ('${startDate}') TO ('${endDate}')
      `);
      
      console.log(`✅ Created partition ${partitionName}`);
    }
    
    // Step 3: Insert test data
    console.log('📝 Inserting test data...');
    
    // Copy a small sample from the original table
    const insertResult = await client.query(`
      INSERT INTO nav_data_test_partitioned 
      SELECT fund_id, nav_date, nav_value, nav_change, nav_change_pct, aum_cr, created_at
      FROM nav_data
      WHERE nav_date >= '2025-01-01' AND nav_date < '2025-04-01'
      AND fund_id IN (SELECT id FROM funds LIMIT 100)
      LIMIT 10000
    `);
    
    console.log(`✅ Inserted ${insertResult.rowCount} test rows`);
    
    // Step 4: Test query performance
    console.log('⚡ Testing query performance...');
    
    // Test 1: Query without partitioning
    const start1 = Date.now();
    const result1 = await client.query(`
      SELECT COUNT(*) FROM nav_data
      WHERE nav_date BETWEEN '2025-01-01' AND '2025-03-31'
      AND fund_id = 10061
    `);
    const time1 = Date.now() - start1;
    
    // Test 2: Query with partitioning
    const start2 = Date.now();
    const result2 = await client.query(`
      SELECT COUNT(*) FROM nav_data_test_partitioned
      WHERE nav_date BETWEEN '2025-01-01' AND '2025-03-31'
      AND fund_id IN (SELECT id FROM funds LIMIT 100)
    `);
    const time2 = Date.now() - start2;
    
    console.log(`📊 Performance comparison:`);
    console.log(`   Original table: ${time1}ms (${result1.rows[0].count} rows)`);
    console.log(`   Partitioned table: ${time2}ms (${result2.rows[0].count} rows)`);
    console.log(`   Improvement: ${((time1 - time2) / time1 * 100).toFixed(1)}%`);
    
    // Step 5: Verify partition pruning
    const explainResult = await client.query(`
      EXPLAIN (ANALYZE, BUFFERS) 
      SELECT * FROM nav_data_test_partitioned
      WHERE nav_date = '2025-02-15'
      AND fund_id = 10061
    `);
    
    console.log('\n🔍 Query plan (checking partition pruning):');
    explainResult.rows.forEach(row => {
      if (row['QUERY PLAN'].includes('Partitions')) {
        console.log(`   ${row['QUERY PLAN']}`);
      }
    });
    
    // Cleanup
    await client.query('DROP TABLE nav_data_test_partitioned CASCADE');
    console.log('\n🧹 Cleaned up test tables');
    
    return {
      success: true,
      originalQueryTime: time1,
      partitionedQueryTime: time2,
      improvement: ((time1 - time2) / time1 * 100).toFixed(1)
    };
    
  } catch (error) {
    console.error('❌ Test failed:', error);
    
    // Cleanup on error
    try {
      await client.query('DROP TABLE IF EXISTS nav_data_test_partitioned CASCADE');
    } catch (cleanupError) {
      // Ignore cleanup errors
    }
    
    return { success: false, error: error.message };
  } finally {
    client.release();
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  testNavPartitioning()
    .then(result => {
      console.log('\nTest completed:', result);
      process.exit(result.success ? 0 : 1);
    })
    .catch(error => {
      console.error('Test error:', error);
      process.exit(1);
    });
}