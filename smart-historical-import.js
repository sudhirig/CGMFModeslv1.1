/**
 * Smart Historical Import Script
 * Uses successful scheme code patterns to import more authentic NAV data
 */

import { db } from './server/db.ts';
import { funds, navData } from './shared/schema.ts';
import { eq, sql, and } from 'drizzle-orm';
import axios from 'axios';

async function smartHistoricalImport() {
  console.log('🎯 Starting smart historical import using successful patterns...');
  
  try {
    // Get funds that need more data, focusing on successful scheme code ranges
    const fundsNeedingData = await db
      .select({
        id: funds.id,
        schemeCode: funds.schemeCode,
        fundName: funds.fundName,
        category: funds.category
      })
      .from(funds)
      .leftJoin(
        sql`(SELECT fund_id, COUNT(*) as record_count FROM nav_data GROUP BY fund_id) nav_counts`,
        sql`nav_counts.fund_id = ${funds.id}`
      )
      .where(
        and(
          eq(funds.status, 'ACTIVE'),
          sql`COALESCE(nav_counts.record_count, 0) < 100`,
          sql`${funds.schemeCode} IS NOT NULL`,
          sql`${funds.schemeCode} ~ '^[0-9]+$'`,
          // Focus on scheme codes where we know MFAPI.in has data
          sql`${funds.schemeCode}::INTEGER BETWEEN 100000 AND 119000`
        )
      )
      .orderBy(sql`RANDOM()`)
      .limit(50);

    console.log(`📊 Found ${fundsNeedingData.length} funds needing more data in successful ranges`);

    let totalImported = 0;
    let successfulFunds = 0;

    for (const fund of fundsNeedingData) {
      console.log(`\n📈 Processing ${fund.fundName} (${fund.schemeCode})`);
      
      try {
        // Use the all-time API endpoint that worked successfully before
        const url = `https://api.mfapi.in/mf/${fund.schemeCode}`;
        
        const response = await axios.get(url, {
          timeout: 10000,
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          }
        });

        if (response.data && response.data.data && response.data.data.length > 0) {
          const navRecords = response.data.data
            .filter(entry => entry.date && entry.nav && !isNaN(parseFloat(entry.nav)))
            .map(entry => ({
              fundId: fund.id,
              navDate: entry.date,
              navValue: parseFloat(entry.nav).toFixed(4),
              createdAt: new Date()
            }));

          if (navRecords.length > 0) {
            // Insert new records (ignore duplicates)
            await db.insert(navData)
              .values(navRecords)
              .onConflictDoNothing();

            totalImported += navRecords.length;
            successfulFunds++;
            
            console.log(`✅ Imported ${navRecords.length} records for ${fund.fundName}`);
          } else {
            console.log(`⚠️ No valid NAV data for ${fund.fundName}`);
          }
        } else {
          console.log(`❌ No data available for ${fund.fundName}`);
        }

        // Small delay to be respectful to the API
        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (error) {
        console.log(`❌ Error processing ${fund.fundName}: ${error.message}`);
        continue;
      }
    }

    console.log(`\n🎯 Smart import completed!`);
    console.log(`📊 Successfully processed: ${successfulFunds} funds`);
    console.log(`📈 Total records imported: ${totalImported}`);
    
    // Get updated totals
    const totalResult = await db
      .select({ count: sql`COUNT(*)` })
      .from(navData);
    
    console.log(`💾 Total NAV records in system: ${totalResult[0].count}`);

  } catch (error) {
    console.error('❌ Smart import failed:', error);
  }
}

smartHistoricalImport();