import { Pool, neonConfig } from '@neondatabase/serverless';
import { drizzle } from 'drizzle-orm/neon-serverless';
import ws from "ws";
import * as schema from "@shared/schema";

neonConfig.webSocketConstructor = ws;

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL must be set. Did you forget to provision a database?");
  process.exit(1);
}

// Export the pool connection for direct use with enhanced error handling
export const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 5000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 0
});

// Add comprehensive pool error handling to prevent crashes
pool.on('error', (err) => {
  console.error('Database pool error (handled):', err.message);
  // Don't exit the process, just log the error
});

pool.on('connect', () => {
  console.log('Database pool connection established');
});

pool.on('remove', () => {
  console.log('Database pool connection removed');
});

export const db = drizzle(pool, { schema });

// Enhanced helper function with comprehensive error handling
export async function executeRawQuery(sql: string, params: any[] = []) {
  const maxRetries = 3;
  let attempt = 0;
  
  while (attempt < maxRetries) {
    try {
      const result = await pool.query(sql, params);
      return result;
    } catch (error: any) {
      attempt++;
      console.error(`Database query error (attempt ${attempt}/${maxRetries}):`, error.message);
      
      // Handle specific error types
      if (error.code === 'ECONNREFUSED' || error.code === 'ENOTFOUND') {
        console.error('Database connection refused - check if database is running');
      } else if (error.code === '42P01') {
        console.error('Table does not exist:', error.message);
      } else if (error.code === '42703') {
        console.error('Column does not exist:', error.message);
      }
      
      // If this was the last attempt, throw the error
      if (attempt >= maxRetries) {
        throw new Error(`Database query failed after ${maxRetries} attempts: ${error.message}`);
      }
      
      // Wait before retrying (exponential backoff)
      await new Promise(resolve => setTimeout(resolve, Math.pow(2, attempt) * 1000));
    }
  }
}

// Database health check function
export async function checkDatabaseHealth(): Promise<boolean> {
  try {
    await executeRawQuery('SELECT 1 as health_check');
    return true;
  } catch (error) {
    console.error('Database health check failed:', error);
    return false;
  }
}

// Track if database is already closed to prevent multiple calls
let isDatabaseClosed = false;

// Graceful shutdown handler
export async function closeDatabase(): Promise<void> {
  if (isDatabaseClosed) {
    console.log('Database pool already closed');
    return;
  }
  
  try {
    isDatabaseClosed = true;
    await pool.end();
    console.log('Database pool closed gracefully');
  } catch (error) {
    console.error('Error closing database pool:', error);
  }
}
