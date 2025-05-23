import { Pool, neonConfig } from '@neondatabase/serverless';
import { drizzle } from 'drizzle-orm/neon-serverless';
import ws from "ws";
import * as schema from "@shared/schema";

neonConfig.webSocketConstructor = ws;

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL must be set. Did you forget to provision a database?",
  );
}

// Export the pool connection for direct use
export const pool = new Pool({ connectionString: process.env.DATABASE_URL });
export const db = drizzle(pool, { schema });

// Helper function to execute raw SQL with proper parameter handling
export async function executeRawQuery(sql: string, params: any[] = []) {
  try {
    const result = await pool.query(sql, params);
    return result;
  } catch (error) {
    console.error("Database query error:", error);
    throw error;
  }
}
