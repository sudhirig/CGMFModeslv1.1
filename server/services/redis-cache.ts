import Redis from 'ioredis';

// Flag to track Redis availability
let redisAvailable = false;

// Create Redis client with error handling
const redis = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  maxRetriesPerRequest: 1,
  retryStrategy: (times: number) => {
    if (times > 1) {
      // Stop retrying after 1 attempt
      redisAvailable = false;
      console.log('⚠️  Redis not available, caching disabled');
      return null;
    }
    return 1000; // Retry after 1 second
  },
  enableOfflineQueue: false,
  lazyConnect: true, // Don't connect immediately
  showFriendlyErrorStack: true,
  enableReadyCheck: true,
  connectTimeout: 5000,
});

// Try to connect
redis.connect().then(() => {
  redisAvailable = true;
}).catch((err) => {
  console.log('⚠️  Redis not available, caching disabled:', err.message);
  redisAvailable = false;
});

// Handle connection events
redis.on('connect', () => {
  console.log('✅ Redis connected successfully');
  redisAvailable = true;
});

redis.on('error', (err) => {
  // Only log error once when Redis becomes unavailable
  if (redisAvailable) {
    console.error('❌ Redis connection error:', err.message);
    redisAvailable = false;
  }
});

redis.on('close', () => {
  console.log('Redis connection closed');
  redisAvailable = false;
});

// Cache configuration
const CACHE_CONFIGS = {
  // High-frequency reads with stable data
  MARKET_INDICES: { ttl: 300 }, // 5 minutes
  TOP_FUNDS: { ttl: 900 }, // 15 minutes
  DASHBOARD_STATS: { ttl: 600 }, // 10 minutes
  ELIVATE_SCORE: { ttl: 1800 }, // 30 minutes
  
  // Medium frequency with moderate stability
  FUND_DETAILS: { ttl: 3600 }, // 1 hour
  QUARTILE_ANALYSIS: { ttl: 3600 }, // 1 hour
  PORTFOLIO_MODEL: { ttl: 1800 }, // 30 minutes
  
  // Low frequency with high stability
  FUND_LIST: { ttl: 7200 }, // 2 hours
  HISTORICAL_NAV: { ttl: 86400 }, // 24 hours
} as const;

type CacheKey = keyof typeof CACHE_CONFIGS;

export class RedisCache {
  private static instance: RedisCache;
  
  static getInstance(): RedisCache {
    if (!RedisCache.instance) {
      RedisCache.instance = new RedisCache();
    }
    return RedisCache.instance;
  }
  
  /**
   * Get value from cache
   */
  async get(key: string): Promise<any> {
    if (!redisAvailable) return null;
    
    try {
      const value = await redis.get(key);
      if (value) {
        return JSON.parse(value);
      }
      return null;
    } catch (error) {
      console.error(`Cache get error for key ${key}:`, error);
      redisAvailable = false;
      return null;
    }
  }
  
  /**
   * Set value in cache with TTL
   */
  async set(key: string, value: any, ttl?: number): Promise<void> {
    if (!redisAvailable) return;
    
    try {
      const serialized = JSON.stringify(value);
      if (ttl) {
        await redis.setex(key, ttl, serialized);
      } else {
        await redis.set(key, serialized);
      }
    } catch (error) {
      console.error(`Cache set error for key ${key}:`, error);
      redisAvailable = false;
    }
  }
  
  /**
   * Delete specific key from cache
   */
  async delete(key: string): Promise<void> {
    try {
      await redis.del(key);
    } catch (error) {
      console.error(`Cache delete error for key ${key}:`, error);
    }
  }
  
  /**
   * Delete all keys matching a pattern
   */
  async deletePattern(pattern: string): Promise<void> {
    try {
      const keys = await redis.keys(pattern);
      if (keys.length > 0) {
        await redis.del(...keys);
      }
    } catch (error) {
      console.error(`Cache delete pattern error for ${pattern}:`, error);
    }
  }
  
  /**
   * Get cache configuration for a specific key type
   */
  getCacheConfig(type: CacheKey): typeof CACHE_CONFIGS[CacheKey] {
    return CACHE_CONFIGS[type];
  }
  
  /**
   * Cache with specific configuration
   */
  async cacheWithConfig(type: CacheKey, key: string, value: any): Promise<void> {
    const config = this.getCacheConfig(type);
    await this.set(key, value, config.ttl);
  }
  
  /**
   * Invalidate related caches when data changes
   */
  async invalidateRelated(entityType: string, entityId?: string): Promise<void> {
    switch (entityType) {
      case 'fund':
        // Invalidate fund-specific caches
        if (entityId) {
          await this.deletePattern(`fund:${entityId}:*`);
        }
        // Also invalidate aggregated data
        await this.deletePattern('stats:*');
        await this.deletePattern('top-funds:*');
        break;
        
      case 'nav':
        // Invalidate NAV-related caches
        if (entityId) {
          await this.deletePattern(`nav:${entityId}:*`);
        }
        await this.deletePattern('market-indices:*');
        break;
        
      case 'score':
        // Invalidate scoring-related caches
        await this.deletePattern('elivate:*');
        await this.deletePattern('quartile:*');
        await this.deletePattern('top-funds:*');
        break;
        
      default:
        // Invalidate all caches if entity type unknown
        await redis.flushdb();
    }
  }
  
  /**
   * Check if Redis is connected
   */
  isConnected(): boolean {
    return redisAvailable && redis.status === 'ready';
  }
  
  /**
   * Close Redis connection
   */
  async close(): Promise<void> {
    await redis.quit();
  }
}

// Export singleton instance
export const cache = RedisCache.getInstance();

// Export cache key builders for consistency
export const cacheKeys = {
  marketIndices: () => 'market-indices:all',
  marketIndex: (name: string) => `market-index:${name}`,
  topFunds: (limit: number = 10) => `top-funds:${limit}`,
  dashboardStats: () => 'stats:dashboard',
  elivateScore: () => 'elivate:score',
  fundDetails: (fundId: number) => `fund:${fundId}:details`,
  fundNAV: (fundId: number, range: string) => `nav:${fundId}:${range}`,
  quartileAnalysis: (category?: string) => `quartile:${category || 'all'}`,
  portfolioModel: (riskProfile: string) => `portfolio:${riskProfile}`,
  fundList: (page: number, limit: number) => `funds:page:${page}:limit:${limit}`,
};