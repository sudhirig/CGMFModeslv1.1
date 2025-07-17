import { Request, Response, NextFunction } from 'express';
import { cache } from '../services/redis-cache';

interface CacheOptions {
  ttl?: number;
  keyBuilder?: (req: Request) => string;
  condition?: (req: Request) => boolean;
}

/**
 * Express middleware for caching API responses
 */
export function cacheMiddleware(options: CacheOptions = {}) {
  return async (req: Request, res: Response, next: NextFunction) => {
    // Skip caching for non-GET requests
    if (req.method !== 'GET') {
      return next();
    }
    
    // Check if caching should be applied based on condition
    if (options.condition && !options.condition(req)) {
      return next();
    }
    
    // Build cache key
    const cacheKey = options.keyBuilder 
      ? options.keyBuilder(req)
      : `api:${req.originalUrl}`;
    
    try {
      // Check if Redis is connected
      if (!cache.isConnected()) {
        console.log('Redis not connected, skipping cache');
        return next();
      }
      
      // Try to get from cache
      const cachedData = await cache.get(cacheKey);
      
      if (cachedData) {
        // Cache hit
        res.setHeader('X-Cache', 'HIT');
        res.setHeader('X-Cache-Key', cacheKey);
        return res.json(cachedData);
      }
      
      // Cache miss - intercept response to cache it
      const originalJson = res.json;
      
      res.json = function(data: any) {
        res.json = originalJson;
        
        // Cache the response if successful
        if (res.statusCode >= 200 && res.statusCode < 300) {
          cache.set(cacheKey, data, options.ttl).catch(err => {
            console.error('Failed to cache response:', err);
          });
        }
        
        res.setHeader('X-Cache', 'MISS');
        res.setHeader('X-Cache-Key', cacheKey);
        
        return originalJson.call(this, data);
      };
      
      next();
      
    } catch (error) {
      console.error('Cache middleware error:', error);
      // Continue without caching on error
      next();
    }
  };
}

/**
 * Invalidate cache middleware for mutation operations
 */
export function invalidateCacheMiddleware(pattern: string | ((req: Request) => string)) {
  return async (req: Request, res: Response, next: NextFunction) => {
    // Intercept response to invalidate cache after successful mutation
    const originalJson = res.json;
    
    res.json = function(data: any) {
      res.json = originalJson;
      
      // Invalidate cache if request was successful
      if (res.statusCode >= 200 && res.statusCode < 300) {
        const cachePattern = typeof pattern === 'function' ? pattern(req) : pattern;
        
        cache.deletePattern(cachePattern).catch(err => {
          console.error('Failed to invalidate cache:', err);
        });
      }
      
      return originalJson.call(this, data);
    };
    
    next();
  };
}