"""
Cache Service - OwnLens
========================

Cache service for OwnLens.
Provides comprehensive caching and performance optimization.

This service provides:
- Multi-level caching (memory, Redis, file)
- Cache invalidation strategies
- Performance optimization through intelligent caching
- Cache statistics and monitoring
- Distributed caching support
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import logging
import asyncio
import pickle
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class CacheLevel(Enum):
    """Cache levels for multi-level caching."""
    MEMORY = "memory"
    REDIS = "redis"
    FILE = "file"


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: datetime
    expires_at: Optional[datetime]
    access_count: int = 0
    last_accessed: datetime = None
    size_bytes: int = 0


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_size_bytes: int = 0
    entry_count: int = 0
    hit_rate: float = 0.0


class CacheService:
    """
    Cache service for GNN Solutions.
    
    Provides comprehensive caching and performance optimization
    across multiple cache levels and strategies.
    """
    
    def __init__(self):
        """Initialize the cache service."""
        self.logger = logging.getLogger(f"{__name__}.CacheService")
        
        # In-memory cache
        self._cache: Dict[str, CacheEntry] = {}
        
        # Cache configuration
        self._config = {
            'default_ttl': 3600,  # 1 hour
            'max_memory_size': 1000,
            'enable_compression': True,
            'enable_serialization': True,
        }
        
        # Cache statistics
        self._stats = CacheStats()
    
    # ==================== CACHE OPERATIONS METHODS ====================
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        try:
            if key not in self._cache:
                self._stats.misses += 1
                self._update_hit_rate()
                return None
            
            entry = self._cache[key]
            
            # Check expiration
            if entry.expires_at and entry.expires_at < datetime.utcnow():
                await self.delete(key)
                self._stats.misses += 1
                self._update_hit_rate()
                return None
            
            # Update access tracking
            entry.access_count += 1
            entry.last_accessed = datetime.utcnow()
            
            self._stats.hits += 1
            self._update_hit_rate()
            
            return entry.value
            
        except Exception as e:
            self.logger.error(f"Error getting from cache: {str(e)}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            
        Returns:
            True if successful
        """
        try:
            ttl = ttl or self._config['default_ttl']
            
            # Calculate size
            size_bytes = len(pickle.dumps(value))
            
            # Create entry
            expires_at = None
            if ttl:
                expires_at = datetime.utcnow() + timedelta(seconds=ttl)
            
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
                size_bytes=size_bytes
            )
            
            # Check if we need to evict
            if len(self._cache) >= self._config['max_memory_size'] and key not in self._cache:
                await self._evict_lru()
            
            # Store entry
            self._cache[key] = entry
            
            # Update stats
            self._stats.entry_count = len(self._cache)
            self._stats.total_size_bytes += size_bytes
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting in cache: {str(e)}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if successful
        """
        try:
            if key in self._cache:
                entry = self._cache[key]
                self._stats.total_size_bytes -= entry.size_bytes
                del self._cache[key]
                
                self._stats.entry_count = len(self._cache)
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error deleting from cache: {str(e)}")
            return False
    
    async def clear(self) -> bool:
        """
        Clear cache.
        
        Returns:
            True if successful
        """
        try:
            self._cache.clear()
            self._stats = CacheStats()
            return True
            
        except Exception as e:
            self.logger.error(f"Error clearing cache: {str(e)}")
            return False
    
    async def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if self._cache:
            # Find LRU entry
            lru_key = min(self._cache.keys(), key=lambda k: self._cache[k].last_accessed or self._cache[k].created_at)
            await self.delete(lru_key)
            self._stats.evictions += 1
    
    def _update_hit_rate(self) -> None:
        """Update hit rate calculation."""
        total = self._stats.hits + self._stats.misses
        if total > 0:
            self._stats.hit_rate = self._stats.hits / total
    
    # ==================== STATISTICS METHODS ====================
    
    async def get_stats(self) -> CacheStats:
        """
        Get cache statistics.
        
        Returns:
            Cache statistics
        """
        return self._stats
    
    # ==================== CONFIGURATION METHODS ====================
    
    def configure(self, **kwargs) -> None:
        """
        Configure cache service.
        
        Args:
            **kwargs: Configuration options
        """
        self._config.update(kwargs)
        self.logger.info(f"Cache service configured with: {kwargs}")
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Configuration key
            default: Default value
            
        Returns:
            Configuration value
        """
        return self._config.get(key, default)

