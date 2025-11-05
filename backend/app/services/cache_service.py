"""
Redis cache service for query results and embeddings

Provides caching functionality to improve performance by storing:
- RAG query results
- Vector search results
- Frequently accessed data
"""
import json
import hashlib
import logging
from typing import Any, Dict, Optional
from redis import Redis
from redis.exceptions import RedisError
from app.core.config import settings

logger = logging.getLogger(__name__)


class CacheService:
    """Redis-based caching service for query results and data"""

    def __init__(self):
        """Initialize Redis connection"""
        self.redis_client: Optional[Redis] = None
        self.enabled = False
        self._connect()

    def _connect(self):
        """Establish Redis connection with error handling"""
        try:
            self.redis_client = Redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2
            )
            # Test connection
            self.redis_client.ping()
            self.enabled = True
            logger.info("Redis cache connected successfully")
        except (RedisError, Exception) as e:
            logger.warning(f"Redis cache unavailable: {e}. Continuing without cache.")
            self.enabled = False
            self.redis_client = None

    def _generate_key(self, prefix: str, **kwargs) -> str:
        """
        Generate cache key from parameters

        Args:
            prefix: Key prefix (e.g., 'query', 'search')
            **kwargs: Parameters to include in key

        Returns:
            Hashed cache key
        """
        # Sort kwargs for consistent key generation
        sorted_params = sorted(kwargs.items())
        param_str = json.dumps(sorted_params, sort_keys=True)
        hash_obj = hashlib.md5(param_str.encode())
        return f"{prefix}:{hash_obj.hexdigest()}"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached value

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        if not self.enabled or not self.redis_client:
            return None

        try:
            cached = self.redis_client.get(key)
            if cached:
                logger.debug(f"Cache hit: {key}")
                return json.loads(cached)
            logger.debug(f"Cache miss: {key}")
            return None
        except (RedisError, json.JSONDecodeError) as e:
            logger.warning(f"Cache get error for key {key}: {e}")
            return None

    def set(self, key: str, value: Dict[str, Any], ttl: int = 3600):
        """
        Set cached value with TTL

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds (default: 1 hour)
        """
        if not self.enabled or not self.redis_client:
            return

        try:
            serialized = json.dumps(value)
            self.redis_client.setex(key, ttl, serialized)
            logger.debug(f"Cache set: {key} (TTL: {ttl}s)")
        except (RedisError, TypeError) as e:
            logger.warning(f"Cache set error for key {key}: {e}")

    def delete(self, key: str):
        """
        Delete cached value

        Args:
            key: Cache key to delete
        """
        if not self.enabled or not self.redis_client:
            return

        try:
            self.redis_client.delete(key)
            logger.debug(f"Cache deleted: {key}")
        except RedisError as e:
            logger.warning(f"Cache delete error for key {key}: {e}")

    def clear_pattern(self, pattern: str):
        """
        Clear all keys matching pattern

        Args:
            pattern: Redis pattern (e.g., 'query:*')
        """
        if not self.enabled or not self.redis_client:
            return

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                self.redis_client.delete(*keys)
                logger.info(f"Cleared {len(keys)} keys matching pattern: {pattern}")
        except RedisError as e:
            logger.warning(f"Cache clear error for pattern {pattern}: {e}")

    def get_query_cache(self, query: str, fund_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        Get cached RAG query result

        Args:
            query: Query string
            fund_id: Optional fund ID filter

        Returns:
            Cached query result or None
        """
        key = self._generate_key("query", q=query, fund_id=fund_id)
        return self.get(key)

    def set_query_cache(
        self,
        query: str,
        result: Dict[str, Any],
        fund_id: Optional[int] = None,
        ttl: int = 3600
    ):
        """
        Cache RAG query result

        Args:
            query: Query string
            result: Query result to cache
            fund_id: Optional fund ID filter
            ttl: Time to live in seconds (default: 1 hour)
        """
        key = self._generate_key("query", q=query, fund_id=fund_id)
        self.set(key, result, ttl)

    def get_search_cache(
        self,
        query: str,
        k: int = 5,
        fund_id: Optional[int] = None,
        document_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached search result

        Args:
            query: Search query
            k: Number of results
            fund_id: Optional fund ID filter
            document_id: Optional document ID filter

        Returns:
            Cached search result or None
        """
        key = self._generate_key(
            "search",
            q=query,
            k=k,
            fund_id=fund_id,
            doc_id=document_id
        )
        return self.get(key)

    def set_search_cache(
        self,
        query: str,
        result: Dict[str, Any],
        k: int = 5,
        fund_id: Optional[int] = None,
        document_id: Optional[int] = None,
        ttl: int = 1800
    ):
        """
        Cache search result

        Args:
            query: Search query
            result: Search result to cache
            k: Number of results
            fund_id: Optional fund ID filter
            document_id: Optional document ID filter
            ttl: Time to live in seconds (default: 30 minutes)
        """
        key = self._generate_key(
            "search",
            q=query,
            k=k,
            fund_id=fund_id,
            doc_id=document_id
        )
        self.set(key, result, ttl)

    def invalidate_document_caches(self, document_id: int):
        """
        Invalidate all caches related to a document
        (useful when document is updated/deleted)

        Args:
            document_id: Document ID to invalidate caches for
        """
        self.clear_pattern(f"search:*doc_id*{document_id}*")
        self.clear_pattern("query:*")  # Query results may reference this document
        logger.info(f"Invalidated caches for document {document_id}")


# Global cache service instance
cache_service = CacheService()
