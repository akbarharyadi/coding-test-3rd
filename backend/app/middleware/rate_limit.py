"""
Rate limiting middleware for FastAPI

Protects API from abuse by limiting request rates per client IP.
Uses Redis for distributed rate limiting.
"""
import logging
import time
from typing import Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from redis import Redis
from redis.exceptions import RedisError
from app.core.config import settings

logger = logging.getLogger(__name__)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Rate limiting middleware using Redis

    Implements sliding window rate limiting per client IP address.
    """

    def __init__(
        self,
        app,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
    ):
        """
        Initialize rate limiter

        Args:
            app: FastAPI application
            requests_per_minute: Maximum requests per minute (default: 60)
            requests_per_hour: Maximum requests per hour (default: 1000)
        """
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.redis_client: Optional[Redis] = None
        self.enabled = False
        self._connect_redis()

    def _connect_redis(self):
        """Connect to Redis for rate limit storage"""
        try:
            self.redis_client = Redis.from_url(
                settings.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            # Test connection
            self.redis_client.ping()
            self.enabled = True
            logger.info("Rate limiting enabled with Redis")
        except (RedisError, Exception) as e:
            logger.warning(
                f"Rate limiting disabled - Redis unavailable: {e}"
            )
            self.enabled = False
            self.redis_client = None

    async def dispatch(self, request: Request, call_next):
        """Check rate limit before processing request"""

        # Skip rate limiting if Redis is unavailable
        if not self.enabled or not self.redis_client:
            return await call_next(request)

        # Get client identifier (IP address)
        client_ip = self._get_client_ip(request)

        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/api/health"]:
            return await call_next(request)

        # Check rate limits
        is_allowed, retry_after = self._check_rate_limit(client_ip)

        if not is_allowed:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Too many requests. Please try again later.",
                    "retry_after": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        # Process request
        response = await call_next(request)

        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(
            self._get_remaining_requests(client_ip, "minute")
        )

        return response

    def _get_client_ip(self, request: Request) -> str:
        """
        Extract client IP address from request

        Args:
            request: Request object

        Returns:
            Client IP address
        """
        # Check for forwarded IP (behind proxy)
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()

        # Check for real IP header
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()

        # Fall back to direct client IP
        if request.client:
            return request.client.host

        return "unknown"

    def _check_rate_limit(self, client_ip: str) -> tuple[bool, int]:
        """
        Check if client has exceeded rate limits

        Args:
            client_ip: Client IP address

        Returns:
            Tuple of (is_allowed, retry_after_seconds)
        """
        if not self.redis_client:
            return True, 0

        try:
            current_time = int(time.time())

            # Check minute limit
            minute_key = f"ratelimit:{client_ip}:minute"
            minute_count = self.redis_client.get(minute_key)

            if minute_count and int(minute_count) >= self.requests_per_minute:
                ttl = self.redis_client.ttl(minute_key)
                return False, max(ttl, 1)

            # Check hour limit
            hour_key = f"ratelimit:{client_ip}:hour"
            hour_count = self.redis_client.get(hour_key)

            if hour_count and int(hour_count) >= self.requests_per_hour:
                ttl = self.redis_client.ttl(hour_key)
                return False, max(ttl, 1)

            # Increment counters
            pipe = self.redis_client.pipeline()

            # Increment minute counter
            pipe.incr(minute_key)
            pipe.expire(minute_key, 60)

            # Increment hour counter
            pipe.incr(hour_key)
            pipe.expire(hour_key, 3600)

            pipe.execute()

            return True, 0

        except RedisError as e:
            logger.error(f"Rate limit check error: {e}")
            # Allow request on error (fail open)
            return True, 0

    def _get_remaining_requests(self, client_ip: str, window: str = "minute") -> int:
        """
        Get remaining requests for client in time window

        Args:
            client_ip: Client IP address
            window: Time window ('minute' or 'hour')

        Returns:
            Remaining request count
        """
        if not self.redis_client:
            return 0

        try:
            key = f"ratelimit:{client_ip}:{window}"
            count = self.redis_client.get(key)

            if window == "minute":
                limit = self.requests_per_minute
            else:
                limit = self.requests_per_hour

            used = int(count) if count else 0
            return max(0, limit - used)

        except RedisError:
            return 0
