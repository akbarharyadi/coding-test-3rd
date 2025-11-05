"""
Response compression middleware for FastAPI

Provides gzip compression for API responses to reduce bandwidth
and improve response times for clients.
"""
import gzip
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.datastructures import Headers, MutableHeaders

logger = logging.getLogger(__name__)


class CompressionMiddleware(BaseHTTPMiddleware):
    """
    Middleware to compress responses with gzip

    Automatically compresses responses when:
    - Client supports gzip (Accept-Encoding header)
    - Response size is above minimum threshold
    - Response is compressible (text, json, etc.)
    """

    def __init__(self, app, minimum_size: int = 500, compression_level: int = 6):
        """
        Initialize compression middleware

        Args:
            app: FastAPI application
            minimum_size: Minimum response size in bytes to compress (default: 500)
            compression_level: Gzip compression level 1-9 (default: 6)
        """
        super().__init__(app)
        self.minimum_size = minimum_size
        self.compression_level = compression_level

    async def dispatch(self, request: Request, call_next):
        """Process request and compress response if appropriate"""

        # Check if client accepts gzip encoding
        accept_encoding = request.headers.get("accept-encoding", "")
        supports_gzip = "gzip" in accept_encoding.lower()

        # Get response from application
        response = await call_next(request)

        # Skip compression if client doesn't support gzip
        if not supports_gzip:
            return response

        # Skip compression for certain responses
        if self._should_skip_compression(response):
            return response

        # For streaming responses, wrap in compression
        if isinstance(response, StreamingResponse):
            return await self._compress_streaming_response(response)

        # For regular responses, compress body
        return await self._compress_response(response)

    def _should_skip_compression(self, response: Response) -> bool:
        """
        Determine if response should skip compression

        Args:
            response: Response object

        Returns:
            True if compression should be skipped
        """
        # Skip if already compressed
        if response.headers.get("content-encoding"):
            return True

        # Skip if content type is not compressible
        content_type = response.headers.get("content-type", "")
        compressible_types = [
            "text/",
            "application/json",
            "application/javascript",
            "application/xml",
            "application/x-javascript",
        ]

        if not any(ct in content_type for ct in compressible_types):
            return True

        return False

    async def _compress_response(self, response: Response) -> Response:
        """
        Compress regular response body

        Args:
            response: Original response

        Returns:
            Compressed response or original if too small
        """
        # Get response body
        body = b""
        async for chunk in response.body_iterator:
            body += chunk

        # Skip compression if body is too small
        if len(body) < self.minimum_size:
            return Response(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
            )

        # Compress body
        compressed_body = gzip.compress(body, compresslevel=self.compression_level)

        # Calculate compression ratio
        ratio = (1 - len(compressed_body) / len(body)) * 100 if len(body) > 0 else 0
        logger.debug(
            f"Compressed response: {len(body)} -> {len(compressed_body)} bytes "
            f"({ratio:.1f}% reduction)"
        )

        # Create new headers with compression info
        headers = MutableHeaders(response.headers)
        headers["content-encoding"] = "gzip"
        headers["content-length"] = str(len(compressed_body))
        headers.setdefault("vary", "Accept-Encoding")

        return Response(
            content=compressed_body,
            status_code=response.status_code,
            headers=dict(headers),
            media_type=response.media_type,
        )

    async def _compress_streaming_response(
        self, response: StreamingResponse
    ) -> StreamingResponse:
        """
        Compress streaming response

        Args:
            response: Original streaming response

        Returns:
            Compressed streaming response
        """

        async def compressed_stream():
            """Generator that yields compressed chunks"""
            compressor = gzip.GzipFile(
                fileobj=None, mode="wb", compresslevel=self.compression_level
            )

            async for chunk in response.body_iterator:
                if chunk:
                    compressed_chunk = compressor.compress(chunk)
                    if compressed_chunk:
                        yield compressed_chunk

            # Flush remaining data
            final_chunk = compressor.flush()
            if final_chunk:
                yield final_chunk

        # Create new headers
        headers = MutableHeaders(response.headers)
        headers["content-encoding"] = "gzip"
        headers.setdefault("vary", "Accept-Encoding")

        # Remove content-length as it will change
        if "content-length" in headers:
            del headers["content-length"]

        return StreamingResponse(
            compressed_stream(),
            status_code=response.status_code,
            headers=dict(headers),
            media_type=response.media_type,
        )
