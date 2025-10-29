"""
Celery application configuration for the fund processing system.

This module sets up the Celery application instance used for handling
asynchronous tasks in the fund processing system. It configures Redis
as both the message broker and result backend for task execution.

The Celery app is configured with appropriate serialization settings,
timezone configuration, and routing rules for different types of tasks.
"""
from __future__ import annotations

from celery import Celery
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)

# Type alias for better code clarity in other modules that import this
from celery import Celery as CeleryApp

# Validate that the Redis URL is configured before initializing Celery
if not settings.REDIS_URL:
    logger.error("Redis URL is not configured in settings")
    raise ValueError("REDIS_URL must be configured in settings")

# Initialize the Celery application instance
# 
# This creates a Celery app instance for the fund processing system
# with Redis configured as both the broker and result backend.
# 
# Args:
#     "fund_processor": The name of the application
#     broker: URL for the message broker (Redis)
#     backend: URL for the result backend (Redis)
celery_app: CeleryApp = Celery(
    "fund_processor",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)
# Ensure task modules are discovered when the worker starts
celery_app.autodiscover_tasks(["app.tasks"])

# Configure Celery application settings
# 
# The following configuration options are set to ensure proper task
# execution, serialization, and timezone handling:
# 
# - task_serializer: Serialization format for task messages (JSON)
# - accept_content: Content types that the worker accepts (JSON only)
# - result_serializer: Serialization format for task results (JSON)
# - timezone: Timezone for task execution (UTC)
# - enable_utc: Use UTC as the default timezone (True)
# - task_routes: Route specific task patterns to dedicated queues
# - broker_connection_retry_on_startup: Retry connection on startup
# - result_expires: Time in seconds for result expiration (1 hour)
# - worker_prefetch_multiplier: Number of tasks to prefetch per worker
# - task_acks_late: Acknowledge tasks after execution
celery_app.conf.update(
    task_serializer="json",          # Serialize tasks as JSON
    accept_content=["json"],         # Only accept JSON content
    result_serializer="json",        # Serialize results as JSON
    timezone="UTC",                  # Use UTC timezone for tasks
    enable_utc=True,                 # Enable UTC timezone by default
    broker_connection_retry_on_startup=True,  # Retry connection on startup
    result_expires=3600,             # Results expire after 1 hour (3600 seconds)
    worker_prefetch_multiplier=1,    # Process one task at a time per worker
    task_acks_late=True,             # Acknowledge tasks after execution
    task_routes={
        # Route document-related tasks to the 'documents' queue
        "app.tasks.document_tasks.*": {"queue": "documents"},
    },
)

# Export the Celery application instance
# 
# This makes the celery_app instance available for import by other modules
# in the application that need to register tasks or interact with the 
# Celery application.
__all__ = ("celery_app",)
