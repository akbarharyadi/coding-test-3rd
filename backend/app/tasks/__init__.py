"""Celery task modules."""

# Ensure tasks are registered when Celery discovers this package.
from . import document_tasks  # noqa: F401
