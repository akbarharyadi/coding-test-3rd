"""
Celery tasks for document processing in the fund management system.

This module contains Celery tasks responsible for processing uploaded documents
asynchronously. The main task processes various types of financial documents
(extracted from PDFs) and extracts structured data for storage in the database.

The document processing includes:
- Table extraction from PDF documents
- Financial data parsing (capital calls, distributions, adjustments)
- Data validation and cleaning
- Database persistence of extracted information

Each task follows a consistent pattern of updating the document status
in the database, processing the document, and returning processing results.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, cast

from app.core.celery_app import celery_app
from app.db.session import SessionLocal
from app.models.document import Document
from app.models.fund import Fund  # noqa: F401  ensure relationship mapping
from app.services.document_processor import DocumentProcessor
from app.schemas.document import ProcessedDocumentResult

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks.document_tasks.process_document")
def process_document_task(document_id: int, file_path: str, fund_id: int) -> ProcessedDocumentResult:
    """
    Process an uploaded document asynchronously using Celery.

    This task orchestrates the document processing workflow by:
    1. Retrieving the document record from the database
    2. Updating the document status to 'processing'
    3. Running the document processor to extract and parse data
    4. Updating the document status based on processing results
    5. Returning detailed processing results

    The task handles both expected and unexpected errors, updating the
    database with appropriate status and error messages in all cases.

    Args:
        document_id: Unique identifier for the document in the database
        file_path: Path to the uploaded document file on the filesystem
        fund_id: Identifier for the fund associated with this document

    Returns:
        ProcessedDocumentResult: Dictionary containing processing results
        with keys including 'status', 'document_id', 'fund_id', and
        potentially 'error' or other metadata depending on the outcome.

    Example:
        >>> # This would typically be called from a web request handler like:
        >>> from app.tasks.document_tasks import process_document_task
        >>> result = process_document_task.delay(
        ...     document_id=123,
        ...     file_path="/path/to/uploaded/document.pdf",
        ...     fund_id=456
        ... )
        >>> print(f"Task ID: {result.id}")
        >>> print(f"Task Status: {result.status}")
        >>> # Check result after completion:
        >>> if result.ready():
        ...     processing_result = result.get()
        ...     print(f"Processing status: {processing_result['status']}")

    Note:
        This function uses a database session that is properly closed
        in the finally block to prevent connection leaks. It also handles
        potential rollback scenarios for database consistency.
    """
    # Validate input parameters
    if not isinstance(document_id, int) or document_id <= 0:
        logger.error("Invalid document_id provided: %s. Document ID must be a positive integer.", document_id)
        return cast(
            ProcessedDocumentResult,
            {
                "status": "failed",
                "document_id": document_id,
                "fund_id": fund_id,
                "error": "Invalid document ID",
            },
        )
    
    if not isinstance(file_path, str) or not file_path.strip():
        logger.error("Invalid file_path provided: %s. File path cannot be empty.", file_path)
        return cast(
            ProcessedDocumentResult,
            {
                "status": "failed",
                "document_id": document_id,
                "fund_id": fund_id,
                "error": "Invalid file path",
            },
        )
    
    if not isinstance(fund_id, int) or fund_id <= 0:
        logger.error("Invalid fund_id provided: %s. Fund ID must be a positive integer.", fund_id)
        return cast(
            ProcessedDocumentResult,
            {
                "status": "failed",
                "document_id": document_id,
                "fund_id": fund_id,
                "error": "Invalid fund ID",
            },
        )
    
    # Log the start of document processing
    logger.info(
        "Starting document processing for document_id=%s, fund_id=%s, file_path=%s", 
        document_id, fund_id, file_path
    )
    
    # Create a new database session for this task
    # Using SessionLocal() creates a new database session tied to this task
    session = SessionLocal()
    document: Optional[Document] = None
    try:
        # Attempt to retrieve the document record from the database
        # If the document doesn't exist, return a failure result
        document = session.get(Document, document_id)
        if not document:
            logger.warning("Document %s not found when processing task", document_id)
            return cast(
                ProcessedDocumentResult,
                {
                    "status": "failed",
                    "document_id": document_id,
                    "fund_id": fund_id,
                    "error": "Document not found",
                },
            )

        # Update the document status to 'processing' and clear any previous error messages
        # This provides real-time feedback to the user about the processing status
        document.parsing_status = "processing"
        document.error_message = None
        session.commit()

        # Create a document processor instance with the database session
        # The processor handles all the complex document parsing and data extraction
        processor = DocumentProcessor(db_session=session)
        
        # Run the asynchronous document processing
        # This extracts tables, parses financial data, and stores results in the database
        result = asyncio.run(processor.process_document(file_path, document_id, fund_id))

        # Extract the processing status from the result and update the document record
        # Set appropriate error messages based on the processing outcome
        status = result.get("status", "failed")
        document.parsing_status = status
        if status == "failed":
            document.error_message = result.get("error")
        else:
            document.error_message = None
        session.commit()

        return result
    except Exception as exc:  # pragma: no cover - unexpected failures
        # Handle any unexpected exceptions during processing
        # Roll back the database session to maintain consistency
        session.rollback()
        
        # Log the full exception with traceback for debugging
        logger.exception("Celery document processing failed for %s: %s", document_id, exc)
        
        # Attempt to update the document status to 'failed' with error message
        # Using a fresh session to avoid issues with the rolled-back session
        try:
            fresh_session = SessionLocal()
            try:
                document = fresh_session.get(Document, document_id)
                if document:
                    document.parsing_status = "failed"
                    document.error_message = f"Unexpected processing error: {exc}"
                    fresh_session.commit()
            finally:
                fresh_session.close()
        except Exception as update_error:
            # If updating the document status also fails, log the error
            logger.error(
                "Failed to update document status after processing error for document %s: %s", 
                document_id, update_error
            )
        
        # Return a failure result with the original exception details
        return cast(
            ProcessedDocumentResult,
            {
                "status": "failed",
                "document_id": document_id,
                "fund_id": fund_id,
                "error": str(exc),
            },
        )
    finally:
        # Always close the database session to prevent connection leaks
        # This is critical in a long-running task environment
        session.close()
