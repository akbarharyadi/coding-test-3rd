"""
Document processing orchestrator.

This module coordinates table/text extraction from PDF documents and maps them
into the application's database models. Heavy-lifting helpers live in
`document_utils.py`, which keeps this class focused on orchestration and makes
unit testing much easier.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional

from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import SessionLocal
from app.models.transaction import Adjustment, CapitalCall, Distribution
from app.services.table_parser import TableParser
from app.services.data_cleaner import TableDataCleaner
from app.schemas.document import (
    ProcessedDocumentFailure,
    ProcessedDocumentResult,
    ProcessedDocumentSuccess,
)

# Note: This import path might need to be corrected based on the actual project structure
from app.helpers.document_utils import (
    TableCandidate,
    TextSegment,
    chunk_text_segments,
    extract_with_docling,
    extract_with_pdfplumber,
)

try:  # pragma: no cover - optional dependency
    from docling.document_converter import DocumentConverter as _DoclingConverter

    DOCLING_AVAILABLE = True
except ImportError:  # pragma: no cover - safe optional import
    _DoclingConverter = None  # type: ignore[assignment]
    DOCLING_AVAILABLE = False

if TYPE_CHECKING:  # pragma: no cover - typing only
    from docling.document_converter import DocumentConverter as DoclingConverterType
else:
    DoclingConverterType = Any

logger = logging.getLogger(__name__)


class DocumentProcessor:
    """
    High-level coordinator that parses documents and persists structured data.
    
    This class handles the full document processing pipeline:
    1. Extracting tables and text from PDF documents
    2. Parsing extracted data into structured formats
    3. Persisting data to the database
    4. Chunking text for vector storage
    
    The processor supports both Docling and pdfplumber as extraction backends,
    with automatic fallback between them based on availability and effectiveness.
    """

    def __init__(self, db_session: Optional[Session] = None, use_docling: Optional[bool] = None):
        """
        Initialize the DocumentProcessor.
        
        Args:
            db_session (Optional[Session]): Database session to use for operations.
                                         If None, a new session will be created per operation.
            use_docling (Optional[bool]): Whether to use Docling for extraction.
                                        If None, falls back to settings.DOCUMENT_PROCESSOR_USE_DOCLING
        """
        self.table_parser = TableParser()
        self.data_cleaner = TableDataCleaner()
        self._db_session = db_session
        if use_docling is None:
            use_docling = settings.DOCUMENT_PROCESSOR_USE_DOCLING
        self.use_docling = bool(use_docling and DOCLING_AVAILABLE)
        if use_docling and not DOCLING_AVAILABLE:
            logger.warning("Docling requested but not installed. Falling back to pdfplumber.")
        self._docling_converter: Optional[DoclingConverterType] = (
            _DoclingConverter() if self.use_docling and _DoclingConverter else None
        )

    async def process_document(
        self, file_path: str, document_id: int, fund_id: int
    ) -> "ProcessedDocumentResult":
        """
        Process a PDF document by extracting tables, persisting transactions, and chunking text.
        
        Args:
            file_path (str): Path to the PDF document to process
            document_id (int): ID of the document in the database
            fund_id (int): ID of the fund associated with the document
            
        Returns:
            ProcessedDocumentResult: Summary of the processing outcome
        """
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            logger.error(error_msg)
            return {
                "status": "failed",
                "document_id": document_id,
                "fund_id": fund_id,
                "error": error_msg,
            }

        with self._get_session() as session:
            parsed_tables: Dict[str, List[Dict[str, Any]]] = {
                "capital_calls": [],
                "distributions": [],
                "adjustments": [],
            }
            parser_engine = "pdfplumber"
            text_segments: List[TextSegment] = []
            table_candidates: List[TableCandidate] = []

            # Prefer Docling when available/configured.
            if self._docling_converter:
                try:
                    logger.debug(f"Processing document {document_id} with Docling")
                    table_candidates, text_segments = extract_with_docling(
                        file_path=file_path,
                        document_id=document_id,
                        fund_id=fund_id,
                        converter=self._docling_converter,
                    )
                    if table_candidates:
                        parser_engine = "docling"
                        logger.info(f"Docling successfully extracted {len(table_candidates)} table candidates for document {document_id}")
                    else:
                        logger.info(f"Docling produced no tables for document {document_id}. Will fall back to pdfplumber.")
                except Exception as docling_error:  # pragma: no cover - best-effort logging
                    logger.warning(f"Docling parsing failed for document {document_id} ({docling_error}). Falling back to pdfplumber.")
                    table_candidates = []
                    text_segments = []

            # If Docling was unavailable or produced nothing, fall back to pdfplumber.
            if not table_candidates:
                try:
                    logger.debug(f"Processing document {document_id} with pdfplumber")
                    table_candidates, fallback_segments = extract_with_pdfplumber(
                        file_path=file_path,
                        document_id=document_id,
                        fund_id=fund_id,
                    )
                    if not text_segments:
                        text_segments = fallback_segments
                    parser_engine = "pdfplumber"
                    logger.info(f"Pdfplumber successfully extracted {len(table_candidates)} table candidates for document {document_id}")
                except Exception as pdfplumber_error:
                    logger.exception(f"Pdfplumber parsing failed for document {document_id}: {pdfplumber_error}")
                    session.rollback()
                    return {
                        "status": "failed",
                        "document_id": document_id,
                        "fund_id": fund_id,
                        "error": f"Document parsing failed: {pdfplumber_error}",
                    }

            try:
                # Parse extracted tables
                successful_parses = 0
                for candidate in table_candidates:
                    parsed = self.table_parser.parse(candidate.data, candidate.page_number)
                    if parsed:
                        parsed_tables[parsed.table_type].extend(parsed.rows)
                        successful_parses += 1
                    else:
                        logger.debug(f"Failed to parse table candidate on page {candidate.page_number}")

                logger.info(f"Successfully parsed {successful_parses} table candidates for document {document_id}")

                cleaned_tables, cleaning_issues = self.data_cleaner.clean(parsed_tables)
                self._log_cleaning_issues(document_id, cleaning_issues)

                self._persist_transactions(session, fund_id, cleaned_tables)

                text_chunks = chunk_text_segments(
                    text_segments=text_segments,
                    chunk_size=settings.CHUNK_SIZE,
                    chunk_overlap=settings.CHUNK_OVERLAP,
                )

                result: ProcessedDocumentSuccess = {
                    "status": "completed",
                    "document_id": document_id,
                    "fund_id": fund_id,
                    "tables_extracted": {key: len(value) for key, value in cleaned_tables.items()},
                    "text_chunks": len(text_chunks),
                    "parser_engine": parser_engine,
                }
                
                logger.info(f"Successfully processed document {document_id}. Tables: {result['tables_extracted']}, Chunks: {result['text_chunks']}")
                return result
                
            except Exception as exc:  # pragma: no cover - unexpected processing errors
                logger.exception(f"Failed to process document {document_id}: {exc}")
                session.rollback()
                error_result: ProcessedDocumentFailure = {
                    "status": "failed",
                    "document_id": document_id,
                    "fund_id": fund_id,
                    "error": str(exc),
                }
                return error_result

    # ------------------------------------------------------------------ #
    # Persistence helpers
    # ------------------------------------------------------------------ #
    def _persist_transactions(
        self,
        session: Session,
        fund_id: int,
        tables: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        """Persist parsed table rows to the database using bulk operations for better performance.
        
        Args:
            session (Session): SQLAlchemy session to use for database operations
            fund_id (int): The fund ID to associate transactions with
            tables (Dict[str, List[Dict[str, Any]]]): Parsed table data organized by type
        """
        try:
            # Bulk delete existing transactions for this fund to avoid duplicates
            session.query(CapitalCall).filter(CapitalCall.fund_id == fund_id).delete(synchronize_session=False)
            session.query(Distribution).filter(Distribution.fund_id == fund_id).delete(synchronize_session=False)
            session.query(Adjustment).filter(Adjustment.fund_id == fund_id).delete(synchronize_session=False)
            
            # Prepare bulk insert data
            capital_calls = [
                CapitalCall(
                    fund_id=fund_id,
                    call_date=call["call_date"],
                    call_type=call.get("call_type"),
                    amount=call["amount"],
                    description=call.get("description"),
                )
                for call in tables.get("capital_calls", [])
                if all(key in call for key in ["call_date", "amount"])  # Ensure required fields exist
            ]
            
            distributions = [
                Distribution(
                    fund_id=fund_id,
                    distribution_date=distribution["distribution_date"],
                    distribution_type=distribution.get("distribution_type"),
                    is_recallable=distribution.get("is_recallable", False),
                    amount=distribution["amount"],
                    description=distribution.get("description"),
                )
                for distribution in tables.get("distributions", [])
                if all(key in distribution for key in ["distribution_date", "amount"])  # Ensure required fields exist
            ]
            
            adjustments = [
                Adjustment(
                    fund_id=fund_id,
                    adjustment_date=adjustment["adjustment_date"],
                    adjustment_type=adjustment.get("adjustment_type"),
                    category=adjustment.get("category"),
                    amount=adjustment["amount"],
                    is_contribution_adjustment=adjustment.get("is_contribution_adjustment", False),
                    description=adjustment.get("description"),
                )
                for adjustment in tables.get("adjustments", [])
                if all(key in adjustment for key in ["adjustment_date", "amount"])  # Ensure required fields exist
            ]
            
            # Bulk insert new transactions
            if capital_calls:
                session.add_all(capital_calls)
                logger.info(f"Added {len(capital_calls)} capital calls for fund {fund_id}")
            
            if distributions:
                session.add_all(distributions)
                logger.info(f"Added {len(distributions)} distributions for fund {fund_id}")
            
            if adjustments:
                session.add_all(adjustments)
                logger.info(f"Added {len(adjustments)} adjustments for fund {fund_id}")
            
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error persisting transactions for fund {fund_id}: {str(e)}")
            raise

    def _log_cleaning_issues(self, document_id: int, issues: Dict[str, List[str]]) -> None:
        """Emit debug logs for rows dropped during validation."""
        for table_type, messages in issues.items():
            for message in messages:
                logger.debug(
                    "Validation issue for document %s [%s]: %s", document_id, table_type, message
                )

    @contextmanager
    def _get_session(self) -> Generator[Session, None, None]:
        """
        Context manager that provides a database session and handles cleanup.
        
        Yields:
            Session: An active SQLAlchemy session
        """
        session = None
        try:
            if self._db_session:
                # Use the provided external session
                yield self._db_session
            else:
                # Create and manage a new session
                session = SessionLocal()
                yield session
        finally:
            # Only close the session if we created it locally
            if session:
                session.close()
