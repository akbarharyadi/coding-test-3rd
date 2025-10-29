"""
Helper utilities for document processing workflows.

This module exposes reusable helpers that make the core
DocumentProcessor easier to test in isolation. Each function
performs a small piece of work without touching the database.

The module provides functionality to:
- Extract tables and text from PDF documents using Docling or pdfplumber
- Convert text segments into overlapping chunks for vector storage
- Represent tabular data and text segments with minimal metadata
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

import pdfplumber

if TYPE_CHECKING:  # pragma: no cover
    from docling.document_converter import DocumentConverter as DoclingConverterType
else:  # pragma: no cover
    DoclingConverterType = Any

logger = logging.getLogger(__name__)


@dataclass
class TableCandidate:
    """
    Represents a table extracted from a document with minimal metadata.
    
    Attributes:
        data: 2D array representing the table cells, where each inner list is a row
        page_number: The page number where the table was found (1-indexed)
        
    Example:
        >>> table_data = [["Name", "Age"], ["John", "25"], ["Jane", "30"]]
        >>> table = TableCandidate(data=table_data, page_number=1)
        >>> len(table.data)  # Number of rows
        3
    """
    data: List[List[str]]
    page_number: int


@dataclass
class TextSegment:
    """
    Represents a chunk of text extracted from a document with associated metadata.
    
    Attributes:
        page_number: The page number where the text was found (1-indexed)
        text: The actual text content extracted from the document
        document_id: Unique identifier for the document
        fund_id: Unique identifier for the fund associated with this document
        
    Example:
        >>> segment = TextSegment(
        ...     page_number=1,
        ...     text="This is a sample text from page 1",
        ...     document_id=123,
        ...     fund_id=456
        ... )
        >>> segment.text
        'This is a sample text from page 1'
    """
    page_number: int
    text: str
    document_id: int
    fund_id: int


def extract_with_docling(
    file_path: str,
    document_id: int,
    fund_id: int,
    converter: DoclingConverterType,
) -> Tuple[List[TableCandidate], List[TextSegment]]:
    """
    Extract tables and text segments using Docling.
    
    Docling is a modern document parsing library that provides more accurate 
    extraction of complex documents including tables, with better support for 
    document structure analysis.
    
    Args:
        file_path: Path to the PDF file to be processed
        document_id: Unique identifier for the document
        fund_id: Unique identifier for the fund associated with this document
        converter: Pre-instantiated Docling DocumentConverter object
        
    Returns:
        A tuple containing:
        - List of TableCandidate objects representing extracted tables
        - List of TextSegment objects representing extracted text segments
        
    Raises:
        FileNotFoundError: If the specified file path does not exist
        Exception: If Docling conversion fails for any reason
        
    Example:
        >>> from docling.document_converter import DocumentConverter
        >>> converter = DocumentConverter()
        >>> tables, segments = extract_with_docling(
        ...     "sample.pdf", 
        ...     document_id=123, 
        ...     fund_id=456, 
        ...     converter=converter
        ... )
        >>> print(f"Found {len(tables)} tables and {len(segments)} text segments")
    """
    document_path = Path(file_path)
    
    try:
        conversion = converter.convert(document_path)
        doc = conversion.document
    except Exception as e:
        logger.error(f"Failed to convert document with Docling: {str(e)}")
        raise

    tables: List[TableCandidate] = []
    for table in getattr(doc, "tables", []) or []:
        table_data = _docling_table_to_matrix(table)
        if not table_data:
            continue

        tables.append(
            TableCandidate(
                data=table_data,
                page_number=_get_docling_page_number(getattr(table, "prov", None)) or 1,
            )
        )

    segments: List[TextSegment] = []
    page_text: Dict[int, List[str]] = {}
    for text_item in getattr(doc, "texts", []) or []:
        text = (getattr(text_item, "text", "") or "").strip()
        if not text:
            continue

        page_number = _get_docling_page_number(getattr(text_item, "prov", None)) or 1
        page_text.setdefault(page_number, []).append(text)

    for page_number, entries in sorted(page_text.items()):
        segments.append(
            TextSegment(
                page_number=page_number,
                text="\n".join(entries),
                document_id=document_id,
                fund_id=fund_id,
            )
        )

    return tables, segments


def extract_with_pdfplumber(
    file_path: str,
    document_id: int,
    fund_id: int,
) -> Tuple[List[TableCandidate], List[TextSegment]]:
    """
    Extract tables and text segments using pdfplumber.
    
    Pdfplumber is a reliable PDF parsing library focused on extracting text and
    tables from PDF documents. It's particularly good for structured documents
    with well-defined tables.
    
    Args:
        file_path: Path to the PDF file to be processed
        document_id: Unique identifier for the document
        fund_id: Unique identifier for the fund associated with this document
        
    Returns:
        A tuple containing:
        - List of TableCandidate objects representing extracted tables
        - List of TextSegment objects representing extracted text segments
        
    Raises:
        FileNotFoundError: If the specified file path does not exist
        pdfplumber.exceptions.PDFSyntaxError: If the PDF is malformed
        Exception: If pdfplumber fails to process the document
        
    Example:
        >>> tables, segments = extract_with_pdfplumber(
        ...     "sample.pdf", 
        ...     document_id=123, 
        ...     fund_id=456
        ... )
        >>> for table in tables:
        ...     print(f"Table on page {table.page_number} has {len(table.data)} rows")
        >>> for segment in segments:
        ...     print(f"Page {segment.page_number} has {len(segment.text)} characters")
    """
    tables: List[TableCandidate] = []
    segments: List[TextSegment] = []

    try:
        with pdfplumber.open(file_path) as pdf:
            for index, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                if page_text.strip():
                    segments.append(
                        TextSegment(
                            page_number=index,
                            text=page_text,
                            document_id=document_id,
                            fund_id=fund_id,
                        )
                    )

                for table in page.extract_tables() or []:
                    tables.append(
                        TableCandidate(
                            data=table,
                            page_number=index,
                        )
                    )
    except FileNotFoundError:
        logger.error(f"PDF file not found: {file_path}")
        raise
    except pdfplumber.exceptions.PDFSyntaxError:
        logger.error(f"Invalid PDF syntax: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to extract content with pdfplumber: {str(e)}")
        raise

    return tables, segments


def chunk_text_segments(
    text_segments: List[TextSegment],
    chunk_size: int,
    chunk_overlap: int,
) -> List[Dict[str, Any]]:
    """
    Convert a list of text segments into overlapping chunks suitable for vector storage.
    
    This function implements a sliding window approach to break down text into chunks
    of specified size with configurable overlap. This is particularly useful for 
    vector databases where context preservation is important for semantic search.
    
    Args:
        text_segments: List of TextSegment objects produced by extraction helpers
        chunk_size: Maximum character length per chunk (must be > 0)
        chunk_overlap: Number of characters to overlap between adjacent chunks
        
    Returns:
        List of chunk dictionaries, each containing:
        - 'content': The actual text chunk
        - 'metadata': Dictionary with document metadata including IDs, page number,
          and character position offsets for reconstruction
        
    Raises:
        ValueError: If chunk_size is not positive
        
    Example:
        >>> segment = TextSegment(
        ...     page_number=1,
        ...     text="This is a sample text that will be chunked into smaller pieces.",
        ...     document_id=123,
        ...     fund_id=456
        ... )
        >>> chunks = chunk_text_segments([segment], chunk_size=20, chunk_overlap=5)
        >>> len(chunks)
        3
        >>> chunks[0]['content']  # doctest: +ELLIPSIS
        'This is a sample...'
        >>> chunks[0]['metadata']['page_number']
        1
    """
    if not text_segments:
        return []

    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive, got {chunk_size}")

    max_chunk = max(chunk_size, 1)
    overlap = max(min(chunk_overlap, max_chunk - 1), 0)

    chunks: List[Dict[str, Any]] = []
    for segment in text_segments:
        text = segment.text
        if not text:
            continue

        start = 0
        position = 0
        length = len(text)

        while start < length:
            end = min(length, start + max_chunk)
            chunk_text = text[start:end].strip()

            if chunk_text:
                chunks.append(
                    {
                        "content": chunk_text,
                        "metadata": {
                            "document_id": segment.document_id,
                            "fund_id": segment.fund_id,
                            "page_number": segment.page_number,
                            "offset_start": start,
                            "offset_end": end,
                            "position": position,
                        },
                    }
                )
                position += 1

            if end >= length:
                break

            start = max(end - overlap, start + 1)

    return chunks


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #

def _docling_table_to_matrix(table: Any) -> List[List[str]]:
    """
    Convert a Docling table object into a 2D array of strings.
    
    This function handles the conversion of Docling's internal table representation
    to a standard 2D array format, taking into account cell spans (row_span and col_span)
    that may occur in complex tables.
    
    Args:
        table: A Docling table object with table data
        
    Returns:
        2D array representing the table where each inner list is a row
        Returns empty list if the table has no valid data
        
    Example:
        >>> # This is an internal function, typically called by extract_with_docling
        >>> # Input: A Docling table object with 2 rows, 2 columns
        >>> # Output: [["Header1", "Header2"], ["Row1Col1", "Row1Col2"]]
    """
    data = getattr(table, "data", None)
    if not data:
        return []

    num_rows = getattr(data, "num_rows", 0) or 0
    num_cols = getattr(data, "num_cols", 0) or 0

    if num_rows <= 0 or num_cols <= 0:
        return []

    # Pre-allocate the matrix to avoid repeated resizing
    matrix: List[List[str]] = [["" for _ in range(num_cols)] for _ in range(num_rows)]

    # Process all cells in a single pass
    for cell in getattr(data, "table_cells", []) or []:
        text = (getattr(cell, "text", "") or "").strip()
        if not text:
            continue

        start_row = getattr(cell, "start_row_offset_idx", 0) or 0
        start_col = getattr(cell, "start_col_offset_idx", 0) or 0
        row_span = max(1, getattr(cell, "row_span", 1) or 1)
        col_span = max(1, getattr(cell, "col_span", 1) or 1)

        # Fill the matrix with the cell text across the spanned area
        end_row = min(num_rows, start_row + row_span)
        end_col = min(num_cols, start_col + col_span)
        
        for row in range(start_row, end_row):
            for col in range(start_col, end_col):
                existing = matrix[row][col]
                matrix[row][col] = f"{existing} {text}".strip() if existing else text

    # Return only rows that contain content
    cleaned_rows = [row for row in matrix if any(cell.strip() for cell in row)]
    return cleaned_rows


def _get_docling_page_number(provenance: Optional[List[Any]]) -> Optional[int]:
    """
    Extract the first page number from Docling's provenance metadata.
    
    Docling's provenance metadata contains information about where in the document
    an element was found. This function extracts the page number from that metadata.
    
    Args:
        provenance: List of Docling provenance objects, or None
        
    Returns:
        The page number from the first provenance object, or None if not available
        
    Example:
        >>> # This is an internal function for use with Docling
        >>> # Input: [{'page_no': 5}, {'page_no': 6}]
        >>> # Output: 5
    """
    if not provenance:
        return None
    first = provenance[0]
    return getattr(first, "page_no", None)


# --------------------------------------------------------------------------- #
# Unit test examples
# --------------------------------------------------------------------------- #

"""
Unit test examples for document_utils module:

import pytest
from unittest.mock import Mock, patch
from app.helpers.document_utils import (
    TableCandidate, 
    TextSegment, 
    extract_with_docling, 
    extract_with_pdfplumber, 
    chunk_text_segments
)

class TestTableCandidate:
    def test_table_candidate_creation(self):
        data = [["Name", "Age"], ["John", "25"]]
        table = TableCandidate(data=data, page_number=1)
        assert table.data == data
        assert table.page_number == 1

class TestTextSegment:
    def test_text_segment_creation(self):
        segment = TextSegment(
            page_number=1,
            text="Sample text",
            document_id=123,
            fund_id=456
        )
        assert segment.page_number == 1
        assert segment.text == "Sample text"
        assert segment.document_id == 123
        assert segment.fund_id == 456

class TestExtractWithDocling:
    @patch('pathlib.Path')
    def test_extract_with_docling_success(self, mock_path):
        # Mock Docling converter
        mock_converter = Mock()
        mock_conversion = Mock()
        mock_doc = Mock()
        mock_conversion.document = mock_doc
        mock_converter.convert.return_value = mock_conversion
        
        # Setup mock document data
        mock_table = Mock()
        mock_table.prov = [{'page_no': 1}]
        mock_cell_data = Mock()
        mock_cell_data.num_rows = 2
        mock_cell_data.num_cols = 2
        mock_cell = Mock()
        mock_cell.text = "Sample"
        mock_cell.start_row_offset_idx = 0
        mock_cell.start_col_offset_idx = 0
        mock_cell.row_span = 1
        mock_cell.col_span = 1
        mock_cell_data.table_cells = [mock_cell]
        mock_table.data = mock_cell_data
        
        mock_text_item = Mock()
        mock_text_item.text = "Sample text"
        mock_text_item.prov = [{'page_no': 1}]
        
        mock_doc.tables = [mock_table]
        mock_doc.texts = [mock_text_item]
        
        tables, segments = extract_with_docling(
            "sample.pdf", 123, 456, mock_converter
        )
        
        assert len(tables) == 1
        assert len(segments) == 1
        assert tables[0].page_number == 1
        assert segments[0].text == "Sample text"

class TestExtractWithPdfplumber:
    @patch('pdfplumber.open')
    def test_extract_with_pdfplumber_success(self, mock_pdf_open):
        # Mock pdfplumber behavior
        mock_pdf = Mock()
        mock_page = Mock()
        mock_page.extract_text.return_value = "Sample page text"
        mock_page.extract_tables.return_value = [["Header1", "Header2"]]
        mock_pdf.pages = [mock_page]
        mock_pdf_open.return_value.__enter__.return_value = mock_pdf
        
        tables, segments = extract_with_pdfplumber("sample.pdf", 123, 456)
        
        assert len(tables) == 1
        assert len(segments) == 1
        assert tables[0].page_number == 1
        assert segments[0].text == "Sample page text"

class TestChunkTextSegments:
    def test_chunk_text_segments_basic(self):
        segments = [
            TextSegment(
                page_number=1,
                text="This is a sample text for chunking.",
                document_id=123,
                fund_id=456
            )
        ]
        
        chunks = chunk_text_segments(segments, chunk_size=20, chunk_overlap=5)
        assert len(chunks) >= 1
        assert 'content' in chunks[0]
        assert 'metadata' in chunks[0]
        
    def test_chunk_text_segments_with_overlap(self):
        segments = [
            TextSegment(
                page_number=1,
                text="A B C D E F G H I J K L M N O P Q R S T U V W X Y Z",
                document_id=123,
                fund_id=456
            )
        ]
        
        chunks = chunk_text_segments(segments, chunk_size=10, chunk_overlap=3)
        # Should have multiple chunks due to size limit
        assert len(chunks) > 1
        
    def test_chunk_text_segments_empty_input(self):
        chunks = chunk_text_segments([], chunk_size=100, chunk_overlap=5)
        assert chunks == []
        
    def test_chunk_text_segments_invalid_chunk_size(self):
        segments = [TextSegment(
            page_number=1,
            text="Sample text",
            document_id=123,
            fund_id=456
        )]
        
        with pytest.raises(ValueError):
            chunk_text_segments(segments, chunk_size=0, chunk_overlap=5)
"""
