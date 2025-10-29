"""
Table parsing utilities for pdfplumber extracted tables.

This module provides functionality to parse and classify financial tables extracted
from PDF documents, particularly focusing on capital calls, distributions, and 
adjustments. The parser can identify different table types based on header keywords
and extract structured data with appropriate field mappings.

The main components include:
- ParsedTable: A dataclass representing the parsed table structure
- TableParser: The main parser class that handles table classification and parsing

This is typically used in financial document processing pipelines where structured
data extraction from PDFs is required for further analysis or storage.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Optional

from app.helpers.table_utils import (
    DEFAULT_DATE_FORMATS,
    clean_table,
    find_column,
    is_contribution_adjustment,
    normalize_cell,
    normalize_header,
    parse_amount,
    parse_bool,
    parse_date,
    safe_get,
    should_skip_row,
)

logger = logging.getLogger(__name__)


@dataclass
class ParsedTable:
    """
    Structured representation of a parsed table with metadata.
    
    Attributes:
        table_type: The classified type of the table (capital_calls, distributions, adjustments)
        rows: List of dictionaries where each dictionary represents a parsed row
        page_number: The page number where the table was found in the source document
        header: The original header row from the source table
        
    Example:
        >>> from datetime import date
        >>> from decimal import Decimal
        >>> table = ParsedTable(
        ...     table_type="capital_calls",
        ...     rows=[
        ...         {
        ...             "call_date": date(2023, 1, 15),
        ...             "call_type": "Regular",
        ...             "amount": Decimal("1000.00"),
        ...             "description": "Q1 capital call"
        ...         }
        ...     ],
        ...     page_number=5,
        ...     header=["Date", "Type", "Amount", "Description"]
        ... )
        >>> table.table_type
        'capital_calls'
        >>> len(table.rows)
        1
    """

    table_type: str
    rows: List[Dict[str, Any]]
    page_number: int
    header: List[str]


class TableParser:
    """
    Parse tables extracted from PDF pages and classify them into financial categories.
    
    This class is specifically designed to handle financial tables commonly found
    in investment documents, fund statements, and financial reports. It can 
    automatically detect and parse three main types of financial tables:
    
    1. Capital Calls: Tables showing fund capital call information
    2. Distributions: Tables showing fund distributions to investors
    3. Adjustments: Tables showing various financial adjustments
    
    The parser uses header analysis and keyword matching to classify tables,
    then applies appropriate parsing logic to extract structured data with
    proper data type conversion.
    
    Date formats that are supported by default:
    - YYYY-MM-DD
    - MM/DD/YYYY
    - DD/MM/YYYY
    - Month DD, YYYY (Jan 01, 2024)
    - Month DD, YYYY (January 01, 2024)
    """

    _DATE_FORMATS = tuple(DEFAULT_DATE_FORMATS)

    def parse(self, table: List[List[Any]], page_number: int) -> Optional[ParsedTable]:
        """
        Parse a pdfplumber table into structured records.
        
        This is the main entry point for table parsing. It performs the following steps:
        1. Cleans and normalizes the raw table data
        2. Classifies the table type based on header keywords
        3. Applies the appropriate parsing logic based on the table type
        4. Returns a structured ParsedTable object or None if parsing fails
        
        Args:
            table: 2D list representing the raw table data as extracted by pdfplumber
                   Each inner list represents a row, and inner elements are the cell values
            page_number: The page number where this table was extracted from in the PDF
            
        Returns:
            ParsedTable object containing the structured data, or None if:
            - The table is empty after cleaning
            - The table type cannot be classified
            - No rows could be parsed successfully after classification
            
        Example:
            >>> parser = TableParser()
            >>> raw_table = [
            ...     ["Date", "Type", "Amount", "Description"],
            ...     ["2023-01-15", "Regular", "$1,000.00", "Q1 Capital Call"],
            ...     ["2023-04-15", "Regular", "$500.00", "Q2 Capital Call"]
            ... ]
            >>> result = parser.parse(raw_table, 5)
            >>> if result:
            ...     print(f"Found {result.table_type} table with {len(result.rows)} rows")
            >>>     # Output: Found capital_calls table with 2 rows
        """
        # Validate inputs
        if not isinstance(table, list):
            logger.error(f"Invalid table input: expected list, got {type(table)}")
            return None
            
        if not isinstance(page_number, int) or page_number < 0:
            logger.error(f"Invalid page number: {page_number}")
            return None

        cleaned = clean_table(table)
        if not cleaned:
            logger.debug(f"No valid content found in table on page {page_number}")
            return None

        header = cleaned[0]
        normalized_header = [normalize_header(cell) for cell in header]
        data_rows = cleaned[1:]
        table_type = self._classify_table(normalized_header, data_rows)

        if not table_type:
            logger.debug(f"Could not classify table type on page {page_number}")
            return None

        if table_type == "capital_calls":
            rows = self._parse_capital_calls(header, normalized_header, data_rows, page_number)
        elif table_type == "distributions":
            rows = self._parse_distributions(header, normalized_header, data_rows, page_number)
        elif table_type == "adjustments":
            rows = self._parse_adjustments(header, normalized_header, data_rows, page_number)
        else:
            logger.warning(f"Unexpected table type: {table_type}")
            return None

        if not rows:
            logger.debug(f"No valid rows parsed for {table_type} table on page {page_number}")
            return None

        return ParsedTable(
            table_type=table_type,
            rows=rows,
            page_number=page_number,
            header=header,
        )

    # --------------------------------------------------------------------- #
    # Classification helpers
    # --------------------------------------------------------------------- #
    def _classify_table(self, header: List[str], rows: List[List[str]]) -> Optional[str]:
        """
        Classify table type based on header keywords and sample row content.
        
        This method analyzes the header and first few rows of a table to 
        determine if it contains capital calls, distributions, or adjustments
        data. It uses keyword matching and pattern recognition to classify
        the table type.
        
        Classification is performed in order of priority:
        1. First checks for adjustment keywords
        2. Then distribution keywords
        3. Then capital call keywords
        4. Falls back to pattern matching if no keywords match
        
        Args:
            header: List of strings representing the table header row
            rows: List of table data rows (excluding header) to use for classification
            
        Returns:
            String representing the table type ("adjustments", "distributions", 
            "capital_calls") or None if classification fails
            
        Example:
            >>> parser = TableParser()
            >>> header = ["Date", "Call Type", "Amount", "Description"]
            >>> rows = [["2023-01-15", "Regular", "$1000.00", "Q1 Call"]]
            >>> parser._classify_table(header, rows)
            'capital_calls'
        """
        if not header:
            return None

        header_text = " ".join(header)
        sample_text = " ".join(
            " ".join(normalize_cell(cell) for cell in row if cell) for row in rows[:3]
        ).lower()
        candidate_text = f"{header_text} {sample_text}".lower()

        adjustment_keywords = [
            "adjustment",
            "recallable distribution",
            "capital call adjustment",
            "contribution adjustment",
            "fee adjustment",
        ]
        distribution_keywords = [
            "distribution",
            "return of capital",
            "recallable",
            "dividend",
            "income",
        ]
        capital_call_keywords = [
            "capital call",
            "call number",
            "capital contribution",
            "capital commitments",
        ]

        if any(keyword in candidate_text for keyword in adjustment_keywords):
            return "adjustments"

        if any(keyword in candidate_text for keyword in distribution_keywords):
            return "distributions"

        if any(keyword in candidate_text for keyword in capital_call_keywords):
            return "capital_calls"

        if "call" in header_text and "amount" in header_text and "recallable" not in header_text:
            return "capital_calls"

        return None

    # --------------------------------------------------------------------- #
    # Parsing helpers per table type
    # --------------------------------------------------------------------- #
    def _parse_capital_calls(
        self,
        raw_header: List[str],
        normalized_header: List[str],
        rows: List[List[str]],
        page_number: int,
    ) -> List[Dict[str, Any]]:
        """
        Parse capital call table rows into structured data.
        
        This method looks for specific column headers in the capital call table
        and maps them to standardized field names. It extracts key information
        such as the call date, amount, type, and description.
        
        The method automatically detects column positions based on header text
        and attempts to parse dates and amounts into appropriate data types.
        
        Args:
            raw_header: Original header row from the table
            normalized_header: Normalized header for keyword matching
            rows: Data rows to parse (excluding header)
            page_number: Page number where the table was found
            
        Returns:
            List of dictionaries, each containing:
            - call_date: date object for the capital call
            - call_type: string describing the type of call
            - amount: Decimal object representing the call amount
            - description: Optional description text
            - page_number: Original page number
            - header: Original header row
            
        Example:
            >>> parser = TableParser()
            >>> header = ["Date", "Call Type", "Amount", "Description"]
            >>> rows = [["2023-01-15", "Regular", "$1000.00", "Q1 Capital Call"]]
            >>> result = parser._parse_capital_calls(header, [h.lower() for h in header], rows, 1)
            >>> len(result)
            1
            >>> result[0]['call_date']
            datetime.date(2023, 1, 15)
        """
        date_idx = find_column(normalized_header, ["date"])
        amount_idx = find_column(normalized_header, ["amount", "value"])
        type_idx = find_column(normalized_header, ["call number", "call no", "call#", "call type", "type"])
        desc_idx = find_column(normalized_header, ["description", "details", "notes"])

        if date_idx is None or amount_idx is None:
            return []

        parsed_rows: List[Dict[str, Any]] = []
        for row in rows:
            if should_skip_row(row):
                continue

            call_date = parse_date(safe_get(row, date_idx), self._DATE_FORMATS)
            amount = parse_amount(safe_get(row, amount_idx))

            if not call_date or amount is None:
                continue

            call_type = safe_get(row, type_idx) if type_idx is not None else None
            description = safe_get(row, desc_idx) if desc_idx is not None else None

            parsed_rows.append(
                {
                    "call_date": call_date,
                    "call_type": call_type or None,
                    "amount": amount,
                    "description": description or None,
                    "page_number": page_number,
                    "header": raw_header,
                }
            )

        return parsed_rows

    def _parse_distributions(
        self,
        raw_header: List[str],
        normalized_header: List[str],
        rows: List[List[str]],
        page_number: int,
    ) -> List[Dict[str, Any]]:
        """
        Parse distribution table rows into structured data.
        
        This method looks for specific column headers in the distribution table
        and maps them to standardized field names. It extracts key information
        such as the distribution date, amount, type, and whether it's recallable.
        
        Args:
            raw_header: Original header row from the table
            normalized_header: Normalized header for keyword matching
            rows: Data rows to parse (excluding header)
            page_number: Page number where the table was found
            
        Returns:
            List of dictionaries, each containing:
            - distribution_date: date object for the distribution
            - distribution_type: string describing the type of distribution
            - amount: Decimal object representing the distribution amount
            - is_recallable: boolean indicating if the distribution is recallable
            - description: Optional description text
            - page_number: Original page number
            - header: Original header row
            
        Example:
            >>> parser = TableParser()
            >>> header = ["Date", "Type", "Amount", "Recallable", "Description"]
            >>> rows = [["2023-06-15", "Return of Capital", "$500.00", "Yes", "Investment return"]]
            >>> result = parser._parse_distributions(header, [h.lower() for h in header], rows, 1)
            >>> len(result)
            1
            >>> result[0]['is_recallable']
            True
        """
        date_idx = find_column(normalized_header, ["date"])
        amount_idx = find_column(normalized_header, ["amount", "value"])
        type_idx = find_column(normalized_header, ["type", "distribution type"])
        recall_idx = find_column(normalized_header, ["recallable", "recall"])
        desc_idx = find_column(normalized_header, ["description", "details", "notes"])

        if date_idx is None or amount_idx is None:
            return []

        parsed_rows: List[Dict[str, Any]] = []
        for row in rows:
            if should_skip_row(row):
                continue

            distribution_date = parse_date(safe_get(row, date_idx), self._DATE_FORMATS)
            amount = parse_amount(safe_get(row, amount_idx))

            if not distribution_date or amount is None:
                continue

            dist_type = safe_get(row, type_idx) if type_idx is not None else None
            recallable_raw = safe_get(row, recall_idx) if recall_idx is not None else None
            description = safe_get(row, desc_idx) if desc_idx is not None else None

            parsed_rows.append(
                {
                    "distribution_date": distribution_date,
                    "distribution_type": dist_type or None,
                    "amount": amount,
                    "is_recallable": parse_bool(recallable_raw),
                    "description": description or None,
                    "page_number": page_number,
                    "header": raw_header,
                }
            )

        return parsed_rows

    def _parse_adjustments(
        self,
        raw_header: List[str],
        normalized_header: List[str],
        rows: List[List[str]],
        page_number: int,
    ) -> List[Dict[str, Any]]:
        """
        Parse adjustment table rows into structured data.
        
        This method looks for specific column headers in the adjustment table
        and maps them to standardized field names. It extracts key information
        such as the adjustment date, amount, type, and category.
        
        The method also determines if an adjustment is related to contributions
        based on the type and category fields.
        
        Args:
            raw_header: Original header row from the table
            normalized_header: Normalized header for keyword matching
            rows: Data rows to parse (excluding header)
            page_number: Page number where the table was found
            
        Returns:
            List of dictionaries, each containing:
            - adjustment_date: date object for the adjustment
            - adjustment_type: string describing the type of adjustment
            - category: category of the adjustment
            - amount: Decimal object representing the adjustment amount
            - is_contribution_adjustment: boolean indicating if this affects contributions
            - description: Optional description text
            - page_number: Original page number
            - header: Original header row
            
        Example:
            >>> parser = TableParser()
            >>> header = ["Date", "Type", "Amount", "Category", "Description"]
            >>> rows = [["2023-03-01", "Fee Adjustment", "$100.00", "Management Fee", "Q1 fee adjustment"]]
            >>> result = parser._parse_adjustments(header, [h.lower() for h in header], rows, 1)
            >>> len(result)
            1
            >>> result[0]['is_contribution_adjustment']
            True
        """
        date_idx = find_column(normalized_header, ["date"])
        amount_idx = find_column(normalized_header, ["amount", "value"])
        type_idx = find_column(normalized_header, ["type", "adjustment type"])
        category_idx = find_column(normalized_header, ["category"])
        desc_idx = find_column(normalized_header, ["description", "details", "notes"])

        if date_idx is None or amount_idx is None:
            return []

        parsed_rows: List[Dict[str, Any]] = []
        for row in rows:
            if should_skip_row(row):
                continue

            adjustment_date = parse_date(safe_get(row, date_idx), self._DATE_FORMATS)
            amount = parse_amount(safe_get(row, amount_idx))

            if not adjustment_date or amount is None:
                continue

            adj_type = safe_get(row, type_idx) if type_idx is not None else None
            category = safe_get(row, category_idx) if category_idx is not None else None
            description = safe_get(row, desc_idx) if desc_idx is not None else None

            parsed_rows.append(
                {
                    "adjustment_date": adjustment_date,
                    "adjustment_type": adj_type or None,
                    "category": category or None,
                    "amount": amount,
                    "is_contribution_adjustment": is_contribution_adjustment(adj_type, category),
                    "description": description or None,
                    "page_number": page_number,
                    "header": raw_header,
                }
            )

        return parsed_rows
