"""
Shared table parsing helpers used by `TableParser`.

This module provides a collection of utility functions for parsing and manipulating
table data extracted from financial documents, particularly investment-related PDFs.
The functions are designed to be reusable across different table parsing contexts
and can be unit tested independently from the main parser class.

The module includes functionality for:
- Normalizing and cleaning table data
- Finding columns by keywords
- Parsing dates, amounts, and boolean values
- Identifying rows that should be skipped during parsing
- Determining if adjustments affect contributions

These functions are optimized for financial document processing and handle
various formats commonly found in investment statements and fund reports.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, List, Optional, Sequence

__all__ = [
    "DEFAULT_DATE_FORMATS",
    "clean_table",
    "find_column",
    "is_contribution_adjustment",
    "normalize_cell",
    "normalize_header",
    "parse_amount",
    "parse_bool",
    "parse_date",
    "safe_get",
    "should_skip_row",
]

logger = logging.getLogger(__name__)

# Pre-compiled regex patterns keep repeated parsing cheap.
_MONTH_YEAR_PATTERN = re.compile(r"[,\s]+")
_DIGIT_PATTERN = re.compile(r"-?\d+(?:\.\d+)?")
_AMOUNT_CLEAN_PATTERN = re.compile(r"[^\d.-]+")

# Default date formats cover the most common investment document patterns.
DEFAULT_DATE_FORMATS: Sequence[str] = (
    "%Y-%m-%d",     # ISO format: 2023-01-15
    "%m/%d/%Y",     # US format: 01/15/2023
    "%d/%m/%Y",     # EU format: 15/01/2023
    "%b %d, %Y",    # Abbrev month: Jan 15, 2023
    "%B %d, %Y",    # Full month: January 15, 2023
)


def normalize_cell(cell: Any) -> str:
    """
    Normalize a table cell to a trimmed string representation.
    
    This function converts any input value to a string and trims whitespace.
    It handles None values by returning an empty string.
    
    Args:
        cell: Input value of any type (str, int, float, None, etc.)
        
    Returns:
        Trimmed string representation of the input value
        
    Example:
        >>> normalize_cell("  test  ")
        'test'
        >>> normalize_cell(None)
        ''
        >>> normalize_cell(123)
        '123'
        >>> normalize_cell(42.5)
        '42.5'
    """
    if cell is None:
        return ""
    return str(cell).strip()


def normalize_header(cell: str) -> str:
    """
    Normalize header cell for deterministic keyword matching.
    
    This function converts a header cell to lowercase after normalizing it,
    making subsequent keyword searches case-insensitive.
    
    Args:
        cell: Header cell value to normalize
        
    Returns:
        Lowercase, trimmed string version of the header cell
        
    Example:
        >>> normalize_header("DATE")
        'date'
        >>> normalize_header("  Call Type  ")
        'call type'
    """
    return normalize_cell(cell).lower()


def clean_table(table: Iterable[Iterable[Any]]) -> List[List[str]]:
    """
    Clean a table by normalizing cells and removing empty rows.
    
    This function processes the raw table data by:
    1. Normalizing each cell to a trimmed string using normalize_cell
    2. Removing rows that contain no meaningful content (all empty cells)
    
    Args:
        table: 2D iterable representing the table data to clean
        
    Returns:
        2D list with normalized cells and no empty rows
        
    Example:
        >>> raw_table = [["   ", "  ", ""], ["Date", "Amount"], ["2023-01-01", "$100"]]
        >>> cleaned = clean_table(raw_table)
        >>> len(cleaned)  # Excludes the empty first row
        2
        >>> cleaned[0]
        ['Date', 'Amount']
    """
    cleaned: List[List[str]] = []
    for row in table or []:
        normalized_row = [normalize_cell(cell) for cell in row]
        if any(normalized_row):
            cleaned.append(normalized_row)
    return cleaned


def find_column(header: Sequence[str], keywords: Sequence[str]) -> Optional[int]:
    """
    Find the index of the first column header that matches any of the given keywords.
    
    This function searches through the header row for columns that contain
    any of the specified keywords. It performs case-insensitive matching
    and returns the index of the first match.
    
    Args:
        header: List of header column names to search through
        keywords: List of keywords to look for in the header
        
    Returns:
        Index of the first column that matches any keyword, or None if no match
        
    Example:
        >>> header = ["Call Date", "Type", "Amount"]
        >>> idx = find_column(header, ["date"])
        >>> idx
        0
        >>> idx = find_column(header, ["amount"])
        >>> idx
        2
        >>> idx = find_column(header, ["nonexistent"])
        >>> idx is None
        True
    """
    # Convert keywords to a frozenset for faster lookups
    keyword_set = frozenset(kw.lower() for kw in keywords)
    
    for idx, column in enumerate(header):
        normalized_column = column.lower()
        if any(keyword in normalized_column for keyword in keyword_set):
            return idx
    return None


def safe_get(row: Sequence[Any], index: Optional[int]) -> Optional[str]:
    """
    Safely retrieve a value from a row by index.
    
    This function prevents index errors by checking bounds before accessing
    the row data. It returns None if the index is invalid or the value is empty.
    
    Args:
        row: Sequence representing a table row
        index: Index of the cell to retrieve
        
    Returns:
        Cell value as string if valid and non-empty, otherwise None
        
    Example:
        >>> row = ["2023-01-01", "Type", "$100"]
        >>> safe_get(row, 0)
        '2023-01-01'
        >>> safe_get(row, 10)  # Out of bounds
        None
        >>> safe_get(row, None)  # Invalid index
        None
    """
    if index is None or index >= len(row):
        return None
    value = row[index]
    return value if value else None


def should_skip_row(row: Sequence[Any]) -> bool:
    """
    Determine whether to skip a data row during parsing.
    
    This function identifies rows that should be skipped, such as:
    - Empty rows
    - Header-like rows (e.g., rows starting with "date", "type")
    - Total rows (e.g., rows containing "total", "subtotal")
    
    Args:
        row: Sequence representing a table row to evaluate
        
    Returns:
        True if the row should be skipped, False otherwise
        
    Example:
        >>> should_skip_row(["Date", "Type", "Amount"])  # Header row
        True
        >>> should_skip_row(["2023-01-01", "Regular", "$100"])  # Data row
        False
        >>> should_skip_row(["Total", "$1000"])  # Total row
        True
        >>> should_skip_row([])  # Empty row
        True
    """
    if not row:
        return True

    # Use generator to avoid creating intermediate list when possible
    normalized = [cell_val for cell in row 
                  if (cell_val := normalize_cell(cell).lower())]
    if not normalized:
        return True

    first_cell = normalized[0]
    if first_cell in {"date", "type"}:
        return True

    # Use set lookup for faster comparison
    skip_values = {"total", "subtotal"}
    if any(value in skip_values for value in normalized):
        return True

    return False


def parse_date(value: Optional[str], formats: Sequence[str] | None = None) -> Optional[date]:
    """
    Parse a date string into a date object using multiple formats.
    
    This function attempts to parse date strings in various common formats
    including:
    - YYYY-MM-DD
    - MM/DD/YYYY
    - DD/MM/YYYY
    - Month DD, YYYY (Jan 01, 2024)
    - Month DD, YYYY (January 01, 2024)
    - Month YYYY (Jan 2024) - assumes first day of month
    
    Args:
        value: String representation of a date to parse
        formats: Optional sequence of date formats to try. If None, uses DEFAULT_DATE_FORMATS
        
    Returns:
        Date object if parsing succeeds, None otherwise
        
    Example:
        >>> parse_date("2023-01-15")
        datetime.date(2023, 1, 15)
        >>> parse_date("01/15/2023")
        datetime.date(2023, 1, 15)
        >>> parse_date("Jan 2023")  # Flexible format
        datetime.date(2023, 1, 1)
        >>> parse_date("invalid date") is None
        True
    """
    if not value:
        return None

    formats = formats or DEFAULT_DATE_FORMATS
    text = value.strip()
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    try:
        parts = _MONTH_YEAR_PATTERN.split(text)
        if len(parts) == 2:
            month, year = parts
            return datetime.strptime(f"{month} 1 {year}", "%b %d %Y").date()
    except ValueError:
        pass

    return None


def parse_amount(value: Optional[str]) -> Optional[Decimal]:
    """
    Parse an amount string into Decimal with handling for various formats.
    
    This function handles various amount formats including:
    - Currency symbols ($, etc.)
    - Comma separators
    - Negative values in parentheses (e.g., "(1,234.56)")
    - Negative values with minus sign
    - N/A values like "n/a", "na", "-",
    - Partially extractable amounts from text
    
    Args:
        value: String representation of an amount to parse
        
    Returns:
        Decimal object if parsing succeeds, None otherwise
        
    Example:
        >>> parse_amount("$1,234.56")
        Decimal('1234.56')
        >>> parse_amount("(500.00)")  # Negative in parentheses
        Decimal('-500.00')
        >>> parse_amount("-$200.00")  # Negative with minus
        Decimal('-200.00')
        >>> parse_amount("n/a") is None
        True
        >>> parse_amount("text with 123.45 inside")  # Extracts number
        Decimal('123.45')
    """
    if value is None:
        return None

    text = value.strip()
    if not text or text.lower() in {"n/a", "na", "-", ""}:
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    if text.startswith("-"):
        negative = True
        text = text[1:]

    text = _AMOUNT_CLEAN_PATTERN.sub("", text)
    try:
        amount = Decimal(text)
    except InvalidOperation:
        digits = _DIGIT_PATTERN.findall(text)
        if not digits:
            return None
        try:
            amount = Decimal(digits[0])
        except InvalidOperation:
            return None

    if negative:
        amount = -amount

    return amount


def parse_bool(value: Optional[str]) -> bool:
    """
    Parse boolean-like strings into boolean values.
    
    This function handles common ways of representing boolean values:
    - True: "yes", "y", "true", "1", case-insensitive
    - False: "no", "n", "false", "0", case-insensitive
    - Default: False for any other value
    
    Args:
        value: String representation of a boolean value
        
    Returns:
        Boolean value based on the input string
        
    Example:
        >>> parse_bool("yes")
        True
        >>> parse_bool("No")
        False
        >>> parse_bool("true")
        True
        >>> parse_bool("maybe")  # Not in known values
        False
        >>> parse_bool(None)
        False
    """
    if value is None:
        return False

    normalized = value.strip().lower()
    if normalized in {"yes", "y", "true", "1"}:
        return True
    if normalized in {"no", "n", "false", "0"}:
        return False

    return False


def is_contribution_adjustment(adj_type: Optional[str], category: Optional[str]) -> bool:
    """
    Determine whether an adjustment impacts contributions based on type and category.
    
    This function determines if an adjustment affects contributions by looking for
    keywords related to contributions, capital calls, fees, or management in
    the adjustment type and category fields.
    
    Args:
        adj_type: String describing the adjustment type (optional)
        category: String describing the adjustment category (optional)
        
    Returns:
        True if the adjustment appears to impact contributions, False otherwise
        
    Example:
        >>> is_contribution_adjustment("Capital Call Adjustment", "Fees")
        True
        >>> is_contribution_adjustment("Management Fee", "Fee")
        True
        >>> is_contribution_adjustment("Distribution", "Income")
        False
        >>> is_contribution_adjustment(None, "Fee")
        True
        >>> is_contribution_adjustment("Regular", "Income")
        False
    """
    candidates = " ".join(filter(None, [adj_type, category])).lower()
    keywords = {"contribution", "capital call", "fee", "management"}
    return any(keyword in candidates for keyword in keywords)