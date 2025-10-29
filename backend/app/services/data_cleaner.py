"""
Utilities for validating and cleaning parsed table rows before persistence.

This module provides the TableDataCleaner class which is responsible for
validating, cleaning, and normalizing parsed table data before storage.
The cleaner handles various types of financial tables including capital calls,
distributions, and adjustments.

Key features:
- Validation of required fields and data types
- Normalization of string values and amounts
- Deduplication of table rows
- Error reporting and logging
- Support for different financial table types

The module ensures data integrity by validating dates, amounts, and other
field types while maintaining data quality for accurate financial reporting.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from app.helpers.table_utils import parse_amount, parse_bool, parse_date

logger = logging.getLogger(__name__)

# Decimal constant for rounding amounts to 2 decimal places
TWO_PLACES = Decimal("0.01")

# Type alias for cleaned table rows data structure
TableRows = Dict[str, List[Dict[str, Any]]]

# Type alias for validation error reports
ValidationReport = Dict[str, List[str]]


class TableDataCleaner:
    """
    Validates and cleans parsed document tables before persistence.
    
    This class processes parsed table data by validating required fields,
    normalizing values, removing duplicates, and generating validation reports.
    It handles different types of financial tables including capital calls,
    distributions, and adjustments with table-specific validation rules.
    
    The cleaner performs the following operations:
    - Type coercion and validation for dates, amounts, and booleans
    - String normalization and cleaning
    - Duplicate detection and removal
    - Error reporting for invalid records
    
    Example:
        >>> cleaner = TableDataCleaner()
        >>> raw_tables = {
        ...     "capital_calls": [
        ...         {"call_date": "2023-01-15", "amount": "$1000.00", "description": "Q1 call"}
        ...     ]
        ... }
        >>> cleaned, issues = cleaner.clean(raw_tables)
        >>> len(cleaned["capital_calls"])
        1
        >>> len(issues["capital_calls"])
        0
    """

    def clean(self, tables: Mapping[str, Iterable[Dict[str, Any]]]) -> Tuple[TableRows, ValidationReport]:
        """
        Clean and validate tables, returning cleaned data and validation issues.
        
        This is the main entry point for table cleaning. It processes each table
        according to its type, applying appropriate validation and cleaning logic.
        Invalid rows are discarded and reported in the validation issues, while
        valid rows are cleaned and deduplicated.
        
        Args:
            tables: Mapping of table type names to sequences of uncleaned rows.
                   Expected table types: "capital_calls", "distributions", "adjustments"
        
        Returns:
            A tuple containing:
            - Cleaned table data with validated and normalized rows
            - Validation report with errors for each table type
            
        Example:
            >>> cleaner = TableDataCleaner()
            >>> raw_tables = {
            ...     "capital_calls": [
            ...         {"call_date": "2023-01-15", "amount": "$1000.00", "description": "Q1 call"},
            ...         {"call_date": "2023-01-15", "amount": "$1000.00", "description": "Q1 call"}  # duplicate
            ...     ],
            ...     "distributions": [
            ...         {"distribution_date": "2023-06-15", "amount": "$500.00"}
            ...     ]
            ... }
            >>> cleaned, issues = cleaner.clean(raw_tables)
            >>> len(cleaned["capital_calls"])  # Only one due to deduplication
            1
            >>> len(issues["distributions"])  # No issues expected
            0
        """
        cleaned: TableRows = {table_type: [] for table_type in tables}
        issues: ValidationReport = {table_type: [] for table_type in tables}
        seen: Dict[str, set] = {table_type: set() for table_type in tables}

        for table_type, rows in tables.items():
            rows_count = 0
            valid_count = 0
            invalid_count = 0
            duplicate_count = 0
            
            for row in rows or []:
                rows_count += 1
                handler = getattr(self, f"_clean_{table_type}", None)
                if not handler:
                    cleaned[table_type].append(row)
                    continue

                cleaned_row, error = handler(row)
                if error:
                    issues[table_type].append(error)
                    logger.debug("Discarding %s row: %s (%s)", table_type, row, error)
                    invalid_count += 1
                    continue

                dedupe_key = self._dedupe_key(table_type, cleaned_row)
                if dedupe_key in seen[table_type]:
                    logger.debug("Dropping duplicate %s row: %s", table_type, cleaned_row)
                    duplicate_count += 1
                    continue

                seen[table_type].add(dedupe_key)
                cleaned[table_type].append(cleaned_row)
                valid_count += 1

            logger.info(
                "Processed %s table: %d total, %d valid, %d invalid, %d duplicates", 
                table_type, rows_count, valid_count, invalid_count, duplicate_count
            )

        return cleaned, issues

    # ------------------------------------------------------------------ #
    # Table-specific cleaners
    # ------------------------------------------------------------------ #
    def _clean_capital_calls(self, row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Clean and validate a capital calls table row.
        
        This method validates that the required fields (call_date, amount) are present
        and valid, and that the amount is not zero. It normalizes optional fields.
        
        Args:
            row: Raw capital calls row to clean
            
        Returns:
            A tuple of (cleaned_row, error_message) where:
            - cleaned_row: Validated and cleaned dictionary, or None if invalid
            - error_message: String error message if validation failed, else None
            
        Example:
            >>> cleaner = TableDataCleaner()
            >>> row = {
            ...     "call_date": "2023-01-15",
            ...     "amount": "$1,000.00",
            ...     "call_type": "Regular",
            ...     "description": "Q1 capital call"
            ... }
            >>> cleaned, error = cleaner._clean_capital_calls(row)
            >>> cleaned["call_date"]
            datetime.date(2023, 1, 15)
            >>> cleaned["amount"]
            Decimal('1000.00')
            >>> error is None
            True
        """
        call_date = self._coerce_date(row.get("call_date"))
        amount = self._coerce_amount(row.get("amount"), allow_negative=False)

        if not call_date:
            return None, "missing or invalid call_date"
        if amount is None:
            return None, "missing or invalid amount"
        if amount == Decimal("0.00"):
            return None, "capital call amount cannot be zero"

        cleaned = {
            "call_date": call_date,
            "call_type": self._normalize_str(row.get("call_type")),
            "amount": amount,
            "description": self._normalize_str(row.get("description")),
        }
        return cleaned, None

    def _clean_distributions(self, row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Clean and validate a distributions table row.
        
        This method validates that the required fields (distribution_date, amount) 
        are present and valid, and that the amount is not zero. It normalizes 
        optional fields and handles the is_recallable boolean field.
        
        Args:
            row: Raw distributions row to clean
            
        Returns:
            A tuple of (cleaned_row, error_message) where:
            - cleaned_row: Validated and cleaned dictionary, or None if invalid
            - error_message: String error message if validation failed, else None
            
        Example:
            >>> cleaner = TableDataCleaner()
            >>> row = {
            ...     "distribution_date": "2023-06-15",
            ...     "amount": "$500.00",
            ...     "distribution_type": "Return of Capital",
            ...     "is_recallable": "yes",
            ...     "description": "Investment return"
            ... }
            >>> cleaned, error = cleaner._clean_distributions(row)
            >>> cleaned["distribution_date"]
            datetime.date(2023, 6, 15)
            >>> cleaned["is_recallable"]
            True
            >>> error is None
            True
        """
        distribution_date = self._coerce_date(row.get("distribution_date"))
        amount = self._coerce_amount(row.get("amount"), allow_negative=False)

        if not distribution_date:
            return None, "missing or invalid distribution_date"
        if amount is None:
            return None, "missing or invalid amount"
        if amount == Decimal("0.00"):
            return None, "distribution amount cannot be zero"

        is_recallable = self._coerce_bool(row.get("is_recallable"))
        cleaned = {
            "distribution_date": distribution_date,
            "distribution_type": self._normalize_str(row.get("distribution_type")),
            "amount": amount,
            "is_recallable": is_recallable,
            "description": self._normalize_str(row.get("description")),
        }
        return cleaned, None

    def _clean_adjustments(self, row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Clean and validate an adjustments table row.
        
        This method validates that the required fields (adjustment_date, amount) 
        are present and valid. For adjustments, amounts can be negative (allow_negative=True).
        It normalizes optional fields and handles boolean fields.
        
        Args:
            row: Raw adjustments row to clean
            
        Returns:
            A tuple of (cleaned_row, error_message) where:
            - cleaned_row: Validated and cleaned dictionary, or None if invalid
            - error_message: String error message if validation failed, else None
            
        Example:
            >>> cleaner = TableDataCleaner()
            >>> row = {
            ...     "adjustment_date": "2023-03-01",
            ...     "amount": "-$100.00",  # Negative adjustment
            ...     "adjustment_type": "Fee Adjustment",
            ...     "category": "Management Fee",
            ...     "is_contribution_adjustment": "true",
            ...     "description": "Q1 fee adjustment"
            ... }
            >>> cleaned, error = cleaner._clean_adjustments(row)
            >>> cleaned["adjustment_date"]
            datetime.date(2023, 3, 1)
            >>> cleaned["amount"]
            Decimal('-100.00')
            >>> error is None
            True
        """
        adjustment_date = self._coerce_date(row.get("adjustment_date"))
        amount = self._coerce_amount(row.get("amount"), allow_negative=True)

        if not adjustment_date:
            return None, "missing or invalid adjustment_date"
        if amount is None:
            return None, "missing or invalid amount"

        cleaned = {
            "adjustment_date": adjustment_date,
            "adjustment_type": self._normalize_str(row.get("adjustment_type")),
            "category": self._normalize_str(row.get("category")),
            "amount": amount,
            "is_contribution_adjustment": self._coerce_bool(row.get("is_contribution_adjustment")),
            "description": self._normalize_str(row.get("description")),
        }
        return cleaned, None

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize_str(value: Any) -> Optional[str]:
        """
        Normalize a string value by stripping whitespace and handling empty values.
        
        This method converts a value to string, trims whitespace, and returns
        None for empty strings.
        
        Args:
            value: Input value to normalize
            
        Returns:
            Normalized string or None if empty after trimming
            
        Example:
            >>> TableDataCleaner._normalize_str("  test  ")
            'test'
            >>> TableDataCleaner._normalize_str("")
            >>> TableDataCleaner._normalize_str("   ")
            >>> TableDataCleaner._normalize_str(None)
        """
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        """
        Coerce any value to a boolean with proper handling of string representations.
        
        This method properly handles boolean values, string representations
        ("yes", "no", etc.), and other types using the parse_bool utility.
        
        Args:
            value: Input value to convert to boolean
            
        Returns:
            Boolean representation of the input value
            
        Example:
            >>> TableDataCleaner._coerce_bool(True)
            True
            >>> TableDataCleaner._coerce_bool("yes")
            True
            >>> TableDataCleaner._coerce_bool("false")
            False
            >>> TableDataCleaner._coerce_bool(1)
            True
            >>> TableDataCleaner._coerce_bool(0)
            False
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return parse_bool(value)
        return bool(value)

    def _coerce_date(self, value: Any) -> Optional[date]:
        """
        Coerce a value to a date object with proper type handling.
        
        This method handles various input types including date objects, datetime 
        objects (converting to date), and string representations.
        
        Args:
            value: Input value to convert to date
            
        Returns:
            Date object or None if conversion fails
            
        Example:
            >>> from datetime import date
            >>> cleaner = TableDataCleaner()
            >>> cleaner._coerce_date("2023-01-15")
            datetime.date(2023, 1, 15)
            >>> cleaner._coerce_date(date(2023, 1, 15))
            datetime.date(2023, 1, 15)
        """
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return parse_date(value)
        return None

    def _coerce_amount(self, value: Any, *, allow_negative: bool) -> Optional[Decimal]:
        """
        Coerce a value to a Decimal amount with proper validation and rounding.
        
        This method handles various input types (Decimal, int, float, string) and
        applies validation including zero checks and negative value restrictions.
        
        Args:
            value: Input value to convert to Decimal amount
            allow_negative: Whether negative amounts are permitted
            
        Returns:
            Decimal amount rounded to 2 decimal places, or None if invalid
            
        Example:
            >>> cleaner = TableDataCleaner()
            >>> cleaner._coerce_amount("$1,234.56", allow_negative=False)
            Decimal('1234.56')
            >>> cleaner._coerce_amount("-$100.00", allow_negative=True)
            Decimal('-100.00')
            >>> cleaner._coerce_amount("-$100.00", allow_negative=False)  # Not allowed
        """
        if value is None:
            return None

        if isinstance(value, Decimal):
            amount = value
        elif isinstance(value, (int, float)):
            amount = Decimal(str(value))
        elif isinstance(value, str):
            amount = self._parse_amount_string(value)
        else:
            return None

        if amount is None:
            return None

        try:
            amount = amount.quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return None

        if not allow_negative and amount < Decimal("0.00"):
            return None
        return amount

    @staticmethod
    def _parse_amount_string(value: str) -> Optional[Decimal]:
        """
        Parse a string value to a Decimal amount using the table utilities.
        
        This helper method normalizes string input and uses the parse_amount
        utility to handle various amount formats.
        
        Args:
            value: String representation of an amount
            
        Returns:
            Parsed Decimal amount or None if parsing fails
            
        Example:
            >>> TableDataCleaner._parse_amount_string("$1,000.00")
            Decimal('1000.00')
            >>> TableDataCleaner._parse_amount_string("(500.00)")  # Negative in parentheses
            Decimal('-500.00')
        """
        text = value.strip()
        if not text:
            return None
        parsed = parse_amount(text)
        if parsed is None:
            return None
        return parsed

    @staticmethod
    def _dedupe_key(table_type: str, row: Dict[str, Any]) -> Tuple[Any, ...]:
        """
        Generate a deduplication key for a table row based on its type.
        
        This method creates consistent keys for identifying duplicate rows
        according to the rules for each table type.
        
        Args:
            table_type: Type of table ("capital_calls", "distributions", "adjustments")
            row: Table row dictionary
            
        Returns:
            Tuple of values that form the deduplication key
            
        Example:
            >>> row = {
            ...     "call_date": "2023-01-15",
            ...     "amount": Decimal("1000.00"),
            ...     "call_type": "Regular",
            ...     "description": "Q1 call"
            ... }
            >>> key = TableDataCleaner._dedupe_key("capital_calls", row)
            >>> len(key)  # Call date, amount, type, and description
            4
        """
        if table_type == "capital_calls":
            return (
                row.get("call_date"),
                row.get("amount"),
                row.get("call_type"),
                row.get("description"),
            )
        if table_type == "distributions":
            return (
                row.get("distribution_date"),
                row.get("amount"),
                row.get("distribution_type"),
                row.get("description"),
            )
        if table_type == "adjustments":
            return (
                row.get("adjustment_date"),
                row.get("amount"),
                row.get("adjustment_type"),
                row.get("category"),
                row.get("description"),
            )
        # For unknown table types, create a consistent key from the items
        return tuple(sorted(row.items()))
