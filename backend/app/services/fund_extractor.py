"""
Fund information extraction service.

This module provides functionality to extract fund information from PDF documents
including fund name, GP name, vintage year, and other relevant metadata.
"""
import re
from typing import Dict, Optional, List, Tuple, Any
from datetime import datetime


class FundExtractor:
    """
    Service class for extracting fund information from document text content.
    
    This class uses pattern matching to identify and extract fund-related 
    information from PDF text content, including:
    - Fund name
    - General Partner (GP) name
    - Vintage year
    - Fund size
    - Report date
    """
    
    def __init__(self):
        # Define regex patterns for different fund attributes
        self.patterns = {
            'fund_name': [
                r'Fund\s+Name\s*[:\-\s]+([^\n\r]+)',
                r'Name\s+of\s+Fund\s*[:\-\s]+([^\n\r]+)',
                r'Fund[:\-\s]+([^\n\r]+)',
                r'([^\n\r]+)\s+FUND',
                r'([^\n\r]+)\s+Fund',
            ],
            'gp_name': [
                r'GP[:\-\s]+([^\n\r]+)',
                r'General\s+Partner[:\-\s]+([^\n\r]+)',
                r'GP\s+Name[:\-\s]+([^\n\r]+)',
                r'Managed\s+by[:\-\s]+([^\n\r]+)',
                r'Investment\s+Manager[:\-\s]+([^\n\r]+)',
            ],
            'vintage_year': [
                r'Vintage[:\-\s]+(\d{4})',
                r'Vintage\s+Year[:\-\s]+(\d{4})',
                r'Inception[:\-\s]+(\d{4})',
                r'(\d{4})\s+Fund',
            ],
            'fund_size': [
                r'Fund\s+Size[:\-\s]+([\$€£¥\w\s,\.]+)',
                r'Total\s+Fund\s+Size[:\-\s]+([\$€£¥\w\s,\.]+)',
                r'Commitment[:\-\s]+([\$€£¥\w\s,\.]+)',
                r'Capital[:\-\s]+([\$€£¥\w\s,\.]+)',
            ],
            'report_date': [
                r'Report\s+Date[:\-\s]+([^\n\r]+)',
                r'As\s+of[:\-\s]+([^\n\r]+)',
                r'Date[:\-\s]+([^\n\r]+)',
            ]
        }
    
    def extract_fund_info_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract fund information from text content.

        Args:
            text: The text content extracted from the PDF document

        Returns:
            Dictionary containing extracted fund information
        """
        fund_info = {}

        # Clean the text but preserve line breaks to prevent cross-section matches
        # Only collapse multiple spaces within lines
        clean_text = re.sub(r' +', ' ', text)

        # Extract each field using defined patterns
        for field, patterns in self.patterns.items():
            for pattern in patterns:
                match = re.search(pattern, clean_text, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    if value and field not in fund_info:  # Don't overwrite if already found
                        # Limit extracted value length to prevent capturing entire tables
                        # Fund names and GP names are typically < 100 chars
                        if field in ['fund_name', 'gp_name']:
                            # Take only the first line and limit to 200 chars
                            value = value.split('\n')[0].split('\r')[0][:200]

                        fund_info[field] = self._clean_value(value, field)
                        break  # Move to next field after first match

        return fund_info
    
    def _clean_value(self, value: str, field_type: str) -> Any:
        """
        Clean and normalize extracted values based on field type.
        
        Args:
            value: The raw extracted value
            field_type: The type of field being cleaned
            
        Returns:
            Cleaned and normalized value
        """
        value = value.strip(' :-\t\n\r')
        
        if field_type == 'vintage_year':
            # Extract 4-digit year
            year_match = re.search(r'(\d{4})', value)
            if year_match:
                year = int(year_match.group(1))
                # Validate it's a reasonable year
                current_year = datetime.now().year
                if 1900 <= year <= current_year + 1:
                    return year
        
        elif field_type == 'report_date':
            # Try to parse date - accept various formats
            date_patterns = [
                r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{4})',  # MM/DD/YYYY or MM-DD-YYYY
                r'(\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2})',  # YYYY/MM/DD or YYYY-MM-DD
                r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})',    # Month DD, YYYY
                r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})',      # DD Month YYYY
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, value)
                if match:
                    date_str = match.group(1)
                    # Validate and return the date string
                    # For now, just return the found date string
                    return date_str
        
        return value


def extract_fund_info_from_segments(text_segments: List['TextSegment']) -> Dict[str, Any]:
    """
    Extract fund information from a list of text segments.

    Args:
        text_segments: List of text segments extracted from the PDF

    Returns:
        Dictionary containing extracted fund information
    """
    extractor = FundExtractor()
    # Join with newlines to preserve document structure and prevent
    # regex patterns from matching across unrelated sections
    combined_text = "\n".join([seg.text for seg in text_segments])
    return extractor.extract_fund_info_from_text(combined_text)


def extract_fund_info_from_tables(table_candidates: List['TableCandidate']) -> Dict[str, Any]:
    """
    Extract fund information that might be present in table headers or footers.
    
    Args:
        table_candidates: List of table candidates extracted from the PDF
        
    Returns:
        Dictionary containing extracted fund information
    """
    extractor = FundExtractor()
    combined_table_text = ""
    
    for table in table_candidates:
        # Check headers and first few rows for fund information
        if table.data:
            # Combine first few rows as potential source of fund info
            for i, row in enumerate(table.data[:3]):  # Check first 3 rows
                if i < len(table.data):
                    combined_table_text += " ".join([str(cell) for cell in row if cell]) + " "
    
    return extractor.extract_fund_info_from_text(combined_table_text)