# Sample Data Files

## Provided Files

### 1. `ILPA based Capital Accounting and Performance Metrics_ PIC, Net PIC, DPI, IRR.pdf`
- **Type**: Reference document
- **Purpose**: Explains fund performance metrics (PIC, DPI, IRR, TVPI)
- **Use for**: Testing text extraction and RAG (Retrieval Augmented Generation)
- **Contains**: Definitions, formulas, and explanations

## Generate Sample Fund Reports

### Quick Start

```bash
# Install dependencies
pip install reportlab

# Generate sample PDF
python create_sample_pdf.py
```

This will create `Sample_Fund_Performance_Report.pdf` with realistic fund data.

### What's Included in Generated PDF

The generated sample report contains:

#### Capital Calls Table
- 4 capital call entries
- Dates from 2023-2024
- Total: $11,500,000

#### Distributions Table
- 4 distribution entries
- Mix of recallable and non-recallable
- Total: $4,300,000

#### Adjustments Table
- 3 adjustment entries
- Includes recallable distributions and fee adjustments
- Net adjustment: -$450,000

#### Performance Metrics
- **Net PIC**: $11,050,000
- **DPI**: 0.39
- **IRR**: ~12.5%
- **TVPI**: 1.45

### Testing Your Implementation

Use these files to test:

1. **Document Upload**
   - Upload the generated PDF
   - Verify file is saved correctly
   - Check background task is triggered

2. **Table Extraction**
   - Verify all 3 tables are detected
   - Check table classification (capital calls, distributions, adjustments)
   - Validate data parsing (dates, amounts, descriptions)

3. **Database Storage**
   - Confirm records are inserted into PostgreSQL
   - Verify foreign key relationships
   - Check data types and constraints

4. **Vector Storage**
   - Verify text is chunked properly
   - Check embeddings are generated
   - Test FAISS index creation

5. **RAG Queries**
   - Ask: "What is the DPI of this fund?"
   - Ask: "What does DPI mean?"
   - Ask: "Show me all capital calls in 2024"
   - Ask: "What is the total amount distributed?"

### Expected Query Results

Based on the sample data:

**Calculation Queries:**
- "What is the DPI?" → 0.39
- "What is the IRR?" → ~12.5%
- "Total capital called?" → $11,500,000
- "Total distributions?" → $4,300,000

**Definition Queries:**
- "What is DPI?" → Should retrieve definition from text
- "What is IRR?" → Should explain Internal Rate of Return
- "What is TVPI?" → Should explain Total Value to Paid-In

**Data Queries:**
- "Capital calls in 2024?" → Should return 2 entries (March and September)
- "Recallable distributions?" → Should return 1 entry ($2M from DataCorp)
- "Latest distribution?" → December 20, 2024 ($300,000)

## Creating Your Own Test Data

If you want to create additional test PDFs:

### Option 1: Modify the Script
Edit `create_sample_pdf.py` to add more entries or change values.

### Option 2: Use Word/Google Docs
1. Create tables with the same structure
2. Export as PDF
3. Test with your parser

### Option 3: Use Online Tools
- Canva
- Adobe Express
- Google Docs

## Tips for Testing

1. **Start Simple**: Test with the provided sample first
2. **Edge Cases**: Create PDFs with:
   - Missing data
   - Unusual date formats
   - Large numbers
   - Negative amounts
3. **Multiple Funds**: Create PDFs for different funds to test isolation
4. **Malformed PDFs**: Test error handling with corrupted files

## Troubleshooting

### PDF Generation Fails
```bash
# Make sure reportlab is installed
pip install reportlab

# Check Python version (3.8+)
python --version
```

### Tables Not Extracted
- Ensure PDF has actual tables (not images)
- Check pdfplumber configuration
- Try with simpler table layouts first

### Incorrect Calculations
- Verify all transactions are parsed
- Check date parsing (different formats)
- Validate amount parsing (currency symbols, commas)

## Need Help?

- Check the main README.md for detailed instructions
- Review the CALCULATIONS.md for metric formulas
- See ARCHITECTURE.md for system design
