"""
Sample PDF Generator for Fund Performance Reports

This script creates a simple PDF with fund performance tables
that can be used for testing the document processing pipeline.

Usage:
    python create_sample_pdf.py

Requirements:
    pip install reportlab
"""

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from datetime import datetime


def create_sample_fund_report():
    """Create a sample fund performance report PDF"""
    
    filename = "Sample_Fund_Performance_Report.pdf"
    doc = SimpleDocTemplate(filename, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1a1a1a'),
        spaceAfter=30,
        alignment=1  # Center
    )
    
    title = Paragraph("Tech Ventures Fund III", title_style)
    story.append(title)
    
    subtitle = Paragraph("Quarterly Performance Report - Q4 2024", styles['Heading2'])
    story.append(subtitle)
    story.append(Spacer(1, 0.5*inch))
    
    # Fund Information
    info_text = """
    <b>Fund Name:</b> Tech Ventures Fund III<br/>
    <b>GP:</b> Tech Ventures Partners<br/>
    <b>Vintage Year:</b> 2023<br/>
    <b>Fund Size:</b> $100,000,000<br/>
    <b>Report Date:</b> December 31, 2024
    """
    story.append(Paragraph(info_text, styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Capital Calls Section
    story.append(Paragraph("<b>Capital Calls</b>", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    capital_calls_data = [
        ['Date', 'Call Number', 'Amount', 'Description'],
        ['2023-01-15', 'Call 1', '$5,000,000', 'Initial Capital Call'],
        ['2023-06-20', 'Call 2', '$3,000,000', 'Follow-on Investment'],
        ['2024-03-10', 'Call 3', '$2,000,000', 'Bridge Round Funding'],
        ['2024-09-15', 'Call 4', '$1,500,000', 'Additional Capital'],
    ]
    
    capital_table = Table(capital_calls_data, colWidths=[1.2*inch, 1.2*inch, 1.3*inch, 2.5*inch])
    capital_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
    ]))
    
    story.append(capital_table)
    story.append(Spacer(1, 0.5*inch))
    
    # Distributions Section
    story.append(Paragraph("<b>Distributions</b>", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    distributions_data = [
        ['Date', 'Type', 'Amount', 'Recallable', 'Description'],
        ['2023-12-15', 'Return of Capital', '$1,500,000', 'No', 'Exit: TechCo Inc'],
        ['2024-06-20', 'Income', '$500,000', 'No', 'Dividend Payment'],
        ['2024-09-10', 'Return of Capital', '$2,000,000', 'Yes', 'Partial Exit: DataCorp'],
        ['2024-12-20', 'Income', '$300,000', 'No', 'Year-end Distribution'],
    ]
    
    dist_table = Table(distributions_data, colWidths=[1*inch, 1.2*inch, 1.2*inch, 1*inch, 2*inch])
    dist_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
    ]))
    
    story.append(dist_table)
    story.append(Spacer(1, 0.5*inch))
    
    # Adjustments Section
    story.append(Paragraph("<b>Adjustments</b>", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    adjustments_data = [
        ['Date', 'Type', 'Amount', 'Description'],
        ['2024-01-15', 'Recallable Distribution', '-$500,000', 'Recalled distribution from Q4 2023'],
        ['2024-03-20', 'Capital Call Adjustment', '$100,000', 'Management fee adjustment'],
        ['2024-07-10', 'Contribution Adjustment', '-$50,000', 'Expense reimbursement'],
    ]
    
    adj_table = Table(adjustments_data, colWidths=[1.2*inch, 1.8*inch, 1.3*inch, 2.5*inch])
    adj_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
    ]))
    
    story.append(adj_table)
    story.append(Spacer(1, 0.5*inch))
    
    # Performance Summary
    story.append(Paragraph("<b>Performance Summary</b>", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    summary_text = """
    <b>Total Capital Called:</b> $11,500,000<br/>
    <b>Total Distributions:</b> $4,300,000<br/>
    <b>Net Paid-In Capital (PIC):</b> $11,050,000<br/>
    <b>Distributions to Paid-In (DPI):</b> 0.39<br/>
    <b>Internal Rate of Return (IRR):</b> 12.5%<br/>
    <b>Total Value to Paid-In (TVPI):</b> 1.45<br/>
    <br/>
    <b>Fund Strategy:</b> The fund focuses on early-stage technology companies in the SaaS, 
    fintech, and AI sectors. Our investment thesis centers on identifying companies with 
    strong product-market fit and scalable business models.
    <br/><br/>
    <b>Key Definitions:</b><br/>
    • <b>DPI (Distributions to Paid-In):</b> Total distributions divided by total paid-in capital. 
    Measures cash returned to investors.<br/>
    • <b>IRR (Internal Rate of Return):</b> The annualized rate of return that makes the net 
    present value of all cash flows equal to zero.<br/>
    • <b>TVPI (Total Value to Paid-In):</b> The sum of distributions and residual value divided 
    by paid-in capital. Measures total value creation.
    """
    
    story.append(Paragraph(summary_text, styles['Normal']))
    
    # Build PDF
    doc.build(story)
    print(f"✅ Sample PDF created: {filename}")
    print(f"\nExpected Metrics:")
    print(f"  - Total Capital Called: $11,500,000")
    print(f"  - Total Distributions: $4,300,000")
    print(f"  - Net PIC: $11,050,000 (after adjustments)")
    print(f"  - DPI: 0.39")
    print(f"  - IRR: ~12.5%")


if __name__ == "__main__":
    try:
        create_sample_fund_report()
    except ImportError:
        print("❌ Error: reportlab not installed")
        print("Install it with: pip install reportlab")
    except Exception as e:
        print(f"❌ Error creating PDF: {e}")
