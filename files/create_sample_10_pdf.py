"""
Generate 10 Sample Fund Performance Reports (PDF)
- Struktur fungsi create_sample_fund_report() tidak diubah.
- Data di dalam tabel & ringkasan diambil dari KONSTANTA yang bisa diganti per iterasi.
- Nilai tabel & metrik (IRR/DPI/TVPI) dirandom untuk tiap fund.
"""

# ==== IMPORT ASLI (biarkan sama) ====
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from datetime import datetime

# ==== KONSTANTA YANG BISA DIGANTI2 (akan diisi dalam loop) ====
TITLE_FUND_NAME = "Tech Ventures Fund III"  # Judul paling atas
OUTPUT_FILENAME = "Sample_Fund_Performance_Report.pdf"  # Nama file output

INFO_TEXT = """
<b>Fund Name:</b> Tech Ventures Fund III<br/>
<b>GP:</b> Tech Ventures Partners<br/>
<b>Vintage Year:</b> 2023<br/>
<b>Fund Size:</b> $100,000,000<br/>
<b>Report Date:</b> December 31, 2024
"""

CAPITAL_CALLS_DATA = [
    ['Date', 'Call Number', 'Amount', 'Description'],
    ['2023-01-15', 'Call 1', '$5,000,000', 'Initial Capital Call'],
    ['2023-06-20', 'Call 2', '$3,000,000', 'Follow-on Investment'],
    ['2024-03-10', 'Call 3', '$2,000,000', 'Bridge Round Funding'],
    ['2024-09-15', 'Call 4', '$1,500,000', 'Additional Capital'],
]

DISTRIBUTIONS_DATA = [
    ['Date', 'Type', 'Amount', 'Recallable', 'Description'],
    ['2023-12-15', 'Return of Capital', '$1,500,000', 'No', 'Exit: TechCo Inc'],
    ['2024-06-20', 'Income', '$500,000', 'No', 'Dividend Payment'],
    ['2024-09-10', 'Return of Capital', '$2,000,000', 'Yes', 'Partial Exit: DataCorp'],
    ['2024-12-20', 'Income', '$300,000', 'No', 'Year-end Distribution'],
]

ADJUSTMENTS_DATA = [
    ['Date', 'Type', 'Amount', 'Description'],
    ['2024-01-15', 'Recallable Distribution', '-$500,000', 'Recalled distribution from Q4 2023'],
    ['2024-03-20', 'Capital Call Adjustment', '$100,000', 'Management fee adjustment'],
    ['2024-07-10', 'Contribution Adjustment', '-$50,000', 'Expense reimbursement'],
]

SUMMARY_TEXT = """
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

# ==== FUNGSI ASLI (STRUKTUR TIDAK DIUBAH) ====
def create_sample_fund_report():
    """Create a sample fund performance report PDF"""
    
    filename = OUTPUT_FILENAME
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
    
    title = Paragraph(TITLE_FUND_NAME, title_style)
    story.append(title)
    
    subtitle = Paragraph("Quarterly Performance Report - Q4 2024", styles['Heading2'])
    story.append(subtitle)
    story.append(Spacer(1, 0.5*inch))
    
    # Fund Information
    info_text = INFO_TEXT
    story.append(Paragraph(info_text, styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Capital Calls Section
    story.append(Paragraph("<b>Capital Calls</b>", styles['Heading2']))
    story.append(Spacer(1, 0.2*inch))
    
    capital_calls_data = CAPITAL_CALLS_DATA
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
    
    distributions_data = DISTRIBUTIONS_DATA
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
    
    adjustments_data = ADJUSTMENTS_DATA
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
    
    summary_text = SUMMARY_TEXT
    story.append(Paragraph(summary_text, styles['Normal']))
    
    # Build PDF
    doc.build(story)
    print(f"✅ Sample PDF created: {filename}")
    print(f"\nExpected Metrics:")
    print(f"  - Total Capital Called: (lihat SUMMARY_TEXT)")
    print(f"  - Total Distributions: (lihat SUMMARY_TEXT)")
    print(f"  - Net PIC: (lihat SUMMARY_TEXT)")
    print(f"  - DPI: (lihat SUMMARY_TEXT)")
    print(f"  - IRR: (lihat SUMMARY_TEXT)")

# ==== GENERATOR 10 FILE DENGAN RANDOM ====
import os, random, zipfile

FUNDS = [
    "Alpha Growth Fund I",
    "Summit Capital Partners II",
    "Nova Innovation Fund",
    "Evergreen Equity Fund IV",
    "Velocity Ventures Fund",
    "Titan Global Growth Fund",
    "Pioneer AI Ventures",
    "Horizon Digital Fund",
    "Atlas Fintech Fund",
    "Starlight Technology Fund",
]

def _usd(n: int) -> str:
    sign = "-" if n < 0 else ""
    n = abs(n)
    return f"{sign}${n:,.0f}"

def _parse_int_usd(s: str) -> int:
    s = s.replace("$", "").replace(",", "")
    return int(s)

def _build_constants_for_fund(fund_name: str):
    """Bangun konstanta untuk 1 fund (random per tabel + ringkasan)."""
    # --- Capital Calls (4 baris) ---
    calls_rows = []
    total_calls = 0
    calls_tpl = [
        ("2023-01-15", "Call 1", "Initial Capital Call"),
        ("2023-06-20", "Call 2", "Follow-on Investment"),
        ("2024-03-10", "Call 3", "Bridge Round Funding"),
        ("2024-09-15", "Call 4", "Additional Capital"),
    ]
    for date, call_no, desc in calls_tpl:
        amt = random.randint(1_000_000, 6_000_000)
        total_calls += amt
        calls_rows.append([date, call_no, _usd(amt), desc])

    # --- Distributions (4 baris) ---
    dist_rows = []
    total_dist = 0
    dists_tpl = [
        ("2023-12-15", "Return of Capital", "No", "Exit: TechCo Inc"),
        ("2024-06-20", "Income", "No", "Dividend Payment"),
        ("2024-09-10", "Return of Capital", "Yes", "Partial Exit: DataCorp"),
        ("2024-12-20", "Income", "No", "Year-end Distribution"),
    ]
    for date, typ, recall, desc in dists_tpl:
        amt = random.randint(300_000, 2_500_000)
        total_dist += amt
        dist_rows.append([date, typ, _usd(amt), recall, desc])

    # --- Adjustments (3 baris) ---
    a1 = -random.randint(100_000, 500_000)  # recallable distribution
    a2 =  random.randint( 50_000, 200_000)  # fee adjustment
    a3 = -random.randint( 10_000, 100_000)  # expense reimbursement
    adj_rows = [
        ["2024-01-15", "Recallable Distribution", _usd(a1), "Recalled distribution from Q4 2023"],
        ["2024-03-20", "Capital Call Adjustment", _usd(a2), "Management fee adjustment"],
        ["2024-07-10", "Contribution Adjustment", _usd(a3), "Expense reimbursement"],
    ]
    net_adj = a1 + a2 + a3

    # --- Summary metrics ---
    net_pic = total_calls + net_adj  # paid-in after adjustments
    dpi = total_dist / net_pic if net_pic != 0 else 0.0

    # IRR random 8%–20%
    irr = round(random.uniform(8.0, 20.0), 2)

    # Residual value ~ 0.4–1.2 x PIC (kasar untuk simulasi)
    residual_multiple = random.uniform(0.4, 1.2)
    residual_value = int(net_pic * residual_multiple)
    tvpi = (total_dist + residual_value) / net_pic if net_pic != 0 else 1.0

    # --- Build constants text blobs ---
    title_name = fund_name
    info_text = f"""
    <b>Fund Name:</b> {fund_name}<br/>
    <b>GP:</b> {fund_name.split()[0]} Partners<br/>
    <b>Vintage Year:</b> 2023<br/>
    <b>Fund Size:</b> $100,000,000<br/>
    <b>Report Date:</b> December 31, 2024
    """

    summary_text = f"""
    <b>Total Capital Called:</b> {_usd(total_calls)}<br/>
    <b>Total Distributions:</b> {_usd(total_dist)}<br/>
    <b>Net Paid-In Capital (PIC):</b> {_usd(net_pic)}<br/>
    <b>Distributions to Paid-In (DPI):</b> {dpi:.2f}<br/>
    <b>Internal Rate of Return (IRR):</b> {irr:.2f}%<br/>
    <b>Total Value to Paid-In (TVPI):</b> {tvpi:.2f}<br/>
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

    # Kembalikan semua konstanta siap pakai
    return {
        "TITLE_FUND_NAME": title_name,
        "OUTPUT_FILENAME": f"{fund_name.replace(' ', '_')}.pdf",
        "INFO_TEXT": info_text,
        "CAPITAL_CALLS_DATA": [['Date','Call Number','Amount','Description']] + calls_rows,
        "DISTRIBUTIONS_DATA": [['Date','Type','Amount','Recallable','Description']] + dist_rows,
        "ADJUSTMENTS_DATA": [['Date','Type','Amount','Description']] + adj_rows,
        "SUMMARY_TEXT": summary_text,
    }

# ==== MAIN LOOP ====
if __name__ == "__main__":
    generated = []
    for fund in FUNDS:
        consts = _build_constants_for_fund(fund)

        # set konstanta modul untuk dipakai fungsi tanpa ubah struktur
        TITLE_FUND_NAME = consts["TITLE_FUND_NAME"]
        OUTPUT_FILENAME = consts["OUTPUT_FILENAME"]
        INFO_TEXT = consts["INFO_TEXT"]
        CAPITAL_CALLS_DATA = consts["CAPITAL_CALLS_DATA"]
        DISTRIBUTIONS_DATA = consts["DISTRIBUTIONS_DATA"]
        ADJUSTMENTS_DATA = consts["ADJUSTMENTS_DATA"]
        SUMMARY_TEXT = consts["SUMMARY_TEXT"]

        # Panggil fungsi asli (struktur tetap)
        create_sample_fund_report()
        generated.append(OUTPUT_FILENAME)

    # Optional: zip semua file
    zip_name = "fund_reports.zip"
    with zipfile.ZipFile(zip_name, "w") as z:
        for f in generated:
            z.write(f, arcname=f)
    print(f"\n🎉 Done. Generated {len(generated)} PDFs and zipped to {zip_name}")
