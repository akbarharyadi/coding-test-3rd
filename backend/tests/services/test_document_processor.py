from unittest.mock import MagicMock

import pytest

from app.helpers.document_utils import TableCandidate, TextSegment
from app.services.document_processor import DocumentProcessor
from app.services.table_parser import ParsedTable


@pytest.mark.asyncio
async def test_process_document_success(monkeypatch, tmp_path):
    file_path = tmp_path / "sample.pdf"
    file_path.write_bytes(b"%PDF-1.4\n")

    table_candidate = TableCandidate(
        data=[["Date", "Amount"], ["2023-01-01", "$100.00"]],
        page_number=1,
    )
    text_segment = TextSegment(
        page_number=1,
        text="sample text chunk",
        document_id=1,
        fund_id=1,
    )

    mock_pdf_extract = MagicMock(return_value=([table_candidate], [text_segment]))
    monkeypatch.setattr(
        "app.services.document_processor.extract_with_pdfplumber",
        mock_pdf_extract,
    )
    monkeypatch.setattr(
        "app.services.document_processor.chunk_text_segments",
        lambda **kwargs: [{"content": "chunk", "metadata": {}}],
    )

    processor = DocumentProcessor(db_session=MagicMock(), use_docling=False)

    parsed_table = ParsedTable(
        table_type="capital_calls",
        rows=[
            {"call_date": "2023-01-01", "amount": 100, "description": "Initial"},
        ],
        page_number=1,
        header=["Date", "Amount", "Description"],
    )
    processor.table_parser.parse = MagicMock(return_value=parsed_table)
    processor._persist_transactions = MagicMock()

    result = await processor.process_document(str(file_path), document_id=1, fund_id=2)

    assert result["status"] == "completed"
    assert result["parser_engine"] == "pdfplumber"
    assert result["tables_extracted"]["capital_calls"] == 1
    processor._persist_transactions.assert_called_once()


@pytest.mark.asyncio
async def test_process_document_missing_file(tmp_path):
    processor = DocumentProcessor(db_session=MagicMock(), use_docling=False)

    result = await processor.process_document(
        str(tmp_path / "missing.pdf"), document_id=99, fund_id=1
    )

    assert result["status"] == "failed"
    assert "File not found" in result["error"]
