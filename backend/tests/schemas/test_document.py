from datetime import datetime
from types import SimpleNamespace

from app.schemas.document import (
    Document,
    DocumentStatus,
    DocumentUploadResponse,
    ProcessedDocumentFailure,
    ProcessedDocumentResult,
    ProcessedDocumentSuccess,
)


def test_document_model_validation_from_attributes():
    source = SimpleNamespace(
        id=42,
        fund_id=7,
        file_name="test.pdf",
        file_path="/tmp/test.pdf",
        upload_date=datetime.utcnow(),
        parsing_status="completed",
        error_message=None,
    )

    model = Document.model_validate(source)

    assert model.id == 42
    assert model.file_name == "test.pdf"
    assert model.parsing_status == "completed"


def test_document_upload_response_defaults():
    response = DocumentUploadResponse(
        document_id=1,
        status="pending",
        message="queued",
    )

    assert response.document_id == 1
    assert response.task_id is None
    assert response.status == "pending"


def test_document_status_optional_fields():
    status = DocumentStatus(document_id=9, status="processing")

    assert status.document_id == 9
    assert status.status == "processing"
    assert status.progress is None
    assert status.error_message is None


def test_processed_document_result_success_and_failure_shapes():
    success: ProcessedDocumentSuccess = {
        "status": "completed",
        "document_id": 10,
        "fund_id": 2,
        "tables_extracted": {"capital_calls": 1, "distributions": 0, "adjustments": 0},
        "text_chunks": 3,
        "parser_engine": "pdfplumber",
    }

    failure: ProcessedDocumentFailure = {
        "status": "failed",
        "document_id": 10,
        "fund_id": 2,
        "error": "boom",
    }

    assert success["status"] == "completed"
    assert failure["status"] == "failed"

    result: ProcessedDocumentResult = success
    assert result["document_id"] == 10
